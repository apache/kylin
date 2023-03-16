/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.LogManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.util.ExecAndComp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.io.Files;

import lombok.val;

/**
 */
public class KylinTestBase extends NLocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(KylinTestBase.class);

    protected static KylinConfig config = null;
    protected static String joinType = "default";

    protected String getProject() {
        return ProjectInstance.DEFAULT_PROJECT_NAME;
    }

    private static class FileByNameComparator implements Comparator<File> {
        @Override
        public int compare(File o1, File o2) {
            return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
        }
    }

    /**
     * @param folder
     * @param fileType specify the interested file type by file extension
     * @return
     */
    protected static List<File> getFilesFromFolder(final File folder, final String fileType) {
        System.out.println(folder.getAbsolutePath());
        Set<File> set = new TreeSet<>(new FileByNameComparator());
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            if (fileEntry.getName().toLowerCase(Locale.ROOT).endsWith(fileType.toLowerCase(Locale.ROOT))) {
                set.add(fileEntry);
            }
        }
        return new ArrayList<>(set);
    }

    public static String getTextFromFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }

    public static List<String> getParameterFromFile(File sqlFile) throws IOException {
        String sqlFileName = sqlFile.getAbsolutePath();
        int prefixIndex = sqlFileName.lastIndexOf(".sql");
        String dataFielName = sqlFileName.substring(0, prefixIndex) + ".dat";
        File dataFile = new File(dataFielName);
        List<String> parameters = Files.readLines(dataFile, Charset.defaultCharset());
        return parameters;
    }

    // ////////////////////////////////////////////////////////////////////////////////////////
    // execute

    protected int executeQuery(String sql) throws Exception {
        // change join type to match current setting
        sql = ExecAndComp.changeJoinType(sql, joinType);

        try {
            return new QueryExec(getProject(), KylinConfig.getInstanceFromEnv()).executeQuery(sql).getRows().size();
        } catch (SQLException sqlException) {
            Pair<List<List<String>>, List<SelectedColumnMeta>> result = tryPushDownSelectQuery(
                    ProjectInstance.DEFAULT_PROJECT_NAME, sql, "DEFAULT", sqlException,
                    BackdoorToggles.getPrepareOnly(), false);
            if (result == null) {
                throw sqlException;
            }
            return result.getFirst().size();
        }
    }

    public Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(String project, String sql,
            String defaultSchema, SQLException sqlException, boolean isPrepare, boolean isForced) throws Exception {
        String massagedSql = QueryUtil.appendLimitOffset(project, sql, 0, 0);
        QueryParams queryParams = new QueryParams(project, massagedSql, defaultSchema, isPrepare, sqlException,
                isForced);
        queryParams.setSelect(true);
        queryParams.setLimit(0);
        queryParams.setOffset(0);
        return tryPushDownQuery(queryParams);
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownQuery(QueryParams queryParams)
            throws Exception {
        val results = PushDownUtil.tryIterQuery(queryParams);
        if (results == null) {
            return null;
        }
        return new Pair<>(ImmutableList.copyOf(results.getRows()), results.getColumnMetas());
    }

    // end of execute
    // ////////////////////////////////////////////////////////////////////////////////////////

    protected int runSQL(File sqlFile, boolean debug, boolean explain) throws Exception {
        if (debug) {
            overwriteSystemProp("calcite.debug", "true");
            InputStream inputStream = new FileInputStream("src/test/resources/logging.properties");
            LogManager.getLogManager().readConfiguration(inputStream);
        }

        String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
        logger.info("Testing Query " + queryName);
        String sql = getTextFromFile(sqlFile);
        if (explain) {
            sql = "explain plan for " + sql;
        }
        int count = executeQuery(sql);

        if (debug) {
            restoreSystemProp("calcite.debug");
        }
        return count;
    }

}
