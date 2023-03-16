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

package org.apache.kylin.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class MetadataTestUtils {

    public static final String TABLE_EXCLUSION_SETTING = "kylin.metadata.table-exclusion-enabled";

    private MetadataTestUtils() {
    }

    public static Set<String> getExcludedTables(String project) {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return tableMgr.listAllTables().stream() //
                .map(tableMgr::getTableExtIfExists).filter(Objects::nonNull) //
                .filter(TableExtDesc::isExcluded) //
                .map(TableExtDesc::getIdentity) //
                .collect(Collectors.toSet());
    }

    public static Set<String> getExcludedColumns(String project, String tableIdentity) {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        TableDesc tableDesc = tableMgr.getTableDesc(StringUtils.upperCase(tableIdentity));
        TableExtDesc tableExt = tableMgr.getOrCreateTableExt(tableDesc);
        if (tableExt.isExcluded()) {
            return Arrays.stream(tableDesc.getColumns()).map(ColumnDesc::getName).collect(Collectors.toSet());
        }
        return tableExt.getExcludedColumns();
    }

    /**
     * Mock an excluded table.
     * @param project name of project
     * @param tableIdentity qualified name of table
     */
    public static void mockExcludedTable(String project, String tableIdentity) {
        mockExcludedCols(project, tableIdentity, Sets.newHashSet());
    }

    /**
     * Mock excluded columns of give table.
     * @param project name of project
     * @param tableIdentity qualified name of table
     * @param excludedColumns columns to be excluded
     */
    public static void mockExcludedCols(String project, String tableIdentity, Set<String> excludedColumns) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            innerUpdate(project, TABLE_EXCLUSION_SETTING, "true");
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, project);
            TableDesc tableDesc = tableMgr.getTableDesc(StringUtils.upperCase(tableIdentity));
            boolean tableExtExisted = tableMgr.isTableExtExist(StringUtils.upperCase(tableIdentity));
            TableExtDesc tableExt = tableMgr.getOrCreateTableExt(tableDesc);
            tableExt.setExcluded(excludedColumns.isEmpty());
            Set<String> toBeExcludedColumns = excludedColumns.stream() //
                    .map(StringUtils::upperCase) //
                    .collect(Collectors.toSet());
            tableExt.getExcludedColumns().addAll(toBeExcludedColumns);
            tableMgr.saveOrUpdateTableExt(tableExtExisted, tableExt);
            return null;
        }, project);
    }

    public static void mockExcludedTables(String project, List<String> tableIdentityList) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            innerUpdate(project, TABLE_EXCLUSION_SETTING, "true");
            tableIdentityList.forEach(table -> mockExcludedTable(project, table));
            return null;
        }, project);
    }

    public static void toSemiAutoProjectMode(String project) {
        updateProjectConfig(project, "kylin.metadata.semi-automatic-mode", "true");
    }

    public static KylinConfig turnOnExcludedTable(KylinConfig config) {
        config.setProperty("kylin.metadata.table-exclusion-enabled", "true");
        return config;
    }

    public static KylinConfig setOnlyReuseUseDefinedCC(KylinConfig config) {
        config.setProperty("kylin.metadata.only-reuse-user-defined-computed-column", "true");
        return config;
    }

    public static void updateProjectConfig(String project, String property, String value) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            innerUpdate(project, property, value);
            return null;
        }, project);
    }

    private static void innerUpdate(String project, String property, String value) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(project, copyForWrite -> {
            LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
            if (overrideKylinProps == null) {
                overrideKylinProps = Maps.newLinkedHashMap();
            }
            overrideKylinProps.put(property, value);
            copyForWrite.setOverrideKylinProps(overrideKylinProps);
        });
    }

    public static void updateProjectConfig(String project, Map<String, String> properties) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            projectManager.updateProject(project, copyForWrite -> {
                LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
                overrideKylinProps.putAll(properties);
            });
            return null;
        }, project);
    }

    public static void createTable(String project, Class<?> clazz, String srcTableDir, String tableIdentity)
            throws IOException {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            String tableJsonPath = concatTablePath(srcTableDir, tableIdentity);
            String fullPath = Objects.requireNonNull(clazz.getResource(tableJsonPath)).getPath();
            TableDesc newTable = JsonUtil.readValue(new File(fullPath), TableDesc.class);
            NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            newTable.setMvcc(-1);
            mgr.saveSourceTable(newTable);
            return null;
        }, project);
    }

    /**
     * Replace the table in destTableDir with the table in srcTableDir.
     * For example:
     * --- srcTableDir is `/data/tableDesc`
     * --- tableIdentity is `SSB.CUSTOMER`
     */
    public static void replaceTable(String project, Class<?> clazz, String srcTableDir, String tableIdentity)
            throws IOException {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            String tableJsonPath = concatTablePath(srcTableDir, tableIdentity);
            String fullPath = Objects.requireNonNull(clazz.getResource(tableJsonPath)).getPath();
            TableDesc srcTable = JsonUtil.readValue(new File(fullPath), TableDesc.class);
            NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            TableDesc oldTable = mgr.getTableDesc(tableIdentity);
            srcTable.setMvcc(oldTable.getMvcc());
            mgr.updateTableDesc(srcTable);
            return null;
        }, project);
    }

    private static String concatTablePath(String tableDir, String tableIdentity) {
        return concat(tableDir, tableIdentity, false);
    }

    /**
     * Put the data from source directory to the kylin home directory.
     */
    public static void putTableCSVData(String srcDir, Class<?> clazz, String tableIdentity) throws IOException {
        String dataPath = concatTableDataPath(srcDir, tableIdentity);
        String fullPath = Objects.requireNonNull(clazz.getResource(dataPath)).getPath();
        String data = FileUtils.readFileToString(new File(fullPath), Charset.defaultCharset());
        FileUtils.write(new File(concatTableDataPath(KylinConfig.getKylinHome(), tableIdentity)), data,
                Charset.defaultCharset());
    }

    private static String concatTableDataPath(String dataDir, String tableIdentity) {
        return concat(dataDir, tableIdentity, true);
    }

    private static String concat(String dir, String table, boolean isData) {
        String parentPath = dir.endsWith("/") ? dir : dir + "/";
        String postfix = isData ? ".csv" : ".json";
        return parentPath + table + postfix;
    }
}
