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

package org.apache.kylin.rest.util;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.MetadataManager.CCInfo;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.source.adhocquery.IAdHocConverter;
import org.apache.kylin.source.adhocquery.IAdHocRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class AdHocUtil {
    private static final Logger logger = LoggerFactory.getLogger(AdHocUtil.class);

    public static boolean doAdHocQuery(String project, String sql, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas, SQLException sqlException) throws Exception {

        boolean isExpectedCause = (ExceptionUtils.getRootCause(sqlException).getClass()
                .equals(NoRealizationFoundException.class));
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (isExpectedCause && kylinConfig.isAdhocEnabled()) {

            logger.info("Query failed to utilize pre-calculation, routing to other engines", sqlException);
            IAdHocRunner runner = (IAdHocRunner) ClassUtil.newInstance(kylinConfig.getAdHocRunnerClassName());
            IAdHocConverter converter = (IAdHocConverter) ClassUtil
                    .newInstance(kylinConfig.getAdHocConverterClassName());

            runner.init(kylinConfig);

            logger.debug("Ad-hoc query runner {}", runner);

            String expandCC = restoreComputedColumnToExpr(sql, project);
            String adhocSql = converter.convert(expandCC);
            if (!adhocSql.equals(sql)) {
                logger.info("before delegating to adhoc, the query is converted to {} ", adhocSql);
            }

            runner.executeQuery(adhocSql, results, columnMetas);

            return true;
        } else {
            throw sqlException;
        }
    }

    private final static Pattern identifierInSqlPattern = Pattern.compile(
            //find pattern like "table"."column" or "column"
            "((?<![\\p{L}_0-9\\.\\\"])(\\\"[\\p{L}_0-9]+\\\"\\.)?(\\\"[\\p{L}_0-9]+\\\")(?![\\p{L}_0-9\\.\\\"]))" + "|"
            //find pattern like table.column or column
                    + "((?<![\\p{L}_0-9\\.\\\"])([\\p{L}_0-9]+\\.)?([\\p{L}_0-9]+)(?![\\p{L}_0-9\\.\\\"]))");

    private final static Pattern identifierInExprPattern = Pattern.compile(
            // a.b.c
            "((?<![\\p{L}_0-9\\.\\\"])([\\p{L}_0-9]+\\.)([\\p{L}_0-9]+\\.)([\\p{L}_0-9]+)(?![\\p{L}_0-9\\.\\\"]))");

    private final static Pattern endWithAsPattern = Pattern.compile("\\s+as\\s+$", Pattern.CASE_INSENSITIVE);

    public static String restoreComputedColumnToExpr(String beforeSql, String project) {
        MetadataManager metadataManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        Map<String, CCInfo> ccInfoMap = metadataManager.getCcInfoMap();
        final ProjectInstance projectInstance = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);

        Iterable<CCInfo> projectCCInfo = Iterables.filter(ccInfoMap.values(), new Predicate<CCInfo>() {
            @Override
            public boolean apply(@Nullable CCInfo ccInfo) {
                return Iterables.any(ccInfo.getDataModelDescs(), new Predicate<DataModelDesc>() {
                    @Override
                    public boolean apply(@Nullable DataModelDesc model) {
                        return projectInstance.containsModel(model.getName());
                    }
                });
            }
        });

        String afterSql = beforeSql;
        for (CCInfo ccInfo : projectCCInfo) {
            afterSql = restoreComputedColumnToExpr(afterSql, ccInfo);
        }

        if (!StringUtils.equals(beforeSql, afterSql)) {
            logger.info("computed column in sql is expanded before sending to adhoc engine: " + afterSql);
        }
        return afterSql;
    }

    static String restoreComputedColumnToExpr(String sql, CCInfo ccInfo) {

        String ccName = ccInfo.getComputedColumnDesc().getColumnName();
        List<Triple<Integer, Integer, String>> replacements = Lists.newArrayList();
        Matcher matcher = identifierInSqlPattern.matcher(sql);

        while (matcher.find()) {
            if (matcher.group(1) != null) { //with quote case: "TABLE"."COLUMN"

                String quotedColumnName = matcher.group(3);
                Preconditions.checkNotNull(quotedColumnName);
                String columnName = StringUtils.strip(quotedColumnName, "\"");
                if (!columnName.equalsIgnoreCase(ccName)) {
                    continue;
                }

                if (matcher.group(2) != null) { // table name exist 
                    String quotedTableAlias = StringUtils.strip(matcher.group(2), ".");
                    String tableAlias = StringUtils.strip(quotedTableAlias, "\"");
                    replacements.add(Triple.of(matcher.start(1), matcher.end(1),
                            replaceIdentifierInExpr(ccInfo.getComputedColumnDesc().getExpression(), tableAlias, true)));
                } else { //only column
                    if (endWithAsPattern.matcher(sql.substring(0, matcher.start(1))).find()) {
                        //select DEAL_AMOUNT as "deal_amount" case
                        continue;
                    }
                    replacements.add(Triple.of(matcher.start(1), matcher.end(1),
                            replaceIdentifierInExpr(ccInfo.getComputedColumnDesc().getExpression(), null, true)));
                }
            } else if (matcher.group(4) != null) { //without quote case: table.column or simply column
                String columnName = matcher.group(6);
                Preconditions.checkNotNull(columnName);
                if (!columnName.equalsIgnoreCase(ccName)) {
                    continue;
                }

                if (matcher.group(5) != null) { //table name exist
                    String tableAlias = StringUtils.strip(matcher.group(5), ".");
                    replacements.add(Triple.of(matcher.start(4), matcher.end(4), replaceIdentifierInExpr(
                            ccInfo.getComputedColumnDesc().getExpression(), tableAlias, false)));

                } else { //only column 
                    if (endWithAsPattern.matcher(sql.substring(0, matcher.start(4))).find()) {
                        //select DEAL_AMOUNT as deal_amount case
                        continue;
                    }
                    replacements.add(Triple.of(matcher.start(4), matcher.end(4),
                            replaceIdentifierInExpr(ccInfo.getComputedColumnDesc().getExpression(), null, false)));
                }
            }
        }

        Collections.reverse(replacements);
        for (Triple<Integer, Integer, String> triple : replacements) {
            sql = sql.substring(0, triple.getLeft()) + "(" + triple.getRight() + ")"
                    + sql.substring(triple.getMiddle());
        }
        return sql;
    }

    static String replaceIdentifierInExpr(String expr, String tableAlias, boolean quoted) {
        if (tableAlias == null) {
            return expr;
        }

        return CalciteParser.insertAliasInExpr(expr, tableAlias);
    }
}
