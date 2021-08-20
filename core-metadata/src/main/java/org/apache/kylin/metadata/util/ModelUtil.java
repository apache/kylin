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

package org.apache.kylin.metadata.util;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ModelUtil {

    private static final Logger logger = LoggerFactory.getLogger(ModelUtil.class);

    public static void verifyFilterCondition(String project, TableMetadataManager tableManager, DataModelDesc modelDesc,
            String filterCondition) throws Exception {
        StringBuilder checkSql = new StringBuilder();
        checkSql.append("select * from ").append(modelDesc.getRootFactTableName()).append(" where ")
                .append(filterCondition);

        SqlCall inputToNode = (SqlCall) parse(doubleQuoteKeywordDefault(checkSql.toString()));
        SqlVerify sqlVerify = new SqlVerify(project, tableManager, modelDesc);
        sqlVerify.visit(inputToNode);
    }

    public static SqlNode parse(String sql) throws Exception {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder().setIdentifierMaxLength(300);
        SqlParser sqlParser = SqlParser.create(sql, parserBuilder.build());
        return sqlParser.parseQuery();
    }

    private static class SqlVerify extends SqlBasicVisitor {

        private DataModelDesc modelDesc;
        private TableMetadataManager tableManager;
        private String project;

        SqlVerify(String project, TableMetadataManager tableManager, DataModelDesc modelDesc) {
            this.modelDesc = modelDesc;
            this.tableManager = tableManager;
            this.project = project;
        }

        @Override
        public Object visit(SqlCall call) {
            SqlSelect select = (SqlSelect) call;
            WhereColumnVerify.verify(project, select.getWhere(), modelDesc, tableManager);
            return null;
        }
    }

    private static class WhereColumnVerify extends SqlBasicVisitor {

        private List<String> allSqlIdentifier = Lists.newArrayList();

        static void verify(String project, SqlNode whereNode, DataModelDesc modelDesc,
                TableMetadataManager tableManager) {
            WhereColumnVerify whereColumnVerify = new WhereColumnVerify();
            whereNode.accept(whereColumnVerify);

            List<ColumnDesc> columnDesc = Arrays.stream(modelDesc.getJoinTables()).flatMap(table -> {
                return Arrays.stream(tableManager.getTableDesc(table.getTable(), project).getColumns());
            }).collect(Collectors.toList());
            columnDesc.addAll(
                    Arrays.asList(tableManager.getTableDesc(modelDesc.getRootFactTableName(), project).getColumns()));
            List<String> allColumn = columnDesc.stream().map(cd -> cd.getName().toLowerCase(Locale.ROOT))
                    .collect(Collectors.toList());
            whereColumnVerify.allSqlIdentifier.stream().forEach(col -> {
                if (!allColumn.contains(col.toLowerCase(Locale.ROOT))) {
                    String verifyError = String.format(Locale.ROOT,
                            "filter condition col: %s is not a column in the table ", col);
                    logger.error(verifyError);
                    throw new IllegalArgumentException(verifyError);
                }
            });
        }

        public Object visit(SqlIdentifier id) {
            if (id.names.size() == 1) {
                allSqlIdentifier.add(id.names.get(0));
            } else if (id.names.size() == 2) {
                allSqlIdentifier.add(id.names.get(1));
            }
            return null;
        }
    }

    public static String doubleQuoteKeywordDefault(String sql) {
        sql = sql.replaceAll("(?i)default\\.", "\"DEFAULT\".");
        sql = sql.replace("defaultCatalog.", "");
        sql = sql.replace("\"defaultCatalog\".", "");
        return sql;
    }

}
