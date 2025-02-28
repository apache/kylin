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

package org.apache.kylin.query.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterPushDownUtil {

    private static final Logger logger = LoggerFactory.getLogger(FilterPushDownUtil.class);

    /**
     * Related to hive push down schema, aligned to
     * org.apache.kylin.query.util.PushDownUtil#tryPushDownQuery(String, String, String, SQLException, boolean, boolean)
     */
    private static final String HIVE_DEFAULT_SCHEMA = "DEFAULT";

    /**
     * Apply the extraCondition to the sqlToUpdate with a extra condition, only if the select
     * directive contains the target table.
     *
     * @param sqlToUpdate    input sql need update
     * @param extraCondition the extra condition to be added
     * @param targetTable    target table
     * @return               sql applied filter condition
     * @throws SqlParseException if there is a parse error
     */
    static String applyFilterCondition(String sqlToUpdate, String extraCondition, String targetTable)
            throws SqlParseException {
        targetTable = quickTableCheck(sqlToUpdate, targetTable);
        SqlCall inputToNode = (SqlCall) CalciteParser.parse(sqlToUpdate);

        ConditionModifier modifier = new ConditionModifier(targetTable, extraCondition);
        modifier.visit(inputToNode);

        return inputToNode.toSqlString(SqlDialect.DatabaseProduct.HIVE.getDialect()).toString();
    }

    /**
     * Quick check, ensure the target table appears in the given sql. Note that if the schema is 'DEFAULT',
     * delete schema before return.
     *
     * @param sql         the environment sql
     * @param targetTable target table to check
     * @return            target table
     */
    private static String quickTableCheck(final String sql, String targetTable) {

        Preconditions.checkNotNull(targetTable);
        final String[] names = targetTable.split("\\.");
        Preconditions.checkState(names.length == 2 && names[0].length() > 0 && names[1].length() > 0,
                "Malformed target table '" + targetTable + "', missing schema or table name.");

        if (names[0].trim().equalsIgnoreCase(HIVE_DEFAULT_SCHEMA)) {
            targetTable = names[1];
        }

        Preconditions.checkState(sql.contains(targetTable),
                "The target table '" + targetTable + "' cannot be found in query\n." + sql);
        return targetTable;
    }

    /**
     * Used to find the select statements directive contains primitive table, and modify them
     * with given condition
     */
    private static class ConditionModifier extends SqlBasicVisitor {
        private final String targetTable;
        private final String filterCondition;

        ConditionModifier(String targetTable, String filterCondition) {
            this.targetTable = targetTable.toUpperCase(Locale.ROOT);
            this.filterCondition = filterCondition;
        }

        @Override
        public Object visit(SqlNodeList nodeList) {
            for (SqlNode node : nodeList) {
                if (node instanceof SqlWithItem) {
                    SqlWithItem item = (SqlWithItem) node;
                    item.query.accept(this);
                }
            }
            return null;
        }

        @Override
        public Object visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                select.getFrom().accept(this);
                try {
                    updateTableScanSelect(select);
                } catch (SqlParseException e) {
                    throw new IllegalArgumentException("While parsing filter condition '" + filterCondition
                            + "' throws an " + e.getCause().getClass().getName());
                }
                return null;
            }

            if (call instanceof SqlOrderBy) {
                SqlOrderBy orderBy = (SqlOrderBy) call;
                orderBy.query.accept(this);
                return null;
            }

            if (call instanceof SqlWith) {
                SqlWith sqlWith = (SqlWith) call;
                sqlWith.withList.accept(this);
                sqlWith.body.accept(this);
            }

            if (call instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) call;
                for (SqlNode node : basicCall.getOperandList()) {
                    node.accept(this);
                }
                return null;
            }

            if (call instanceof SqlJoin) {
                SqlJoin node = (SqlJoin) call;
                node.getRight().accept(this);
                node.getLeft().accept(this);
                return null;
            }
            return null;
        }

        /**
         * Update the node sqlSelectToUpdate if it need scan the target table; otherwise, do nothing.
         */
        private void updateTableScanSelect(SqlSelect sqlSelectToUpdate) throws SqlParseException {
            List<Pair<String, String>> nameAliasNamePairs = Lists.newArrayList();

            SqlNode from = sqlSelectToUpdate.getFrom();
            LinkedList<SqlNode> queue = new LinkedList<>();
            queue.push(from);
            while (queue.size() > 0) {
                final SqlNode pop = queue.pop();
                if (pop instanceof SqlIdentifier) {
                    nameAliasNamePairs.add(new Pair<>(pop.toString(), ""));
                } else if (sqlKindEqualsAs(pop)) {
                    extractNames(nameAliasNamePairs, pop);
                }

                if (pop instanceof SqlJoin) {
                    queue.push(((SqlJoin) pop).getLeft());
                    queue.push(((SqlJoin) pop).getRight());
                }
            }

            for (Pair<String, String> pair : nameAliasNamePairs) {
                if (!pair.getFirst().contains(targetTable)) {
                    continue;
                }

                SqlNode toAppend = createCondition(filterCondition, pair);
                if (sqlSelectToUpdate.getWhere() == null) {
                    sqlSelectToUpdate.setWhere(toAppend);
                } else {
                    SqlNode where = sqlSelectToUpdate.getWhere();
                    SqlNode newWhere = new SqlBasicCall(//
                            SqlStdOperatorTable.AND, //
                            new SqlNode[] { where, toAppend }, //
                            where.getParserPosition()//
                    );
                    sqlSelectToUpdate.setWhere(newWhere);
                }
            }
        }

        /**
         * Check the node is a instance of <code>BasicSqlCall</code> and it's <code>SqlKind</code>
         * equals to <code>SqlKind.AS</code>.
         */
        private boolean sqlKindEqualsAs(SqlNode node) {
            return node instanceof SqlBasicCall && node.getKind() == SqlKind.AS;
        }

        /**
         * Extract the table name and alias name, if the given node is a instance of <code>SqlBasicCall</code>
         * and it's <code>SqlKind</code> equals to <code>SqlKind.AS</code>.
         */
        private void extractNames(List<Pair<String, String>> nameAndAliasNames, SqlNode node) {
            if (node instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) node;
                final SqlNode sqlNode = call.operand(0);
                if (sqlNode instanceof SqlIdentifier) {
                    String name = sqlNode.toString();
                    String aliasName = call.operand(1).toString();
                    nameAndAliasNames.add(new Pair<>(name, aliasName));
                }
            }
        }

        /**
         * Create filter node based on the given table name and alias name.
         * @throws SqlParseException if there is a parse error
         */
        private SqlNode createCondition(String filterCondition, Pair<String, String> tableNameAndAliasName)
                throws SqlParseException {
            Preconditions.checkState(filterCondition != null && filterCondition.length() > 0,
                    "Filter condition can not be NULL or empty string.");
            SqlNode node = SqlParser.create(filterCondition).parseExpression();
            if (tableNameAndAliasName.getSecond().length() > 0) {
                AliasModifier aliasModifier = new AliasModifier(tableNameAndAliasName.getSecond());
                if (node instanceof SqlCall) {
                    aliasModifier.visit((SqlCall) node);
                }
            }
            return node;
        }
    }

    /**
     * Used to modify the alias name only if it's a instance of <code>SqlIdentifier</code>.
     */
    private static class AliasModifier extends SqlBasicVisitor {
        private String aliasName;

        AliasModifier(String aliasName) {
            this.aliasName = aliasName;
        }

        @Override
        public SqlCall visit(SqlCall call) {
            if (call instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) call;

                for (SqlNode node : basicCall.getOperandList()) {
                    node.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlIdentifier visit(SqlIdentifier identifier) {
            if (identifier.names.size() == 2) {
                final String[] values = identifier.names.toArray(new String[0]);
                values[0] = aliasName;
                identifier.names = ImmutableList.copyOf(values);
            }
            return null;
        }
    }
}
