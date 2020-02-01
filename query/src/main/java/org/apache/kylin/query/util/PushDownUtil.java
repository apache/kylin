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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class PushDownUtil {
    private static final Logger logger = LoggerFactory.getLogger(PushDownUtil.class);

    private PushDownUtil() {
        throw new IllegalStateException("Class PushDownUtil is an utility class !");
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(String project, String sql,
            String defaultSchema, SQLException sqlException, boolean isPrepare) throws Exception {
        PushDownExecutor executor = new PushDownExecutor();
        return executor.pushDownQuery(project, sql, defaultSchema, sqlException, true, isPrepare);
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownNonSelectQuery(String project,
            String sql, String defaultSchema, boolean isPrepare) throws Exception {
        PushDownExecutor executor = new PushDownExecutor();
        return executor.pushDownQuery(project, sql, defaultSchema, null, true, isPrepare);
    }

    static String schemaCompletion(String inputSql, String schema) throws SqlParseException {
        if (inputSql == null || inputSql.equals("")) {
            return "";
        }
        SqlNode node = CalciteParser.parse(inputSql);

        // get all table node that don't have schema by visitor pattern
        PushDownUtil.FromTablesVisitor ftv = new PushDownUtil.FromTablesVisitor();
        node.accept(ftv);
        List<SqlNode> tablesWithoutSchema = ftv.getTablesWithoutSchema();
        // sql do not need completion
        if (tablesWithoutSchema.isEmpty()) {
            return inputSql;
        }

        List<Pair<Integer, Integer>> tablesPos = new ArrayList<>();
        for (SqlNode tables : tablesWithoutSchema) {
            tablesPos.add(CalciteParser.getReplacePos(tables, inputSql));
        }

        // make the behind position in the front of the list, so that the front position
        // will not be affected when replaced
        Collections.sort(tablesPos, new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                int r = o2.getFirst() - o1.getFirst();
                return r == 0 ? o2.getSecond() - o1.getSecond() : r;
            }
        });

        StrBuilder afterConvert = new StrBuilder(inputSql);
        for (Pair<Integer, Integer> pos : tablesPos) {
            String tableWithSchema = schema + "." + inputSql.substring(pos.getFirst(), pos.getSecond());
            afterConvert.replace(pos.getFirst(), pos.getSecond(), tableWithSchema);
        }
        return afterConvert.toString();
    }

    /**
     * Get all the tables from "FROM clause" that without schema
     * subquery is only considered in "from clause"
     */
    static class FromTablesVisitor implements SqlVisitor<SqlNode> {
        private List<SqlNode> tables;
        private List<SqlNode> withTables;

        FromTablesVisitor() {
            this.tables = new ArrayList<>();
            this.withTables = new ArrayList<>();
        }

        List<SqlNode> getTablesWithoutSchema() {
            List<SqlNode> sqlNodes = Lists.newArrayList();
            List<String> withs = Lists.newArrayList();
            for (SqlNode withTable : withTables) {
                withs.add(((SqlIdentifier) withTable).names.get(0)); // with clause not allow database.table pattern
            }
            for (SqlNode table : tables) {
                SqlIdentifier identifier = (SqlIdentifier) table;
                if (!withs.contains(identifier.names.get(0))) {
                    sqlNodes.add(identifier);
                }
            }
            return sqlNodes;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                if (node instanceof SqlWithItem) {
                    SqlWithItem item = (SqlWithItem) node;
                    item.query.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                select.getFrom().accept(this);
                return null;
            }
            if (call instanceof SqlOrderBy) {
                SqlOrderBy orderBy = (SqlOrderBy) call;
                orderBy.query.accept(this);
                return null;
            }
            if (call instanceof SqlWith) {
                SqlWith sqlWith = (SqlWith) call;
                List<SqlNode> list = sqlWith.withList.getList();
                for (SqlNode sqlNode : list) {
                    withTables.add(((SqlWithItem) sqlNode).name);
                }
                sqlWith.body.accept(this);
                sqlWith.withList.accept(this);
            }
            if (call instanceof SqlBasicCall) {
                SqlBasicCall node = (SqlBasicCall) call;
                node.getOperands()[0].accept(this);
                return null;
            }
            if (call instanceof SqlJoin) {
                SqlJoin node = (SqlJoin) call;
                node.getLeft().accept(this);
                node.getRight().accept(this);
                return null;
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() == 1) {
                tables.add(id);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlDataTypeSpec type) {
            return null;
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            return null;
        }

        @Override
        public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }
    }
}
