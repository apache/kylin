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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.model.tool.CalciteParser;

//find child inner select first
public class SqlSubqueryFinder extends SqlBasicVisitor<SqlNode> {
    private List<SqlCall> sqlSelectsOrOrderbys;
    private List<SqlIdentifier> subqueryAlias;
    private boolean includeNestedQueries = false;

    SqlSubqueryFinder() {
        this.sqlSelectsOrOrderbys = new ArrayList<>();
        this.subqueryAlias = new ArrayList<>();
    }

    public SqlSubqueryFinder(boolean includeNestedQueries) {
        this();
        this.includeNestedQueries = includeNestedQueries;
    }

    public static List<SqlCall> getSubqueries(String sql) throws SqlParseException {
        return getSubqueries(sql, false);
    }

    public static List<SqlCall> getSubqueries(String sql, boolean includeNestedQueries) throws SqlParseException {
        SqlNode parsed = CalciteParser.parse(sql);
        SqlSubqueryFinder sqlSubqueryFinder = new SqlSubqueryFinder(includeNestedQueries);
        parsed.accept(sqlSubqueryFinder);
        return sqlSubqueryFinder.getSqlSelectsOrOrderbys();
    }

    //subquery will precede
    List<SqlCall> getSqlSelectsOrOrderbys() {
        return sqlSelectsOrOrderbys;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null) {
                operand.accept(this);
            }
        }
        if (call instanceof SqlSelect && ((SqlSelect) call).getFrom() != null) {
            SqlSelect select = (SqlSelect) call;
            RootTableValidator validator = new RootTableValidator();
            select.getFrom().accept(validator);
            if (validator.hasRoot) {
                sqlSelectsOrOrderbys.add(call);
            }
        }
        if (call instanceof SqlWithItem) {
            SqlWithItem sqlWithQuery = (SqlWithItem) call;
            subqueryAlias.add(sqlWithQuery.name);
        }
        if (includeNestedQueries && SqlKind.UNION == call.getKind()) {
            sqlSelectsOrOrderbys.add(call);
        }

        if (call instanceof SqlOrderBy) {
            SqlCall sqlCall = sqlSelectsOrOrderbys.get(sqlSelectsOrOrderbys.size() - 1);
            SqlNode query = ((SqlOrderBy) call).query;
            if (query instanceof SqlWith) {
                query = ((SqlWith) query).body;
            }
            if (query instanceof SqlBasicCall && SqlKind.UNION == query.getKind()) {
                for (SqlNode operand : ((SqlBasicCall) query).getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            } else {
                RootTableValidator validator = new RootTableValidator();
                ((SqlSelect) query).getFrom().accept(validator);
                if (validator.hasRoot) {
                    Preconditions.checkState(query == sqlCall);
                    sqlSelectsOrOrderbys.set(sqlSelectsOrOrderbys.size() - 1, call);
                }
            }
        }
        return null;
    }

    private class RootTableValidator extends SqlBasicVisitor<SqlNode> {

        private boolean hasRoot = true;

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                if (includeNestedQueries) {
                    SqlNodeList list = ((SqlSelect) call).getSelectList();
                    hasRoot = list.get(0).toString().equals("*");
                } else {
                    hasRoot = false; // false if a nested select is found
                }
            } else if (call instanceof SqlJoin) {
                ((SqlJoin) call).getLeft().accept(this);
            } else if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                call.getOperandList().get(0).accept(this);
            } else {
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            }

            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            for (SqlIdentifier alias : subqueryAlias) {
                if (alias.equalsDeep(id, Litmus.IGNORE)) {
                    hasRoot = false;
                    break;
                }
            }
            return null;
        }
    }
}
