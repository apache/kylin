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

package org.apache.kylin.query.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

public class RowFilter implements IQueryTransformer, IPushDownConverter {
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    static boolean needEscape(String sql, String defaultSchema, Map<String, String> cond) {
        return StringUtils.isEmpty(defaultSchema) //
                || StringUtils.isEmpty(sql) //
                || !StringUtils.containsIgnoreCase(sql, "from") //
                || cond.isEmpty(); //
    }

    static String whereClauseBracketsCompletion(String schema, String inputSQL, Set<String> candidateTables) {
        return whereClauseBracketsCompletion(schema, inputSQL, candidateTables, null);
    }

    static String whereClauseBracketsCompletion(String schema, String inputSQL, Set<String> candidateTables,
            String project) {
        Map<SqlSelect, List<Table>> selectClausesWithTbls = getSelectClausesWithTbls(inputSQL, schema, project);
        List<Pair<Integer, String>> toBeInsertedPosAndExprs = new ArrayList<>();

        for (Map.Entry<SqlSelect, List<Table>> select : selectClausesWithTbls.entrySet()) {
            if (!select.getKey().hasWhere()) {
                continue;
            }

            for (Table table : select.getValue()) {
                if (candidateTables.contains(table.getName())) {
                    Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(select.getKey().getWhere(),
                            inputSQL);
                    toBeInsertedPosAndExprs.add(Pair.newPair(replacePos.getFirst(), "("));
                    toBeInsertedPosAndExprs.add(Pair.newPair(replacePos.getSecond(), ")"));
                    break;
                }
            }
        }
        return afterInsertSQL(inputSQL, toBeInsertedPosAndExprs);
    }

    static String rowFilter(String schema, String inputSQL, Map<String, String> whereCondWithTbls) {
        return rowFilter(schema, inputSQL, whereCondWithTbls, null);
    }

    static String rowFilter(String schema, String inputSQL, Map<String, String> whereCondWithTbls, String project) {
        Map<SqlSelect, List<Table>> selectClausesWithTbls = getSelectClausesWithTbls(inputSQL, schema, project);
        List<Pair<Integer, String>> toBeInsertedPosAndExprs = getInsertPosAndExpr(inputSQL, whereCondWithTbls,
                selectClausesWithTbls);
        return afterInsertSQL(inputSQL, toBeInsertedPosAndExprs);
    }

    private static String afterInsertSQL(String inputSQL, List<Pair<Integer, String>> toBeInsertedPosAndExprs) {
        // latter replace position in the front of the list.
        Collections.sort(toBeInsertedPosAndExprs,
                (Pair<Integer, String> o1, Pair<Integer, String> o2) -> -(o1.getFirst() - o2.getFirst()));

        StrBuilder convertedSQL = new StrBuilder(inputSQL);
        for (Pair<Integer, String> toBeInserted : toBeInsertedPosAndExprs) {
            int insertPos = toBeInserted.getFirst();
            convertedSQL.insert(insertPos, toBeInserted.getSecond());
        }
        return convertedSQL.toString();
    }

    // concat all table's row ACL defined condition and insert to user's inputSQL as where clause.
    // return [insertPos : toBeInsertExpr]
    private static List<Pair<Integer, String>> getInsertPosAndExpr(String inputSQL,
            Map<String, String> whereCondWithTbls, Map<SqlSelect, List<Table>> selectClausesWithTbls) {

        List<Pair<Integer, String>> toBeReplacedPosAndExprs = new ArrayList<>();

        for (Map.Entry<SqlSelect, List<Table>> select : selectClausesWithTbls.entrySet()) {
            int insertPos = getInsertPos(inputSQL, select.getKey());

            //Will concat one select clause's all tables's row ACL conditions into one where clause.
            List<Table> tables = select.getValue();
            String whereCond = getToBeInsertCond(whereCondWithTbls, select.getKey(), tables);

            if (!whereCond.isEmpty()) {
                toBeReplacedPosAndExprs.add(Pair.newPair(insertPos, whereCond));
            }
        }

        return toBeReplacedPosAndExprs;
    }

    private static int getInsertPos(String inputSQL, SqlSelect select) {
        SqlNode insertAfter = getInsertAfterNode(select);
        Pair<Integer, Integer> pos = CalciteParser.getReplacePos(insertAfter, inputSQL);
        int finalPos = pos.getSecond();
        int bracketNum = 0;
        //move the pos to the rightest ")", if has.
        //bracketNum if for the situation like that: "from ( select * from t where t.a > 0 )", the rightest ")" is not belong to where clause
        for (int j = pos.getFirst() - 1;; j--) {
            if (inputSQL.charAt(j) == ' ' || inputSQL.charAt(j) == '\t' || inputSQL.charAt(j) == '\n') {
                continue;
            } else if (inputSQL.charAt(j) == '(') {
                bracketNum++;
            } else {
                break;
            }
        }

        for (int i = pos.getSecond(); i < inputSQL.length() && bracketNum > 0; i++) {
            if (inputSQL.charAt(i) == ' ' || inputSQL.charAt(i) == '\t' || inputSQL.charAt(i) == '\n') {
                continue;
            } else if (inputSQL.charAt(i) == ')') {
                finalPos = i + 1;
                bracketNum--;
            } else {
                break;
            }
        }
        return finalPos;
    }

    private static String getToBeInsertCond(Map<String, String> whereCondWithTbls, SqlSelect select,
            List<Table> tables) {
        StringBuilder whereCond = new StringBuilder();
        boolean isHeadCond = true;
        for (Table table : tables) {
            String cond = whereCondWithTbls.get(table.getName());
            if (StringUtils.isEmpty(cond)) {
                continue;
            }

            //complete condition expr with alias
            cond = CalciteParser.insertAliasInExpr(cond, table.getAlias());
            if (isHeadCond && !select.hasWhere()) {
                whereCond = new StringBuilder(" WHERE " + cond);
                isHeadCond = false;
            } else {
                whereCond.append(" AND ").append(cond);
            }
        }
        return whereCond.toString();
    }

    private static SqlNode getInsertAfterNode(SqlSelect select) {
        SqlNode rightMost;
        if (!select.hasWhere()) {
            //CALCITE-1973 get right node's pos instead of from's pos.In KYLIN, join must have on operator
            if (select.getFrom() instanceof SqlJoin) {
                rightMost = Preconditions.checkNotNull(((SqlJoin) select.getFrom()).getCondition(),
                        "Join without \"ON\"");
            } else {
                //if inputSQL doesn't have where clause, concat where clause after from clause.
                rightMost = select.getFrom();
            }
        } else {
            rightMost = select.getWhere();
        }
        return rightMost;
    }

    // '{selectClause1:[DB.TABLE1:ALIAS1, DB.TABLE2:ALIAS2]}'
    private static Map<SqlSelect, List<Table>> getSelectClausesWithTbls(String inputSQL, String schema,
            String project) {
        Map<SqlSelect, List<Table>> selectWithTables = new HashMap<>();

        for (SqlSelect select : SelectClauseFinder.getSelectClauses(inputSQL, project)) {
            List<Table> tblsWithAlias = getTblWithAlias(schema, select);
            if (tblsWithAlias.size() > 0) {
                selectWithTables.put(select, tblsWithAlias);
            }
        }
        return selectWithTables;
    }

    static List<Table> getTblWithAlias(String schema, SqlSelect select) {
        List<Table> tblsWithAlias = NonSubqueryTablesFinder.getTblsWithAlias(select.getFrom());

        // complete table with database schema if table hasn't
        for (int i = 0; i < tblsWithAlias.size(); i++) {
            Table t = tblsWithAlias.get(i);
            if (t.getName().split("\\.").length == 1) {
                tblsWithAlias.set(i, new Table(schema + "." + t.getName(), t.getAlias()));
            }
        }
        return tblsWithAlias;
    }

    private static boolean hasAdminPermission(QueryContext.AclInfo aclInfo) {
        if (Objects.isNull(aclInfo) || Objects.isNull(aclInfo.getGroups())) {
            return false;
        }
        return aclInfo.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals) || aclInfo.isHasAdminPermission();
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return transform(originSql, project, defaultSchema);
    }

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        QueryContext.AclInfo aclLocal = QueryContext.current().getAclInfo();
        if (!KylinConfig.getInstanceFromEnv().isAclTCREnabled() || hasAdminPermission(aclLocal)) {
            return sql;
        }

        Map<String, String> allWhereCondWithTbls = getAllWhereCondWithTbls(project, aclLocal);
        if (needEscape(sql, defaultSchema, allWhereCondWithTbls)) {
            return sql;
        }

        logger.debug("\nStart to transform SQL with row ACL\n");
        // if origin SQL has where clause, add "()", see KAP#2873
        sql = whereClauseBracketsCompletion(defaultSchema, sql, getCandidateTables(allWhereCondWithTbls), project);

        sql = rowFilter(defaultSchema, sql, allWhereCondWithTbls, project);

        logger.debug("\nFinish transforming SQL with row ACL.\n");
        return sql;
    }

    private Set<String> getCandidateTables(Map<String, String> allWhereCondWithTbls) {
        Set<String> candidateTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        candidateTables.addAll(allWhereCondWithTbls.keySet());
        return candidateTables;
    }

    //get all user/groups's row ACL
    private Map<String, String> getAllWhereCondWithTbls(String project, QueryContext.AclInfo aclInfo) {
        String user = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
        Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
        return AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getTableColumnConcatWhereCondition(user, groups);
    }

    /*visitor classes.Get all select nodes, include select clause in subquery*/
    static class SelectClauseFinder extends SqlBasicVisitor<SqlNode> {
        private List<SqlSelect> selects;

        SelectClauseFinder() {
            this.selects = new ArrayList<>();
        }

        static List<SqlSelect> getSelectClauses(String inputSQL, String project) {
            SqlNode node = null;
            try {
                node = CalciteParser.parse(inputSQL, project);
            } catch (SqlParseException e) {
                throw new KylinRuntimeException(
                        "Failed to parse SQL \'" + inputSQL + "\', please make sure the SQL is valid");
            }
            SelectClauseFinder sv = new SelectClauseFinder();
            node.accept(sv);
            return sv.getSelectClauses();
        }

        private List<SqlSelect> getSelectClauses() {
            return selects;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                node.accept(this);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                selects.add(select);
            }
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }
    }

    /*visitor classes.Get select clause 's all tablesWithAlias and skip the subquery*/
    static class NonSubqueryTablesFinder extends SqlBasicVisitor<SqlNode> {
        // '{table:alias,...}'
        private List<Table> tablesWithAlias;

        private NonSubqueryTablesFinder() {
            this.tablesWithAlias = new ArrayList<>();
        }

        //please pass SqlSelect.getFrom.Pass other sql nodes will lead error.
        static List<Table> getTblsWithAlias(SqlNode fromNode) {
            NonSubqueryTablesFinder sv = new NonSubqueryTablesFinder();
            fromNode.accept(sv);
            return sv.getTblsWithAlias();
        }

        private List<Table> getTblsWithAlias() {
            return tablesWithAlias;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            // skip subquery in the from clause
            if (call instanceof SqlSelect) {
                // do nothing.
            } else if (call instanceof SqlBasicCall) {
                // for the case table alias like "from t t1".The only SqlBasicCall in from clause is "AS".
                // the instanceof SqlIdentifier is for the case that "select * from (select * from t2) t1".subquery as table.
                SqlBasicCall node = (SqlBasicCall) call;
                if (node.getOperator() instanceof SqlAsOperator && node.getOperands()[0] instanceof SqlIdentifier) {
                    SqlIdentifier id0 = (SqlIdentifier) ((SqlBasicCall) call).getOperands()[0];
                    SqlIdentifier id1 = (SqlIdentifier) ((SqlBasicCall) call).getOperands()[1];
                    String table = id0.toString(); //DB.TABLE OR TABLE
                    String alais = CalciteParser.getLastNthName(id1, 1);
                    tablesWithAlias.add(new Table(table, alais));
                }
            } else if (call instanceof SqlJoin) {
                SqlJoin node = (SqlJoin) call;
                node.getLeft().accept(this);
                node.getRight().accept(this);
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
            // if table has no alias, will come into this method.Put table name as alias
            String[] dotSplits = id.toString().toUpperCase(Locale.ROOT).split("\\.");
            String table = dotSplits[dotSplits.length - 1];
            tablesWithAlias.add(new Table(id.toString().toUpperCase(Locale.ROOT), table));
            return null;
        }
    }

    //immutable class only for replacing Pair<String, String> for tableWithAlias
    static class Table {
        private String name;
        private String alias;

        public Table(String name, String alias) {
            this.name = name;
            this.alias = alias;
        }

        public void setTable(Pair<String, String> tableWithAlias) {
            this.name = tableWithAlias.getFirst();
            this.alias = tableWithAlias.getSecond();
        }

        public String getName() {
            return name;
        }

        public String getAlias() {
            return alias;
        }
    }
}
