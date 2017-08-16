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

package org.apache.kylin.metadata.model.tool;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CalciteParser {
    public static SqlNode parse(String sql) throws SqlParseException {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder();
        SqlParser sqlParser = SqlParser.create(sql, parserBuilder.build());
        return sqlParser.parseQuery();
    }

    public static SqlNode getOnlySelectNode(String sql) {
        SqlNodeList selectList = null;
        try {
            selectList = ((SqlSelect) CalciteParser.parse(sql)).getSelectList();
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse expression \'" + sql
                    + "\', please make sure the expression is valid and no table name exists in the expression");
        }

        Preconditions.checkArgument(selectList.size() == 1,
                "Expression is invalid because size of select list exceeds one");

        return selectList.get(0);
    }

    public static SqlNode getExpNode(String expr) {
        return getOnlySelectNode("select " + expr + " from t");

    }

    public static SqlNode getFromNode(String sql) {
        SqlNode node = null;
        try {
            node = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            throw new RuntimeException(
                    "Failed to parse expression \'" + sql + "\', please make sure the expression is valid");
        }
        //When the sql have limit clause, calcite will parse it as a SqlOrder Object.
        SqlNode fromNode = null;
        if (node instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) node;
            fromNode = ((SqlSelect) orderBy.query).getFrom();
        } else {
            SqlSelect sqlSelect = (SqlSelect) node;
            fromNode = sqlSelect.getFrom();
        }
        return fromNode;
    }

    public static boolean isNodeEqual(SqlNode node0, SqlNode node1) {
        if (node0 == null) {
            return node1 == null;
        } else if (node1 == null) {
            return false;
        }

        if (!Objects.equals(node0.getClass().getSimpleName(), node1.getClass().getSimpleName())) {
            return false;
        }

        if (node0 instanceof SqlCall) {
            SqlCall thisNode = (SqlCall) node0;
            SqlCall thatNode = (SqlCall) node1;
            if (!thisNode.getOperator().getName().equalsIgnoreCase(thatNode.getOperator().getName())) {
                return false;
            }
            return isNodeEqual(thisNode.getOperandList(), thatNode.getOperandList());
        }
        if (node0 instanceof SqlLiteral) {
            SqlLiteral thisNode = (SqlLiteral) node0;
            SqlLiteral thatNode = (SqlLiteral) node1;
            return Objects.equals(thisNode.getValue(), thatNode.getValue());
        }
        if (node0 instanceof SqlNodeList) {
            SqlNodeList thisNode = (SqlNodeList) node0;
            SqlNodeList thatNode = (SqlNodeList) node1;
            if (thisNode.getList().size() != thatNode.getList().size()) {
                return false;
            }
            for (int i = 0; i < thisNode.getList().size(); i++) {
                SqlNode thisChild = thisNode.getList().get(i);
                final SqlNode thatChild = thatNode.getList().get(i);
                if (!isNodeEqual(thisChild, thatChild)) {
                    return false;
                }
            }
            return true;
        }
        if (node0 instanceof SqlIdentifier) {
            SqlIdentifier thisNode = (SqlIdentifier) node0;
            SqlIdentifier thatNode = (SqlIdentifier) node1;
            // compare ignore table alias.eg: expression like "a.b + a.c + a.d" ,alias a will be ignored when compared
            String name0 = thisNode.names.get(thisNode.names.size() - 1).replace("\"", "");
            String name1 = thatNode.names.get(thatNode.names.size() - 1).replace("\"", "");
            return name0.equalsIgnoreCase(name1);
        }

        return false;
    }

    private static boolean isNodeEqual(List<SqlNode> operands0, List<SqlNode> operands1) {
        if (operands0.size() != operands1.size()) {
            return false;
        }
        for (int i = 0; i < operands0.size(); i++) {
            if (!isNodeEqual(operands0.get(i), operands1.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void ensureNoTableNameExists(String expr) {
        SqlNode sqlNode = getExpNode(expr);

        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                if (id.names.size() > 1) {
                    throw new IllegalArgumentException("SqlIdentifier " + id + " contains DB/Table name");
                }
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
    }

    public static String insertAliasInExpr(String expr, String alias) {
        String prefix = "select ";
        String suffix = " from t";
        String sql = prefix + expr + suffix;
        SqlNode sqlNode = getOnlySelectNode(sql);

        final Set<SqlIdentifier> s = Sets.newHashSet();
        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                if (id.names.size() > 1) {
                    throw new IllegalArgumentException("SqlIdentifier " + id + " contains DB/Table name");
                }
                s.add(id);
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        List<SqlIdentifier> sqlIdentifiers = Lists.newArrayList(s);

        Collections.sort(sqlIdentifiers, new Comparator<SqlIdentifier>() {
            @Override
            public int compare(SqlIdentifier o1, SqlIdentifier o2) {
                int linegap = o2.getParserPosition().getLineNum() - o1.getParserPosition().getLineNum();
                if (linegap != 0)
                    return linegap;

                return o2.getParserPosition().getColumnNum() - o1.getParserPosition().getColumnNum();
            }
        });

        for (SqlIdentifier sqlIdentifier : sqlIdentifiers) {
            Pair<Integer, Integer> replacePos = getReplacePos(sqlIdentifier, sql);
            int start = replacePos.getLeft();
            sql = sql.substring(0, start) + alias + "." + sql.substring(start);
        }

        return sql.substring(prefix.length(), sql.length() - suffix.length());
    }

    public static Pair<Integer, Integer> getReplacePos(SqlNode node, String inputSql) {
        if (inputSql == null) {
            return Pair.of(0, 0);
        }
        String[] lines = inputSql.split("\n");
        SqlParserPos pos = node.getParserPosition();
        int lineStart = pos.getLineNum();
        int lineEnd = pos.getEndLineNum();
        int columnStart = pos.getColumnNum() - 1;
        int columnEnd = pos.getEndColumnNum();
        //for the case that sql is multi lines
        for (int i = 0; i < lineStart - 1; i++) {
            columnStart += lines[i].length() + 1;
        }
        for (int i = 0; i < lineEnd - 1; i++) {
            columnEnd += lines[i].length() + 1;
        }
        //for calcite's bug CALCITE-1875
        Pair<Integer, Integer> startEndPos = getPosWithBracketsCompletion(inputSql, columnStart, columnEnd);
        return startEndPos;
    }

    private static Pair<Integer, Integer> getPosWithBracketsCompletion(String inputSql, int left, int right) {
        int leftBracketNum = 0;
        int rightBracketNum = 0;
        String substring = inputSql.substring(left, right);
        for (int i = 0; i < substring.length(); i++) {
            char temp = substring.charAt(i);
            if (temp == '(') {
                leftBracketNum++;
            }
            if (temp == ')') {
                rightBracketNum++;
                if (leftBracketNum < rightBracketNum) {
                    while ('(' != inputSql.charAt(left - 1)) {
                        left--;
                    }
                    left--;
                    leftBracketNum++;
                }
            }
        }
        while (rightBracketNum < leftBracketNum) {
            while (')' != inputSql.charAt(right)) {
                right++;
            }
            right++;
            rightBracketNum++;
        }
        return Pair.of(left, right);
    }
}
