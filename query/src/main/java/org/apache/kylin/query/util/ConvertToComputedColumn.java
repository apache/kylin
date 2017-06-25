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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

public class ConvertToComputedColumn implements QueryUtil.IQueryTransformer {
    private static final Logger logger = LoggerFactory.getLogger(ConvertToComputedColumn.class);

    @Override
    public String transform(String sql, String project) {
        if (project == null) {
            return sql;
        }
        ImmutableSortedMap<String, String> computedColumns = getSortedComputedColumnWithProject(project);
        return replaceComputedColumn(sql, computedColumns);
    }

    static String replaceComputedColumn(String inputSql, ImmutableSortedMap<String, String> computedColumn) {
        if (inputSql == null) {
            return "";
        }

        if (computedColumn == null || computedColumn.isEmpty()) {
            return inputSql;
        }
        String result = inputSql;
        String[] lines = inputSql.split("\n");
        List<Pair<String, String>> toBeReplacedExp = new ArrayList<>(); //{"alias":"expression"}, like {"t1":"t1.a+t1.b+t1.c"}

        for (String ccExp : computedColumn.keySet()) {
            List<SqlNode> matchedNodes = new ArrayList<>();
            try {
                matchedNodes = getMatchedNodes(inputSql, computedColumn.get(ccExp));
            } catch (SqlParseException e) {
                logger.error("Convert to computedColumn Fail,parse sql fail ", e.getMessage());
            }
            for (SqlNode node : matchedNodes) {
                Pair<Integer, Integer> startEndPos = getReplacePos(lines, node);
                int start = startEndPos.getLeft();
                int end = startEndPos.getRight();
                //add table alias like t1.column,if exists alias
                String alias = getTableAlias(node);
                toBeReplacedExp.add(Pair.of(alias, inputSql.substring(start, end)));
            }
            logger.debug("Computed column: " + ccExp + "'s matched list:" + toBeReplacedExp);
            //replace user's input sql
            for (Pair<String, String> toBeReplaced : toBeReplacedExp) {
                result = result.replace(toBeReplaced.getRight(), toBeReplaced.getLeft() + ccExp);
            }
        }
        return result;
    }

    private static Pair<Integer, Integer> getReplacePos(String[] lines, SqlNode node) {
        SqlParserPos pos = node.getParserPosition();
        int lineStart = pos.getLineNum();
        int columnStart = pos.getColumnNum() - 1;
        int columnEnd = pos.getEndColumnNum();
        //for the case that sql is multi lines
        for (int i = 0; i < lineStart - 1; i++) {
            int offset = lines[i].length();
            columnStart += offset + 1;
            columnEnd += offset + 1;
        }
        return Pair.of(columnStart, columnEnd);
    }

    //Return matched node's position and its alias(if exists).If can not find matches, return an empty capacity list
    private static List<SqlNode> getMatchedNodes(String inputSql, String ccExp) throws SqlParseException {
        if (ccExp == null || ccExp.equals("")) {
            return new ArrayList<>();
        }
        ArrayList<SqlNode> toBeReplacedNodes = new ArrayList<>();
        SqlNode ccNode = getCCExpNode(ccExp);
        List<SqlNode> inputNodes = getInputTreeNodes(inputSql);

        // find whether user input sql's tree node equals computed columns's define expression
        for (SqlNode inputNode : inputNodes) {
            if (isNodeEqual(inputNode, ccNode)) {
                toBeReplacedNodes.add(inputNode);
            }
        }
        return toBeReplacedNodes;
    }

    private static List<SqlNode> getInputTreeNodes(String inputSql) throws SqlParseException {
        SqlTreeVisitor stv = new SqlTreeVisitor();
        parse(inputSql).accept(stv);
        return stv.getSqlNodes();
    }

    private static SqlNode getCCExpNode(String ccExp) throws SqlParseException {
        ccExp = "select " + ccExp + " from t";
        return ((SqlSelect) parse(ccExp)).getSelectList().get(0);
    }

    static SqlNode parse(String sql) throws SqlParseException {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder();
        SqlParser sqlParser = SqlParser.create(sql, parserBuilder.build());
        return sqlParser.parseQuery();
    }

    static boolean isNodeEqual(SqlNode node0, SqlNode node1) {
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

        logger.error("Convert to computed column fail,failed to compare two nodes,unknown instance type");
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

    private static String getTableAlias(SqlNode node) {
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            return getTableAlias(call.getOperandList());
        }
        if (node instanceof SqlIdentifier) {
            StringBuilder alias = new StringBuilder("");
            ImmutableList<String> names = ((SqlIdentifier) node).names;
            if (names.size() >= 2) {
                for (int i = 0; i < names.size() - 1; i++) {
                    alias.append(names.get(i)).append(".");
                }
            }
            return alias.toString();
        }
        if (node instanceof SqlNodeList) {
            return "";
        }
        if (node instanceof SqlLiteral) {
            return "";
        }
        return "";
    }

    private static String getTableAlias(List<SqlNode> operands) {
        for (SqlNode operand : operands) {
            return getTableAlias(operand);
        }
        return "";
    }

    private ImmutableSortedMap<String, String> getSortedComputedColumnWithProject(String project) {
        MetadataManager metadataManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        Map<String, MetadataManager.CCInfo> ccInfoMap = metadataManager.getCcInfoMap();
        final ProjectInstance projectInstance = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);

        Iterable<MetadataManager.CCInfo> projectCCInfo = Iterables.filter(ccInfoMap.values(),
                new Predicate<MetadataManager.CCInfo>() {
                    @Override
                    public boolean apply(@Nullable MetadataManager.CCInfo ccInfo) {
                        return Iterables.any(ccInfo.getDataModelDescs(), new Predicate<DataModelDesc>() {
                            @Override
                            public boolean apply(@Nullable DataModelDesc model) {
                                return projectInstance.containsModel(model.getName());
                            }
                        });
                    }
                });

        Map<String, String> computedColumns = new HashMap<>();
        for (MetadataManager.CCInfo ccInfo : projectCCInfo) {
            computedColumns.put(ccInfo.getComputedColumnDesc().getColumnName(),
                    ccInfo.getComputedColumnDesc().getExpression());
        }

        return getMapSortedByValue(computedColumns);
    }

    static ImmutableSortedMap<String, String> getMapSortedByValue(Map<String, String> computedColumns) {
        if (computedColumns == null || computedColumns.isEmpty()) {
            return null;
        }

        Ordering<String> ordering = Ordering.from(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.compare(o1.replaceAll("\\s*", "").length(), o2.replaceAll("\\s*", "").length());
            }
        }).reverse().nullsLast().onResultOf(Functions.forMap(computedColumns, null)).compound(Ordering.natural());
        return ImmutableSortedMap.copyOf(computedColumns, ordering);
    }

}

class SqlTreeVisitor implements SqlVisitor<SqlNode> {
    private List<SqlNode> sqlNodes;

    SqlTreeVisitor() {
        this.sqlNodes = new ArrayList<>();
    }

    List<SqlNode> getSqlNodes() {
        return sqlNodes;
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        sqlNodes.add(nodeList);
        for (int i = 0; i < nodeList.size(); i++) {
            SqlNode node = nodeList.get(i);
            node.accept(this);
        }
        return null;
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        sqlNodes.add(literal);
        return null;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        sqlNodes.add(call);
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null) {
                operand.accept(this);
            }
        }
        return null;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        sqlNodes.add(id);
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