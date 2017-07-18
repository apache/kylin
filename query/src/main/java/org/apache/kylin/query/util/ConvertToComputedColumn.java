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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

public class ConvertToComputedColumn implements QueryUtil.IQueryTransformer {
    private static final Logger logger = LoggerFactory.getLogger(ConvertToComputedColumn.class);

    @Override
    public String transform(String sql, String project) {
        if (project == null) {
            return sql;
        }
        ImmutableSortedMap<String, String> computedColumns = getSortedComputedColumnWithProject(project);
        String s = replaceComputedColumn(sql, computedColumns);
        if (!StringUtils.equals(sql, s)) {
            logger.debug("sql changed");
        }
        return s;
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
                logger.error("Convert to computedColumn Fail,parse sql fail ", e);
                return inputSql;
            }
            for (SqlNode node : matchedNodes) {
                Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(node, lines);
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

    //Return matched node's position and its alias(if exists).If can not find matches, return an empty capacity list
    private static List<SqlNode> getMatchedNodes(String inputSql, String ccExp) throws SqlParseException {
        if (ccExp == null || ccExp.equals("")) {
            return new ArrayList<>();
        }
        ArrayList<SqlNode> toBeReplacedNodes = new ArrayList<>();
        SqlNode ccNode = CalciteParser.getExpNode(ccExp);
        List<SqlNode> inputNodes = getInputTreeNodes(inputSql);

        // find whether user input sql's tree node equals computed columns's define expression
        for (SqlNode inputNode : inputNodes) {
            if (CalciteParser.isNodeEqual(inputNode, ccNode)) {
                toBeReplacedNodes.add(inputNode);
            }
        }
        return toBeReplacedNodes;
    }

    private static List<SqlNode> getInputTreeNodes(String inputSql) throws SqlParseException {
        SqlTreeVisitor stv = new SqlTreeVisitor();
        CalciteParser.parse(inputSql).accept(stv);
        return stv.getSqlNodes();
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

        Map<String, String> computedColumns = new HashMap<>();

        MetadataManager metadataManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<DataModelDesc> dataModelDescs = metadataManager.getModels(project);
        for (DataModelDesc dataModelDesc : dataModelDescs) {
            for (ComputedColumnDesc computedColumnDesc : dataModelDesc.getComputedColumnDescs()) {
                computedColumns.put(computedColumnDesc.getColumnName(), computedColumnDesc.getExpression());
            }
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
