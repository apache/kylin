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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.common.util.ThreadUtil;
import org.apache.kylin.guava30.shaded.common.base.Function;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.cache.CacheLoader;
import org.apache.kylin.guava30.shaded.common.cache.LoadingCache;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Ordering;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.IQueryTransformer;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConvertToComputedColumn implements IQueryTransformer {

    private static final String CONVERT_TO_CC_ERROR_MSG = "Something unexpected while ConvertToComputedColumn transforming the query, return original query.";

    private static final LoadingCache<String, String> transformExpressions = CacheBuilder.newBuilder()
            .maximumSize(10000).expireAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {
                @Override
                public String load(@Nonnull String cc) {
                    return QueryUtil.adaptCalciteSyntax(cc);
                }
            });

    @SneakyThrows
    private static String transformExpr(String expression) {
        return transformExpressions.get(expression);
    }

    private static List<SqlNode> collectInputNodes(SqlSelect select) {
        List<SqlNode> inputNodes = new LinkedList<>();
        inputNodes.addAll(collectCandidateInputNodes(select.getSelectList(), select.getGroup()));
        inputNodes.addAll(collectCandidateInputNodes(select.getOrderList(), select.getGroup()));
        if (select.getFrom() instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) select.getFrom();
            collectJoinNodes(inputNodes, join);
        }
        inputNodes.addAll(collectCandidateInputNode(select.getHaving(), select.getGroup()));
        inputNodes.addAll(getInputTreeNodes(select.getWhere()));
        inputNodes.addAll(getInputTreeNodes(select.getGroup()));
        return inputNodes;
    }

    private static void collectJoinNodes(List<SqlNode> inputNodes, SqlJoin join) {
        if (join.getLeft() instanceof SqlJoin) {
            collectJoinNodes(inputNodes, (SqlJoin) join.getLeft());
        }
        SqlNode condition = join.getCondition();
        if (condition != null && condition.isA(SqlKind.BINARY_COMPARISON)) {
            SqlBasicCall call = (SqlBasicCall) condition;
            call.getOperandList().stream().filter(node -> !isSimpleExpression(node)).forEach(inputNodes::add);
        }
        if (join.getRight() instanceof SqlJoin) {
            collectJoinNodes(inputNodes, (SqlJoin) join.getRight());
        }
    }

    private static List<SqlNode> collectInputNodes(SqlOrderBy sqlOrderBy) {
        // if order list is not empty and query is a select
        // then collect order list with checking on group keys
        List<SqlNode> inputNodes = new LinkedList<>();
        if (sqlOrderBy.orderList != null && sqlOrderBy.query instanceof SqlSelect
                && ((SqlSelect) sqlOrderBy.query).getGroup() != null) {
            inputNodes.addAll(
                    collectCandidateInputNodes(sqlOrderBy.orderList, ((SqlSelect) sqlOrderBy.query).getGroup()));
        } else {
            if (sqlOrderBy.orderList != null) {
                inputNodes.addAll(getInputTreeNodes(sqlOrderBy.orderList));
            }
        }
        return inputNodes;
    }

    private static List<SqlNode> collectCandidateInputNodes(SqlNodeList sqlNodeList, SqlNodeList groupSet) {
        List<SqlNode> inputNodes = new LinkedList<>();
        if (sqlNodeList == null) {
            return inputNodes;
        }
        for (SqlNode sqlNode : sqlNodeList) {
            inputNodes.addAll(collectCandidateInputNode(sqlNode, groupSet));
        }
        return inputNodes;
    }

    private static List<SqlNode> collectCandidateInputNode(SqlNode node, SqlNodeList groupSet) {
        List<SqlNode> inputNodes = new LinkedList<>();
        if (node == null) {
            return inputNodes;
        }
        // strip off AS clause
        if (node instanceof SqlCall && ((SqlCall) node).getOperator().kind == SqlKind.AS) {
            node = ((SqlCall) node).getOperandList().get(0);
        }
        // if agg node, replace with CC directly
        // otherwise the select node needs to be matched with group by nodes
        for (SqlNode sqlNode : getSelectNodesToReplace(node, groupSet)) {
            if (isSimpleExpression(sqlNode)) {
                continue;
            }
            inputNodes.addAll(getInputTreeNodes(sqlNode));
        }
        return inputNodes;
    }

    static boolean isSimpleExpression(SqlNode sqlNode) {
        return sqlNode == null || sqlNode instanceof SqlLiteral || sqlNode instanceof SqlIdentifier;
    }

    /**
     * Collect all the selected nodes: the node is an agg call, or the node is equal to any group key.
     * @param selectExp the selected SqlNode to be replaced
     * @param groupKeys all the group SqlNodes of the query SqlNode,
     *                  the raw query without aggregations has empty group keys.
     * @return the project SqlNode
     */
    private static List<SqlNode> getSelectNodesToReplace(SqlNode selectExp, SqlNodeList groupKeys) {
        // For non-raw queries with grouping expressions,
        // collect the projectNode only if it is included in the grouping expressions
        for (SqlNode groupNode : groupKeys) {
            if (selectExp.equalsDeep(groupNode, Litmus.IGNORE)) {
                return Collections.singletonList(selectExp);
            }
        }
        if (selectExp instanceof SqlCall) {
            SqlCall call = SqlFunctionUtil.resolveCallIfNeed(selectExp);
            if (call.getOperator() instanceof SqlAggFunction) {
                return Collections.singletonList(selectExp);
            } else {
                // iterate through sql call's operands
                // e.g.: case ... when
                return call.getOperandList().stream().filter(Objects::nonNull)
                        .map(node -> getSelectNodesToReplace(node, groupKeys)).flatMap(Collection::stream)
                        .collect(Collectors.toList());
            }
        } else if (selectExp instanceof SqlNodeList) {
            // iterate through select list
            return ((SqlNodeList) selectExp).getList().stream().filter(Objects::nonNull)
                    .map(node -> getSelectNodesToReplace(node, groupKeys)).flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private static List<SqlNode> getInputTreeNodes(SqlNode sqlNode) {
        if (sqlNode == null) {
            return Collections.emptyList();
        }
        SqlTreeVisitor stv = new SqlTreeVisitor();
        sqlNode.accept(stv);
        return stv.getSqlNodes();
    }

    static List<ComputedColumnDesc> getCCListSortByLength(List<ComputedColumnDesc> computedColumns) {
        if (computedColumns == null || computedColumns.isEmpty()) {
            return Lists.newArrayList();
        }

        Ordering<ComputedColumnDesc> ordering = Ordering.from(Comparator.comparingInt(String::length)).reverse() //
                .nullsLast().onResultOf(new Function<ComputedColumnDesc, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable ComputedColumnDesc input) {
                        return input == null ? null : input.getExpression();
                    }
                });

        return ordering.immutableSortedCopy(computedColumns);
    }

    @Override
    public String transform(String originSql, String project, String defaultSchema) {
        try {
            return transformImpl(originSql, project, defaultSchema);
        } catch (Exception e) {
            log.warn("{}, critical stackTrace:\n{}", CONVERT_TO_CC_ERROR_MSG, ThreadUtil.getKylinStackTrace());
            return originSql;
        }
    }

    public String transformImpl(String originSql, String project, String defaultSchema) throws SqlParseException {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        String sql = originSql;
        if (project != null && sql != null && projectConfig.isConvertExpressionToCcEnabled()) {
            List<NDataModel> models = NDataflowManager.getInstance(projectConfig, project).listOnlineDataModels()
                    .stream().filter(m -> !m.getComputedColumnDescs().isEmpty()).collect(Collectors.toList());
            if (!models.isEmpty()) {
                QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher(project, defaultSchema);
                int maxRecursionTimes = projectConfig.getConvertCcMaxIterations();
                for (int i = 0; i < maxRecursionTimes; i++) {
                    Pair<String, Boolean> replacedSqlAndState = transformImplRecursive(sql, queryAliasMatcher, models);
                    sql = replacedSqlAndState.getFirst();
                    if (Boolean.TRUE.equals(replacedSqlAndState.getSecond())) {
                        break;
                    }
                }
            }
        }

        return sql;
    }

    private Pair<String, Boolean> transformImplRecursive(String sql, QueryAliasMatcher queryAliasMatcher,
            List<NDataModel> models) throws SqlParseException {
        boolean recursionCompleted = true;
        List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
        Pair<String, Integer> choiceOfSubQuery = null; // <new sql, number of changes by the model>

        for (int i = 0; i < selectOrOrderbys.size(); i++) { // subQuery will precede
            if (choiceOfSubQuery != null) { // last selectOrOrderBy had a matched
                selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
            }

            SqlCall selectOrOrderBy = selectOrOrderbys.get(i);
            ComputedColumnReplacer ccReplacer = new ComputedColumnReplacer(queryAliasMatcher, models,
                    recursionCompleted, selectOrOrderBy);
            ccReplacer.replace(sql);
            recursionCompleted = ccReplacer.isRecursionCompleted();
            choiceOfSubQuery = ccReplacer.getChoiceForSubQuery();
            if (choiceOfSubQuery != null) {
                sql = choiceOfSubQuery.getFirst();
            }
        }

        return Pair.newPair(sql, recursionCompleted);
    }

    Pair<String, Integer> replaceComputedColumns(String inputSql, List<SqlNode> toMatchExpressions,
            List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo) {

        if (CollectionUtils.isEmpty(computedColumns)) {
            return Pair.newPair(inputSql, 0);
        }

        String result = inputSql;
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp;
        try {
            toBeReplacedExp = matchCcPosition(inputSql, toMatchExpressions, computedColumns, queryAliasMatchInfo);
        } catch (Exception e) {
            log.debug("Convert to computedColumn Fail,parse sql fail ", e);
            return Pair.newPair(inputSql, 0);
        }

        //replace user's input sql
        for (Pair<ComputedColumnDesc, Pair<Integer, Integer>> toBeReplaced : toBeReplacedExp) {
            Pair<Integer, Integer> startEndPos = toBeReplaced.getSecond();
            int start = startEndPos.getFirst();
            int end = startEndPos.getSecond();
            ComputedColumnDesc cc = toBeReplaced.getFirst();

            String alias;
            if (queryAliasMatchInfo.isModelView()) {
                // get alias with model alias
                // as table of cc in model view is model view table itself
                alias = queryAliasMatchInfo.getAliasMap().inverse().get(queryAliasMatchInfo.getModel().getAlias());
            } else {
                alias = queryAliasMatchInfo.getAliasMap().inverse().get(cc.getTableAlias());
            }
            if (alias == null) {
                throw new IllegalStateException(cc.getExpression() + " expression of cc " + cc.getFullName()
                        + " is found in query but its table ref " + cc.getTableAlias() + " is missing in query");
            }

            log.debug("Computed column: {} matching {} at [{},{}] using alias in query: {}", cc.getColumnName(),
                    inputSql.substring(start, end), start, end, alias);
            String aliasCcName = StringHelper.doubleQuote(alias) + "." + StringHelper.doubleQuote(cc.getColumnName());
            result = result.substring(0, start) + aliasCcName + result.substring(end);
        }
        try {
            SqlNode inputNodes = CalciteParser.parse(inputSql);
            int cntNodesBefore = getInputTreeNodes(inputNodes).size();
            SqlNode resultNodes = CalciteParser.parse(result);
            int cntNodesAfter = getInputTreeNodes(resultNodes).size();
            return Pair.newPair(result, cntNodesBefore - cntNodesAfter);
        } catch (SqlParseException e) {
            log.debug("Convert to computedColumn Fail, parse result sql fail: {}", result, e);
            return Pair.newPair(inputSql, 0);
        }
    }

    private List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> matchCcPosition(String inputSql,
            List<SqlNode> toMatchExpressions, List<ComputedColumnDesc> computedColumns,
            QueryAliasMatchInfo aliasMatchInfo) {
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp = new ArrayList<>();
        for (ComputedColumnDesc cc : computedColumns) {
            if (StringUtils.isBlank(cc.getExpression())) {
                continue;
            }
            List<SqlNode> matchedExpList = Lists.newArrayList();
            // match with the expression of the ComputedColumn
            SqlNode ccExpNode = CalciteParser.getExpNode(cc.getExpression());
            toMatchExpressions.stream().filter(inputNode -> isNodesEqual(aliasMatchInfo, ccExpNode, inputNode))
                    .forEach(matchedExpList::add);
            // match with the equivalent expression of the ComputedColumn
            if (StringUtils.isNotBlank(cc.getInnerExpression())) {
                String innerCcExp = transformExpr(cc.getInnerExpression());
                if (!Objects.equals(cc.getExpression(), innerCcExp)) {
                    SqlNode transformedNode = CalciteParser.getReadonlyExpNode(innerCcExp);
                    toMatchExpressions.stream()
                            .filter(inputNode -> isNodesEqual(aliasMatchInfo, transformedNode, inputNode))
                            .forEach(matchedExpList::add);
                }
            }

            for (SqlNode node : matchedExpList) {
                Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(node, inputSql);
                int start = startEndPos.getFirst();
                int end = startEndPos.getSecond();
                boolean conflict = toBeReplacedExp.stream().map(Pair::getSecond)
                        .anyMatch(replaced -> !(replaced.getFirst() >= end || replaced.getSecond() <= start));
                // overlap with chosen areas
                if (conflict) {
                    continue;
                }
                toBeReplacedExp.add(Pair.newPair(cc, Pair.newPair(start, end)));
            }
        }
        toBeReplacedExp.sort((o1, o2) -> o2.getSecond().getFirst().compareTo(o1.getSecond().getFirst()));
        return toBeReplacedExp;
    }

    List<SqlNode> collectLatentCcExpList(SqlCall selectOrOrderBy) {
        List<SqlNode> inputNodes = new LinkedList<>();
        if ("LENIENT".equals(KylinConfig.getInstanceFromEnv().getCalciteConformance())) {
            inputNodes = getInputTreeNodes(selectOrOrderBy);
        } else {
            // for select with group by, if the sql is like 'select expr(A) from tbl group by A'
            // do not replace expr(A) with CC
            if (selectOrOrderBy instanceof SqlSelect && ((SqlSelect) selectOrOrderBy).getGroup() != null) {
                inputNodes.addAll(collectInputNodes((SqlSelect) selectOrOrderBy));
            } else if (selectOrOrderBy instanceof SqlOrderBy) {
                SqlOrderBy sqlOrderBy = (SqlOrderBy) selectOrOrderBy;
                // 1. process order list
                inputNodes.addAll(collectInputNodes(sqlOrderBy));

                // 2. process query part
                // pass to getMatchedNodes directly
                if (sqlOrderBy.query instanceof SqlCall) {
                    inputNodes.addAll(collectLatentCcExpList((SqlCall) sqlOrderBy.query));
                } else {
                    inputNodes.addAll(getInputTreeNodes(sqlOrderBy.query));
                }
            } else {
                inputNodes = getInputTreeNodes(selectOrOrderBy);
            }
        }
        return inputNodes;
    }

    private boolean isNodesEqual(QueryAliasMatchInfo matchInfo, SqlNode ccExpressionNode, SqlNode inputNode) {
        if (matchInfo.isModelView()) {
            return ExpressionComparator.isNodeEqual(inputNode, ccExpressionNode,
                    new ModelViewSqlNodeComparator(matchInfo.getModel()));
        } else {
            return ExpressionComparator.isNodeEqual(inputNode, ccExpressionNode, matchInfo,
                    new AliasDeduceImpl(matchInfo));
        }
    }

    static class SqlTreeVisitor implements SqlVisitor<SqlNode> {
        private static final Set<String> AGG_FUNCTION_SET = ImmutableSet.of("COUNT", "SUM", "MIN", "MAX", "AVG",
                "CORR");
        private final List<SqlNode> sqlNodes;

        SqlTreeVisitor() {
            this.sqlNodes = new ArrayList<>();
        }

        List<SqlNode> getSqlNodes() {
            return sqlNodes;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (SqlNode node : nodeList) {
                node.accept(this);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if ((call instanceof SqlBasicCall || call instanceof SqlCase)
                    && !AGG_FUNCTION_SET.contains(call.getOperator().getName())) {
                sqlNodes.add(call);
            }
            if (call.getOperator() instanceof SqlAsOperator) {
                call.getOperator().acceptCall(this, call, true, SqlBasicVisitor.ArgHandlerImpl.instance());
            } else {
                for (SqlNode operand : call.getOperandList()) {
                    if (ConvertToComputedColumn.isSimpleExpression(operand)) {
                        continue;
                    }
                    operand.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
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

    private class ComputedColumnReplacer {
        private final QueryAliasMatcher queryAliasMatcher;
        private final List<NDataModel> dataModels;
        @Getter
        private boolean recursionCompleted;
        @Getter
        private Pair<String, Integer> choiceForSubQuery;
        private final SqlSelect sqlSelect;
        private final List<SqlNode> toMatchExpressions;

        ComputedColumnReplacer(QueryAliasMatcher queryAliasMatcher, List<NDataModel> dataModels,
                boolean recursionCompleted, SqlCall selectOrOrderBy) {
            this.queryAliasMatcher = queryAliasMatcher;
            this.dataModels = dataModels;
            this.recursionCompleted = recursionCompleted;
            this.sqlSelect = QueryUtil.extractSqlSelect(selectOrOrderBy);
            toMatchExpressions = collectLatentCcExpList(selectOrOrderBy);
        }

        public void replace(String sql) {
            if (sqlSelect == null) {
                return;
            }

            //give each data model a chance to rewrite, choose the model that generates most changes
            for (NDataModel model : dataModels) {
                // try match entire sub query with model
                QueryAliasMatchInfo info = queryAliasMatcher.match(model, sqlSelect);
                if (info == null) {
                    continue;
                }
                // move cc into replaceComputedColumn, good design?
                List<ComputedColumnDesc> computedColumns = getSortedComputedColumnWithModel(model);
                Pair<String, Integer> ret = replaceComputedColumns(sql, toMatchExpressions, computedColumns, info);

                if (!sql.equals(ret.getFirst())) {
                    choiceForSubQuery = ret;
                } else if (ret.getSecond() != 0 //
                        && (choiceForSubQuery == null || ret.getSecond() > choiceForSubQuery.getSecond())) {
                    choiceForSubQuery = ret;
                    recursionCompleted = false;
                }
            }
        }

        //match longer expressions first
        private List<ComputedColumnDesc> getSortedComputedColumnWithModel(NDataModel model) {
            List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
            KylinConfig projectConfig = NProjectManager.getProjectConfig(model.getProject());
            if (projectConfig.onlyReuseUserDefinedCC()) {
                ccList = ccList.stream().filter(cc -> !cc.isAutoCC()).collect(Collectors.toList());
            }
            return getCCListSortByLength(ccList);
        }
    }
}
