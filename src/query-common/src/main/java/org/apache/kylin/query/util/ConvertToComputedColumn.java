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

import javax.annotation.Nullable;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ThreadUtil;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.IQueryTransformer;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import io.kyligence.kap.guava20.shaded.common.cache.CacheBuilder;
import io.kyligence.kap.guava20.shaded.common.cache.CacheLoader;
import io.kyligence.kap.guava20.shaded.common.cache.LoadingCache;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConvertToComputedColumn implements IQueryTransformer {

    private static final String CONVERT_TO_CC_ERROR_MSG = "Something unexpected while ConvertToComputedColumn transforming the query, return original query.";
    private static final String DOUBLE_QUOTE = Quoting.DOUBLE_QUOTE.string;

    private static final LoadingCache<String, String> transformExpressions = CacheBuilder.newBuilder()
            .maximumSize(10000).expireAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {
                @Override
                public String load(String cc) {
                    return new EscapeTransformer().transform(cc, null, null);
                }
            });

    static Pair<String, Integer> replaceComputedColumn(String inputSql, SqlCall selectOrOrderby,
            List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo) {
        return new ConvertToComputedColumn().replaceComputedColumn(inputSql, selectOrOrderby, computedColumns,
                queryAliasMatchInfo, false);
    }

    @SneakyThrows
    private static String transformExpr(String expression) {
        return transformExpressions.get(expression);
    }

    private static List<SqlNode> collectInputNodes(SqlSelect select) {
        List<SqlNode> inputNodes = new LinkedList<>();
        inputNodes.addAll(collectCandidateInputNodes(select.getSelectList(), select.getGroup()));
        inputNodes.addAll(collectCandidateInputNodes(select.getOrderList(), select.getGroup()));
        inputNodes.addAll(collectCandidateInputNode(select.getHaving(), select.getGroup()));
        inputNodes.addAll(getInputTreeNodes(select.getWhere()));
        inputNodes.addAll(getInputTreeNodes(select.getGroup()));
        return inputNodes;
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
            inputNodes.addAll(getInputTreeNodes(sqlNode));
        }
        return inputNodes;
    }

    /**
     * collect all select nodes that
     * 1. is a agg call
     * 2. is equal to any group key
     * @param selectNode
     * @param groupKeys
     * @return
     */
    private static List<SqlNode> getSelectNodesToReplace(SqlNode selectNode, SqlNodeList groupKeys) {
        for (SqlNode groupNode : groupKeys) {
            // for non-agg select node, collect it only when there is a equal node in the group set
            if (selectNode.equalsDeep(groupNode, Litmus.IGNORE)) {
                return Collections.singletonList(selectNode);
            }
        }
        if (selectNode instanceof SqlCall) {
            if (((SqlCall) selectNode).getOperator() instanceof SqlAggFunction) {
                // collect agg node directly
                return Collections.singletonList(selectNode);
            } else {
                // iterate through sql call's operands
                // eg case .. when
                return ((SqlCall) selectNode).getOperandList().stream().filter(Objects::nonNull)
                        .map(node -> getSelectNodesToReplace(node, groupKeys)).flatMap(Collection::stream)
                        .collect(Collectors.toList());
            }
        } else if (selectNode instanceof SqlNodeList) {
            // iterate through select list
            return ((SqlNodeList) selectNode).getList().stream().filter(Objects::nonNull)
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

    private static String getTableAlias(SqlNode node) {
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            return getTableAlias(call.getOperandList());
        }
        if (node instanceof SqlIdentifier) {
            StringBuilder alias = new StringBuilder();
            ImmutableList<String> names = ((SqlIdentifier) node).names;
            if (names.size() >= 2) {
                for (int i = 0; i < names.size() - 1; i++) {
                    alias.append(names.get(i)).append(".");
                }
            }
            return alias.toString();
        }
        // SqlNodeList, SqlLiteral and other return empty string
        return StringUtils.EMPTY;
    }

    private static String getTableAlias(List<SqlNode> operands) {
        if (operands.isEmpty()) {
            return StringUtils.EMPTY;
        }
        return getTableAlias(operands.get(0));
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
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<NDataModel> dataModelDescs = dataflowManager.listOnlineDataModels().stream()
                .filter(m -> !m.getComputedColumnDescs().isEmpty()).collect(Collectors.toList());
        if (dataModelDescs.isEmpty()) {
            return originSql;
        }
        return transformImpl(originSql, project, defaultSchema, dataModelDescs);
    }

    private String transformImpl(String originSql, String project, String defaultSchema,
            List<NDataModel> dataModelDescs) throws SqlParseException {

        if (project == null || originSql == null) {
            return originSql;
        }

        return transformImpl(originSql, new QueryAliasMatcher(project, defaultSchema), dataModelDescs);
    }

    private String transformImpl(String originSql, QueryAliasMatcher queryAliasMatcher, List<NDataModel> dataModelDescs)
            throws SqlParseException {
        if (!KapConfig.getInstanceFromEnv().isImplicitComputedColumnConvertEnabled()) {
            return originSql;
        }

        String sql = originSql;
        if (queryAliasMatcher == null || sql == null) {
            return sql;
        }

        int recursionTimes = 0;
        int maxRecursionTimes = KapConfig.getInstanceFromEnv().getComputedColumnMaxRecursionTimes();

        while ((recursionTimes++) < maxRecursionTimes) {
            Pair<String, Boolean> result = transformImplRecursive(sql, queryAliasMatcher, dataModelDescs, false);
            sql = result.getFirst();
            boolean recursionCompleted = result.getSecond();
            if (recursionCompleted) {
                break;
            }
        }

        return sql;
    }

    private Pair<String, Boolean> transformImplRecursive(String sql, QueryAliasMatcher queryAliasMatcher,
            List<NDataModel> dataModelDescs, boolean replaceCcName) throws SqlParseException {
        boolean recursionCompleted = true;
        List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
        Pair<String, Integer> choiceForCurrentSubquery = null; //<new sql, number of changes by the model>

        for (int i = 0; i < selectOrOrderbys.size(); i++) { //subquery will precede
            if (choiceForCurrentSubquery != null) { //last selectOrOrderby had a matched
                selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
                choiceForCurrentSubquery = null;
            }

            SqlCall selectOrOrderby = selectOrOrderbys.get(i);

            ComputedColumnReplacer rewriteChecker = new ComputedColumnReplacer(queryAliasMatcher, dataModelDescs,
                    recursionCompleted, choiceForCurrentSubquery, selectOrOrderby);
            rewriteChecker.replace(sql, replaceCcName);
            recursionCompleted = rewriteChecker.isRecursionCompleted();
            choiceForCurrentSubquery = rewriteChecker.getChoiceForCurrentSubquery();

            if (choiceForCurrentSubquery != null) {
                sql = choiceForCurrentSubquery.getFirst();
            }
        }

        return Pair.newPair(sql, recursionCompleted);
    }

    private Pair<String, Integer> replaceComputedColumn(String inputSql, SqlCall selectOrOrderby,
            List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo, boolean replaceCcName) {

        if (CollectionUtils.isEmpty(computedColumns)) {
            return Pair.newPair(inputSql, 0);
        }

        String result = inputSql;
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp;
        try {
            toBeReplacedExp = matchComputedColumn(inputSql, selectOrOrderby, computedColumns, queryAliasMatchInfo,
                    replaceCcName);
        } catch (Exception e) {
            log.debug("Convert to computedColumn Fail,parse sql fail ", e);
            return Pair.newPair(inputSql, 0);
        }

        toBeReplacedExp.sort((o1, o2) -> o2.getSecond().getFirst().compareTo(o1.getSecond().getFirst()));

        //replace user's input sql
        for (Pair<ComputedColumnDesc, Pair<Integer, Integer>> toBeReplaced : toBeReplacedExp) {
            Pair<Integer, Integer> startEndPos = toBeReplaced.getSecond();
            int start = startEndPos.getFirst();
            int end = startEndPos.getSecond();
            ComputedColumnDesc cc = toBeReplaced.getFirst();

            String alias = null;
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

            String expr = inputSql.substring(start, end);
            String ccColumnName = replaceCcName ? cc.getInternalCcName() : cc.getColumnName();
            log.debug("Computed column: {} matching {} at [{},{}] using alias in query: {}", cc.getColumnName(), expr,
                    start, end, alias);

            alias = Character.isAlphabetic(alias.charAt(0)) ? alias : DOUBLE_QUOTE + alias + DOUBLE_QUOTE;
            ccColumnName = Character.isAlphabetic(ccColumnName.charAt(0)) ? ccColumnName
                    : DOUBLE_QUOTE + ccColumnName + DOUBLE_QUOTE;
            result = result.substring(0, start) + alias + "." + ccColumnName + result.substring(end);
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

    private List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> matchComputedColumn(String inputSql,
            SqlCall selectOrOrderby, List<ComputedColumnDesc> computedColumns, QueryAliasMatchInfo queryAliasMatchInfo,
            boolean replaceCcName) {
        List<Pair<ComputedColumnDesc, Pair<Integer, Integer>>> toBeReplacedExp = new ArrayList<>();
        for (ComputedColumnDesc cc : computedColumns) {
            List<SqlNode> matchedNodes = Lists.newArrayList();
            matchedNodes.addAll(getMatchedNodes(selectOrOrderby, replaceCcName ? cc.getFullName() : cc.getExpression(),
                    queryAliasMatchInfo));

            String transformedExpression = transformExpr(cc.getExpression());
            if (!transformedExpression.equals(cc.getExpression())) {
                matchedNodes.addAll(getMatchedNodes(selectOrOrderby, transformedExpression, queryAliasMatchInfo));
            }

            for (SqlNode node : matchedNodes) {
                Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(node, inputSql);
                int start = startEndPos.getFirst();
                int end = startEndPos.getSecond();

                boolean conflict = false;
                for (val pair : toBeReplacedExp) {
                    Pair<Integer, Integer> replaced = pair.getSecond();
                    if (!(replaced.getFirst() >= end || replaced.getSecond() <= start)) {
                        // overlap with chosen areas
                        conflict = true;
                        break;
                    }
                }
                if (conflict) {
                    continue;
                }
                toBeReplacedExp.add(Pair.newPair(cc, Pair.newPair(start, end)));
            }
        }
        return toBeReplacedExp;
    }

    //Return matched node's position and its alias(if exists).If can not find matches, return an empty list
    private List<SqlNode> getMatchedNodes(SqlCall selectOrOrderby, String ccExp, QueryAliasMatchInfo matchInfo) {
        if (ccExp == null || ccExp.equals(StringUtils.EMPTY)) {
            return Collections.emptyList();
        }
        ArrayList<SqlNode> matchedNodes = new ArrayList<>();
        SqlNode ccExpressionNode = CalciteParser.getReadonlyExpNode(ccExp);

        List<SqlNode> inputNodes = new LinkedList<>();
        if ("LENIENT".equals(KylinConfig.getInstanceFromEnv().getCalciteConformance())) {
            inputNodes = getInputTreeNodes(selectOrOrderby);
        } else {

            // for select with group by, if the sql is like 'select expr(A) from tbl group by A'
            // do not replace expr(A) with CC
            if (selectOrOrderby instanceof SqlSelect && ((SqlSelect) selectOrOrderby).getGroup() != null) {
                inputNodes.addAll(collectInputNodes((SqlSelect) selectOrOrderby));
            } else if (selectOrOrderby instanceof SqlOrderBy) {
                SqlOrderBy sqlOrderBy = (SqlOrderBy) selectOrOrderby;
                // for sql orderby
                // 1. process order list
                inputNodes.addAll(collectInputNodes(sqlOrderBy));

                // 2. process query part
                // pass to getMatchedNodes directly
                if (sqlOrderBy.query instanceof SqlCall) {
                    matchedNodes.addAll(getMatchedNodes((SqlCall) sqlOrderBy.query, ccExp, matchInfo));
                } else {
                    inputNodes.addAll(getInputTreeNodes(sqlOrderBy.query));
                }
            } else {
                inputNodes = getInputTreeNodes(selectOrOrderby);
            }
        }

        // find whether user input tree node of sql equals defined expression of computed column
        inputNodes.stream().filter(inputNode -> isNodesEqual(matchInfo, ccExpressionNode, inputNode))
                .forEach(matchedNodes::add);
        return matchedNodes;
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
            if (call.getOperator() instanceof SqlAsOperator) {
                call.getOperator().acceptCall(this, call, true, SqlBasicVisitor.ArgHandlerImpl.<SqlNode> instance());
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

    private class ComputedColumnReplacer {
        private QueryAliasMatcher queryAliasMatcher;
        private List<NDataModel> dataModels;
        private boolean recursionCompleted;
        private Pair<String, Integer> choiceForCurrentSubquery;
        private SqlCall selectOrOrderby;

        ComputedColumnReplacer(QueryAliasMatcher queryAliasMatcher, List<NDataModel> dataModels,
                boolean recursionCompleted, Pair<String, Integer> choiceForCurrentSubquery, SqlCall selectOrOrderby) {
            this.queryAliasMatcher = queryAliasMatcher;
            this.dataModels = dataModels;
            this.recursionCompleted = recursionCompleted;
            this.choiceForCurrentSubquery = choiceForCurrentSubquery;
            this.selectOrOrderby = selectOrOrderby;
        }

        boolean isRecursionCompleted() {
            return recursionCompleted;
        }

        Pair<String, Integer> getChoiceForCurrentSubquery() {
            return choiceForCurrentSubquery;
        }

        public void replace(String sql, boolean replaceCcName) {
            SqlSelect sqlSelect = QueryUtil.extractSqlSelect(selectOrOrderby);
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

                Set<String> cols = queryAliasMatcher.getChecker().filterRelatedExcludedColumn(model);
                info.getExcludedColumns().addAll(cols);
                List<ComputedColumnDesc> computedColumns = getSortedComputedColumnWithModel(model);
                if (CollectionUtils.isNotEmpty(computedColumns)) {
                    Pair<String, Integer> ret = replaceComputedColumn(sql, selectOrOrderby, computedColumns, info,
                            replaceCcName);

                    if (replaceCcName && !sql.equals(ret.getFirst())) {
                        choiceForCurrentSubquery = ret;
                    } else if (ret.getSecond() != 0 //
                            && (choiceForCurrentSubquery == null
                                    || ret.getSecond() > choiceForCurrentSubquery.getSecond())) {
                        choiceForCurrentSubquery = ret;
                        recursionCompleted = false;
                    }
                }
            }
        }

        //match longer expressions first
        private List<ComputedColumnDesc> getSortedComputedColumnWithModel(NDataModel model) {
            List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
            KylinConfig projectConfig = NProjectManager.getProjectConfig(model.getProject());
            if (projectConfig.isTableExclusionEnabled() && projectConfig.onlyReuseUserDefinedCC()) {
                ccList = ccList.stream().filter(cc -> !cc.isAutoCC()).collect(Collectors.toList());
            }
            return getCCListSortByLength(ccList);
        }
    }
}
