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

package io.kyligence.kap.query.optrule;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.util.AggExpressionUtil;
import org.apache.kylin.query.util.AggExpressionUtil.AggExpression;
import org.apache.kylin.query.util.AggExpressionUtil.GroupExpression;
import org.apache.kylin.query.util.RuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * expand agg(sum case when .. then col else col end) to agg(sum case when agg(col) else agg(col) end)
 * so that the sql can utilize agg index on the column
 */
public abstract class AbstractAggCaseWhenFunctionRule extends RelOptRule {

    private static final Logger logger = LoggerFactory.getLogger(AbstractAggCaseWhenFunctionRule.class);

    protected AbstractAggCaseWhenFunctionRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        Aggregate oldAgg = ruleCall.rel(0);
        Project oldProject = ruleCall.rel(1);
        return checkAggCaseExpression(oldAgg, oldProject);
    }

    public void onMatch(RelOptRuleCall ruleCall) {
        try {
            RelBuilder relBuilder = ruleCall.builder();
            Aggregate originalAgg = ruleCall.rel(0);
            Project originalProject = ruleCall.rel(1);

            List<AggregateCall> applicableAggCalls = originalAgg.getAggCallList().stream()
                    .filter(aggCall -> isApplicableWithSumCaseRule(aggCall, originalProject))
                    .collect(Collectors.toList());
            List<AggregateCall> nonApplicableAggCalls = new LinkedList<>(originalAgg.getAggCallList());
            nonApplicableAggCalls.removeAll(applicableAggCalls);

            // extract the sum case when agg part from original agg rel
            // and do the sum case when expr transformation
            Aggregate sumCaseAgg = extractPartialAggregateCalls(relBuilder, originalAgg, originalProject,
                    applicableAggCalls);
            RelNode transformedSumCaseAgg = transformSumExprAggregate(relBuilder, sumCaseAgg,
                    (Project) sumCaseAgg.getInput(0));

            // in case there is no other no sum case when agg calls, do transform and return
            if (nonApplicableAggCalls.isEmpty()) {
                ruleCall.transformTo(transformedSumCaseAgg);
                return;
            }

            // otherwise we are going to extract out the non sum case when agg part
            Aggregate nonSumCaseAgg = extractPartialAggregateCalls(relBuilder, originalAgg, originalProject,
                    nonApplicableAggCalls);
            // and join with the sum case when agg part by the group keys
            RelNode joined = joinAggCaseWhenAndNonAggCaseWhenRel(relBuilder, transformedSumCaseAgg, nonSumCaseAgg,
                    originalAgg);

            ContextUtil.dumpCalcitePlan("new plan", joined, logger);
            ruleCall.transformTo(joined);
        } catch (Exception | Error e) {
            logger.error("sql cannot apply sum case when rule ", e);
        }
    }

    /**
     * join two aggregate rels by their group by keys
     * an additional project will be added on top of the joined rel
     * to make sure the output fields are identical to the original aggregate rel fields
     * @param relBuilder
     * @param sumCaseAgg
     * @param nonSumCaseAgg
     * @param originalAgg
     * @return
     */
    private RelNode joinAggCaseWhenAndNonAggCaseWhenRel(RelBuilder relBuilder, RelNode sumCaseAgg,
            Aggregate nonSumCaseAgg, Aggregate originalAgg) {
        relBuilder.push(nonSumCaseAgg);
        relBuilder.push(sumCaseAgg);
        List<RelDataTypeField> leftFields = nonSumCaseAgg.getRowType().getFieldList();
        List<RelDataTypeField> rightFields = sumCaseAgg.getRowType().getFieldList();
        int nonAggExprListSize = leftFields.size() - nonSumCaseAgg.getAggCallList().size();
        List<RexNode> joinConds = new LinkedList<>();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        // join by group keys
        for (int i = 0; i < nonAggExprListSize; i++) {
            joinConds.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(leftFields.get(i).getType(), i),
                    rexBuilder.makeInputRef(leftFields.get(i).getType(), i + leftFields.size())));
        }
        relBuilder.join(JoinRelType.INNER, joinConds);

        List<RexNode> projectNodes = new LinkedList<>();
        // fill group keys
        for (int i = 0; i < nonAggExprListSize; i++) {
            projectNodes.add(rexBuilder.makeInputRef(leftFields.get(i).getType(), i));
        }
        // fill in agg calls
        for (int i = 0, j = 0, k = 0; i < originalAgg.getAggCallList().size(); i++) {
            if (j < nonSumCaseAgg.getAggCallList().size()
                    && originalAgg.getAggCallList().get(i) == nonSumCaseAgg.getAggCallList().get(j)) {
                projectNodes.add(rexBuilder.makeInputRef(leftFields.get(nonAggExprListSize + j).getType(),
                        nonAggExprListSize + j));
                j++;
            } else {
                projectNodes.add(rexBuilder.makeInputRef(rightFields.get(nonAggExprListSize + k).getType(),
                        leftFields.size() + nonAggExprListSize + k));
                k++;
            }
        }
        relBuilder.project(projectNodes);
        return relBuilder.build();
    }

    /**
     * clone and strip off hep vertex
     * @param rel
     * @return
     */
    private RelNode cloneRelNode(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return cloneRelNode(((HepRelVertex) rel).getCurrentRel());
        }
        return rel.copy(rel.getTraitSet(),
                rel.getInputs().stream().map(this::cloneRelNode).collect(Collectors.toList()));
    }

    /**
     * extract part of the aggregate calls from original aggregate rel
     * and build a new aggregate rel with those agg calls
     * @param relBuilder
     * @param oriAgg
     * @param oriProject
     * @param aggCalls
     * @return
     */
    private Aggregate extractPartialAggregateCalls(RelBuilder relBuilder, Aggregate oriAgg, Project oriProject,
            List<AggregateCall> aggCalls) {
        Set<Integer> nonSumInputIdxes = aggCalls.stream().flatMap(e -> e.getArgList().stream())
                .collect(Collectors.toSet());
        nonSumInputIdxes.addAll(oriAgg.getGroupSet().asList());
        // preserve the original project exprs for simplicity
        // clone input rel node since we may create multiple copy of aggregates
        relBuilder.push(cloneRelNode(oriProject.getInput()));
        List<RexNode> newChildExps = new ArrayList<>(oriProject.getChildExps().size());
        for (int i = 0; i < oriProject.getChildExps().size(); i++) {
            if (nonSumInputIdxes.contains(i)) {
                newChildExps.add(oriProject.getChildExps().get(i));
            } else {
                // simply fill zero values for values that we are not going to use
                newChildExps
                        .add(relBuilder.getRexBuilder().makeZeroLiteral(oriProject.getChildExps().get(i).getType()));
            }
        }
        relBuilder.project(newChildExps);
        relBuilder.aggregate(relBuilder.groupKey(oriAgg.getGroupSet(), oriAgg.getGroupSets()), aggCalls);
        return (Aggregate) relBuilder.build();
    }

    private RelNode transformSumExprAggregate(RelBuilder relBuilder, Aggregate oldAgg, Project oldProject) {
        // #0 Set base input
        relBuilder.push(oldProject.getInput());

        // Locate basic sum expression info
        List<AggExpression> aggExpressions = AggExpressionUtil.collectSumExpressions(oldAgg, oldProject);
        List<AggExpression> sumCaseExprs = aggExpressions.stream().filter(this::isApplicableAggExpression)
                .collect(Collectors.toList());
        Pair<List<GroupExpression>, ImmutableList<ImmutableBitSet>> groups = AggExpressionUtil
                .collectGroupExprAndGroup(oldAgg, oldProject);
        List<GroupExpression> groupExpressions = groups.getFirst();
        ImmutableList<ImmutableBitSet> newGroupSets = groups.getSecond();

        // #1 Build bottom project
        List<RexNode> bottomProjectList = buildBottomProject(relBuilder, oldProject, groupExpressions, aggExpressions);
        relBuilder.project(bottomProjectList);

        // #2 Build bottom aggregate
        ImmutableBitSet.Builder groupSetBuilder = ImmutableBitSet.builder();
        for (GroupExpression group : groupExpressions) {
            for (int i = 0; i < group.getBottomAggInput().length; i++) {
                groupSetBuilder.set(group.getBottomAggInput()[i]);
            }
        }
        for (AggExpression aggExpression : sumCaseExprs) {
            for (int i = 0; i < aggExpression.getBottomAggConditionsInput().length; i++) {
                int conditionIdx = aggExpression.getBottomAggConditionsInput()[i];
                groupSetBuilder.set(conditionIdx);
            }
        }
        ImmutableBitSet bottomAggGroupSet = groupSetBuilder.build();
        RelBuilder.GroupKey groupKey = relBuilder.groupKey(bottomAggGroupSet, null);
        List<AggregateCall> aggCalls = buildBottomAggregate(relBuilder, aggExpressions,
                bottomAggGroupSet.cardinality());
        relBuilder.aggregate(groupKey, aggCalls);

        // #3 ReBuild top project
        for (GroupExpression groupExpression : groupExpressions) {
            for (int i = 0; i < groupExpression.getTopProjInput().length; i++) {
                int groupIdx = groupExpression.getBottomAggInput()[i];
                groupExpression.getTopProjInput()[i] = bottomAggGroupSet.indexOf(groupIdx);
            }
        }
        for (AggExpression aggExpression : sumCaseExprs) {
            for (int i = 0; i < aggExpression.getTopProjConditionsInput().length; i++) {
                int conditionIdx = aggExpression.getBottomAggConditionsInput()[i];
                aggExpression.getTopProjConditionsInput()[i] = bottomAggGroupSet.indexOf(conditionIdx);
            }
        }
        List<RexNode> caseProjList = buildTopProject(relBuilder, oldProject, aggExpressions, groupExpressions);

        relBuilder.project(caseProjList);

        // #4 ReBuild top aggregate
        ImmutableBitSet.Builder topGroupSetBuilder = ImmutableBitSet.builder();
        for (int i = 0; i < groupExpressions.size(); i++) {
            topGroupSetBuilder.set(i);
        }
        ImmutableBitSet topGroupSet = topGroupSetBuilder.build();
        List<AggregateCall> topAggregates = buildTopAggregate(oldAgg.getAggCallList(), topGroupSet.cardinality(),
                aggExpressions);
        RelBuilder.GroupKey topGroupKey = relBuilder.groupKey(topGroupSet, newGroupSets);
        relBuilder.aggregate(topGroupKey, topAggregates);
        RelNode relNode = relBuilder.build();
        ContextUtil.dumpCalcitePlan("new plan", relNode, logger);
        return relNode;
    }

    private List<RexNode> buildBottomProject(RelBuilder relBuilder, Project oldProject,
            List<GroupExpression> groupExpressions, List<AggExpression> aggExpressions) {
        List<RexNode> bottomProjectList = Lists.newArrayList();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();

        for (GroupExpression groupExpr : groupExpressions) {
            int[] sourceInput = groupExpr.getBottomProjInput();
            for (int i = 0; i < sourceInput.length; i++) {
                groupExpr.getBottomAggInput()[i] = bottomProjectList.size();
                RexInputRef groupInput = rexBuilder.makeInputRef(oldProject.getInput(), sourceInput[i]);
                bottomProjectList.add(groupInput);
            }
        }

        for (AggExpression aggExpression : aggExpressions) {
            if (isApplicableAggExpression(aggExpression)) {
                // sum expression expanded project
                buildBottomAggExpression(rexBuilder, oldProject, bottomProjectList, aggExpression);
            } else if (aggExpression.getExpression() != null) {
                aggExpression.getBottomAggInput()[0] = bottomProjectList.size();
                bottomProjectList.add(aggExpression.getExpression());
            }
        }
        return bottomProjectList;
    }

    private void buildBottomAggExpression(RexBuilder rexBuilder, Project oldProject, List<RexNode> bottomProjectList,
            AggExpression aggExpression) {
        int[] conditionsInput = aggExpression.getBottomProjConditionsInput();
        for (int i = 0; i < conditionsInput.length; i++) {
            aggExpression.getBottomAggConditionsInput()[i] = bottomProjectList.size();
            RexInputRef conditionInput = rexBuilder.makeInputRef(oldProject.getInput(), conditionsInput[i]);
            bottomProjectList.add(conditionInput);
        }
        List<RexNode> values = aggExpression.getValuesList();
        for (int i = 0; i < values.size(); i++) {
            aggExpression.getBottomAggValuesInput()[i] = bottomProjectList.size();
            if (RuleUtils.isCast(values.get(i))) {
                RexNode rexNode = ((RexCall) (values.get(i))).operands.get(0);
                DataType dataType = DataType.getType(rexNode.getType().getSqlTypeName().getName());
                if (!AggExpressionUtil.isSum(aggExpression.getAggCall().getAggregation().kind)
                        || dataType.isNumberFamily() || dataType.isIntegerFamily()) {
                    bottomProjectList.add(rexNode);
                } else {
                    bottomProjectList.add(values.get(i));
                }
            } else if (RuleUtils.isNotNullLiteral(values.get(i))) {
                bottomProjectList.add(values.get(i));
            } else {
                bottomProjectList.add(rexBuilder.makeBigintLiteral(BigDecimal.ZERO));
            }
        }
    }

    private List<AggregateCall> buildBottomAggregate(RelBuilder relBuilder, List<AggExpression> aggExpressions,
            int bottomAggOffset) {
        List<AggregateCall> bottomAggCalls = Lists.newArrayList();

        List<AggExpression> aggCaseExpressions = Lists.newArrayList();
        for (AggExpression aggExpression : aggExpressions) {
            if (isApplicableAggExpression(aggExpression)) {
                aggCaseExpressions.add(aggExpression);
                continue; // Sum case add to bottomAggCalls later
            }
            aggExpression.getTopProjInput()[0] = bottomAggOffset + bottomAggCalls.size();
            AggregateCall oldAggCall = aggExpression.getAggCall();
            List<Integer> args = Arrays.stream(aggExpression.getBottomAggInput()).boxed().collect(Collectors.toList());
            int filterArg = oldAggCall.filterArg;
            bottomAggCalls.add(oldAggCall.copy(args, filterArg));
        }

        int aggCaseIdx = 0;
        for (AggExpression aggExpression : aggCaseExpressions) {
            for (int valueIdx = 0; valueIdx < aggExpression.getValuesList().size(); valueIdx++) {
                if (!isValidAggColumnExpr(aggExpression.getValuesList().get(valueIdx))) {
                    continue;
                }
                String aggName = getBottomAggPrefix() + aggCaseIdx + "$" + valueIdx;
                List<Integer> args = Lists.newArrayList(aggExpression.getBottomAggValuesInput()[valueIdx]);
                aggExpression.getTopProjValuesInput()[valueIdx] = bottomAggOffset + bottomAggCalls.size();
                bottomAggCalls.add(AggregateCall.create(getBottomAggFunc(aggExpression.getAggCall()), false, false,
                        args, -1, bottomAggOffset, relBuilder.peek(), null, aggName));
            }
            aggCaseIdx++;
        }

        return bottomAggCalls;
    }

    private List<RexNode> buildTopProject(RelBuilder relBuilder, Project oldProject, List<AggExpression> aggExpressions,
            List<GroupExpression> groupExpressions) {
        List<RexNode> topProjectList = Lists.newArrayList();

        for (GroupExpression groupExpr : groupExpressions) {
            int[] aggAdjustments = AggExpressionUtil.generateAdjustments(groupExpr.getBottomProjInput(),
                    groupExpr.getTopProjInput());
            RexNode rexNode = groupExpr.getExpression()
                    .accept(new RelOptUtil.RexInputConverter(relBuilder.getRexBuilder(),
                            oldProject.getInput().getRowType().getFieldList(),
                            relBuilder.peek().getRowType().getFieldList(), aggAdjustments));
            rexNode = relBuilder.getRexBuilder().ensureType(groupExpr.getExpression().getType(), rexNode, false);
            topProjectList.add(rexNode);
        }

        for (AggExpression aggExpression : aggExpressions) {
            if (isApplicableAggExpression(aggExpression)) {
                int[] adjustments = AggExpressionUtil.generateAdjustments(aggExpression.getBottomProjConditionsInput(),
                        aggExpression.getTopProjConditionsInput());
                List<RexNode> conditions = aggExpression.getConditions();
                List<RexNode> valuesList = aggExpression.getValuesList();
                List<RexNode> newArgs = Lists.newArrayList();
                int whenIndex;
                for (whenIndex = 0; whenIndex < conditions.size(); whenIndex++) {
                    RexNode whenNode = conditions.get(whenIndex)
                            .accept(new RelOptUtil.RexInputConverter(relBuilder.getRexBuilder(),
                                    oldProject.getInput().getRowType().getFieldList(),
                                    relBuilder.peek().getRowType().getFieldList(), adjustments));
                    newArgs.add(whenNode);
                    RexNode thenNode = valuesList.get(whenIndex);
                    if (isNeedTackCast(thenNode)) {
                        thenNode = relBuilder.getRexBuilder().makeCast(((RexCall) thenNode).type,
                                relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(),
                                        aggExpression.getTopProjValuesInput()[whenIndex]));
                    } else if (RuleUtils.isNotNullLiteral(thenNode)) {
                        // keep null or sum(null)?
                        thenNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(),
                                aggExpression.getTopProjValuesInput()[whenIndex]);
                    }
                    newArgs.add(thenNode);
                }
                RexNode elseNode = valuesList.get(whenIndex);
                if (isNeedTackCast(elseNode)) {
                    elseNode = relBuilder.getRexBuilder().makeCast(((RexCall) elseNode).type, relBuilder.getRexBuilder()
                            .makeInputRef(relBuilder.peek(), aggExpression.getTopProjValuesInput()[whenIndex]));
                } else if (RuleUtils.isNotNullLiteral(elseNode)) {
                    // keep null or sum(null)?
                    elseNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(),
                            aggExpression.getTopProjValuesInput()[whenIndex]);
                }
                newArgs.add(elseNode);
                RexNode newCaseWhenExpr = relBuilder.call(SqlStdOperatorTable.CASE, newArgs);
                topProjectList.add(newCaseWhenExpr);
            } else {
                RexNode rexNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(),
                        aggExpression.getTopProjInput()[0]);
                topProjectList.add(rexNode);

            }
        }

        return topProjectList;
    }

    private List<AggregateCall> buildTopAggregate(List<AggregateCall> oldAggregates, int groupOffset,
            List<AggExpression> aggExpressions) {
        List<AggregateCall> topAggregates = Lists.newArrayList();
        for (int aggIndex = 0; aggIndex < oldAggregates.size(); aggIndex++) {
            AggExpression aggExpression = aggExpressions.get(aggIndex);
            AggregateCall aggCall = aggExpression.getAggCall();
            String aggName = "AGG$" + aggIndex;
            topAggregates.add(AggregateCall.create(getTopAggFunc(aggCall), false, false,
                    Lists.newArrayList(groupOffset + aggIndex), -1, aggCall.getType(), aggName));
        }
        return topAggregates;
    }

    /**
     * return true if there is any sum/count_distinct case when agg call
     * @param oldAgg
     * @param oldProject
     * @return
     */
    protected abstract boolean checkAggCaseExpression(Aggregate oldAgg, Project oldProject);

    protected abstract boolean isApplicableWithSumCaseRule(AggregateCall aggregateCall, Project project);

    protected abstract boolean isApplicableAggExpression(AggExpression aggExpr);

    protected abstract SqlAggFunction getBottomAggFunc(AggregateCall aggCall);

    protected abstract SqlAggFunction getTopAggFunc(AggregateCall aggCall);

    protected boolean isValidAggColumnExpr(RexNode rexNode) {
        return true;
    }

    protected boolean isNeedTackCast(RexNode rexNode) {
        return RuleUtils.isCast(rexNode);
    }

    private static final String BOTTOM_AGG_PREFIX = "SUB_AGG$";

    protected String getBottomAggPrefix() {
        return BOTTOM_AGG_PREFIX;
    }
}
