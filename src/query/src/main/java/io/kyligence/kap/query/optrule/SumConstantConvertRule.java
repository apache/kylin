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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.exception.SumExprUnSupportException;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapProjectRel;
import org.apache.kylin.query.util.AggExpressionUtil;
import org.apache.kylin.query.util.AggExpressionUtil.AggExpression;
import org.apache.kylin.query.util.AggExpressionUtil.GroupExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * sql: select sum(3) from KYLIN_SALES;
 *
 * EXECUTION PLAN:
 *  OLAPAggregateRel(group=[{}], EXPR$0=[SUM($0)], ctx=[0@null])
 *    OLAPProjectRel($f0=[3], ctx=[0@null])
 *
 * Apply this rule to convert execution plan.
 * After convert:
 *
 * OLAPProjectRel($f0=[*($0, 3)], ctx=[0@null])
 *   OLAPAggregateRel(group=[{}], EXPR$0=[COUNT()], ctx=[0@null])
 *     OLAPProjectRel(ctx=[0@null])
 *
 */

public class SumConstantConvertRule extends RelOptRule {

    private static final Logger logger = LoggerFactory.getLogger(SumConstantConvertRule.class);

    public static final SumConstantConvertRule INSTANCE = new SumConstantConvertRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !AggExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumConstantConvertRule");

    public SumConstantConvertRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        Aggregate oldAgg = ruleCall.rel(0);
        Project oldProject = ruleCall.rel(1);
        try {
            boolean matches = false;
            for (AggExpression sumExpr : AggExpressionUtil.collectSumExpressions(oldAgg, oldProject)) {
                if (sumExpr.isSumConst()) {
                    matches = true;
                }
            }
            return matches;
        } catch (SumExprUnSupportException e) {
            logger.trace("Current rel unable to apply SumBasicOperatorRule", e);
            return false;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        try {
            RelBuilder relBuilder = ruleCall.builder();
            Aggregate oldAgg = ruleCall.rel(0);
            Project oldProject = ruleCall.rel(1);

            ContextUtil.dumpCalcitePlan("old plan", oldAgg, logger);

            List<AggExpression> aggExpressions = AggExpressionUtil.collectSumExpressions(oldAgg, oldProject);
            Pair<List<GroupExpression>, ImmutableList<ImmutableBitSet>> groups = AggExpressionUtil
                    .collectGroupExprAndGroup(oldAgg, oldProject);
            List<GroupExpression> groupExpressions = groups.getFirst();
            ImmutableList<ImmutableBitSet> newGroupSets = groups.getSecond();

            // #1 Build bottom project
            relBuilder.push(oldProject.getInput());
            List<RexNode> bottomProjectList = buildBottomProject(relBuilder, oldProject, groupExpressions,
                    aggExpressions);
            relBuilder.project(bottomProjectList);

            // #2 Build bottom aggregate
            ImmutableBitSet.Builder groupSetBuilder = ImmutableBitSet.builder();
            for (AggExpressionUtil.GroupExpression group : groupExpressions) {
                for (int i = 0; i < group.getBottomAggInput().length; i++) {
                    groupSetBuilder.set(group.getBottomAggInput()[i]);
                }
            }
            ImmutableBitSet bottomAggGroupSet = groupSetBuilder.build();
            RelBuilder.GroupKey groupKey = relBuilder.groupKey(bottomAggGroupSet, null);
            List<AggregateCall> aggCalls = buildBottomAggCall(relBuilder, aggExpressions,
                    bottomAggGroupSet.cardinality());
            relBuilder.aggregate(groupKey, aggCalls);

            // #3 ReBuild sum expr project
            for (GroupExpression groupExpression : groupExpressions) {
                for (int i = 0; i < groupExpression.getTopProjInput().length; i++) {
                    int groupIdx = groupExpression.getBottomAggInput()[i];
                    groupExpression.getTopProjInput()[i] = bottomAggGroupSet.indexOf(groupIdx);
                }
            }
            List<RexNode> topProjectList = buildTopProject(relBuilder, oldProject, groupExpressions, aggExpressions);
            relBuilder.project(topProjectList);

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
            ruleCall.transformTo(relNode);
        } catch (Exception e) {
            logger.error("sql cannot apply sum constant rule ", e);
        }
    }

    private List<RexNode> buildBottomProject(RelBuilder relBuilder, Project oldProject,
            List<GroupExpression> groupExpressions, List<AggExpression> aggExpressions) {
        List<RexNode> bottomProjectList = Lists.newArrayList();

        for (GroupExpression groupExpr : groupExpressions) {
            int[] sourceInput = groupExpr.getBottomProjInput();
            for (int i = 0; i < sourceInput.length; i++) {
                groupExpr.getBottomAggInput()[i] = bottomProjectList.size();
                RexInputRef groupInput = relBuilder.getRexBuilder().makeInputRef(oldProject.getInput(), sourceInput[i]);
                bottomProjectList.add(groupInput);
            }
        }

        for (AggExpression aggExpression : aggExpressions) {
            if (!aggExpression.isSumConst() && aggExpression.getExpression() != null) {
                aggExpression.getBottomAggInput()[0] = bottomProjectList.size();
                bottomProjectList.add(aggExpression.getExpression());
            }
        }
        return bottomProjectList;
    }

    private List<AggregateCall> buildBottomAggCall(RelBuilder relBuilder, List<AggExpression> aggExpressions,
            int bottomAggOffset) {
        List<AggregateCall> aggCalls = Lists.newArrayList();
        int sumConstIdx = 0;
        for (AggExpression sumExpr : aggExpressions) {
            String aggName = "SUM_CONST$" + (sumConstIdx++);
            AggregateCall aggCall;
            if (sumExpr.isSumConst()) {
                aggCall = AggregateCall.create(SqlStdOperatorTable.COUNT, false, false, Lists.newArrayList(), -1,
                        bottomAggOffset, relBuilder.peek(), null, aggName);
            } else {
                AggregateCall oldAggCall = sumExpr.getAggCall();
                List<Integer> args = Arrays.stream(sumExpr.getBottomAggInput()).boxed().collect(Collectors.toList());
                int filterArg = oldAggCall.filterArg;
                aggCall = oldAggCall.copy(args, filterArg);
            }
            sumExpr.getTopProjInput()[0] = aggCalls.size() + bottomAggOffset;
            aggCalls.add(aggCall);
        }
        return aggCalls;
    }

    private List<RexNode> buildTopProject(RelBuilder relBuilder, Project oldProject,
            List<GroupExpression> groupExpressions, List<AggExpression> aggExpressions) {
        List<RexNode> topProjectList = Lists.newArrayList();

        for (GroupExpression groupExpr : groupExpressions) {
            int[] aggAdjustments = AggExpressionUtil.generateAdjustments(groupExpr.getBottomProjInput(),
                    groupExpr.getTopProjInput());
            RexNode projectExpr = groupExpr.getExpression()
                    .accept(new RelOptUtil.RexInputConverter(relBuilder.getRexBuilder(),
                            oldProject.getInput().getRowType().getFieldList(),
                            relBuilder.peek().getRowType().getFieldList(), aggAdjustments));
            projectExpr = relBuilder.getRexBuilder().ensureType(groupExpr.getExpression().getType(), projectExpr,
                    false);
            topProjectList.add(projectExpr);
        }

        for (AggExpression sumExpr : aggExpressions) {
            RexNode rexNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), sumExpr.getTopProjInput()[0]);
            if (sumExpr.isSumConst()) {
                RexNode expr = sumExpr.getExpression();
                List<RexNode> newArgs = Lists.newArrayList();
                newArgs.add(expr);
                newArgs.add(rexNode);
                RexNode result = relBuilder.call(SqlStdOperatorTable.MULTIPLY, newArgs);
                rexNode = relBuilder.getRexBuilder().ensureType(sumExpr.getAggCall().getType(), result, false);
            }
            topProjectList.add(rexNode);

        }
        return topProjectList;
    }

    private List<AggregateCall> buildTopAggregate(List<AggregateCall> oldAggregates, int groupOffset,
            List<AggExpression> aggExpressions) {
        List<AggregateCall> topAggregates = Lists.newArrayList();
        for (int aggIndex = 0; aggIndex < oldAggregates.size(); aggIndex++) {
            AggExpression aggExpression = aggExpressions.get(aggIndex);
            AggregateCall aggCall = aggExpression.getAggCall();
            String aggName = "TOP_AGG$" + aggIndex;
            SqlAggFunction aggFunction = SqlKind.COUNT == aggCall.getAggregation().getKind() ? SqlStdOperatorTable.SUM0
                    : aggCall.getAggregation();
            topAggregates.add(AggregateCall.create(aggFunction, false, false,
                    Lists.newArrayList(groupOffset + aggIndex), -1, aggCall.getType(), aggName));
        }
        return topAggregates;
    }
}
