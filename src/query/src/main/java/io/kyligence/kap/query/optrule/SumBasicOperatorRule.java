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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
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
import com.google.common.collect.Lists;

/**
 * sql: select sum(price*3) from KYLIN_SALES;
 *
 * EXECUTION PLAN:
 * OLAPAggregateRel(group=[{}], EXPR$0=[SUM($0)], ctx=[0@null])
 *   OLAPProjectRel($f0=[*($6, 3)], ctx=[0@null])
 *
 * However in this execution plan, only computed column can answer this sql.
 * So apply this rule to convert execution plan.
 * After convert:
 *
 * OLAPProjectRel($f0=[*($0, 3)], ctx=[0@null])
 *   OLAPAggregateRel(group=[{}], EXPR$0=[SUM($0)], ctx=[0@null])
 *     OLAPProjectRel(PRICE=$6, ctx=[0@null])
 *
 * Limitation: issue #11656
 * if the column has null, then sum(1) isn't equal count(*)
 * like sql: select sum(price+1) from KYLIN_SALES;
 * and it doesn't support sum(column * column)
 * like sql: select sum(price*item_count) from KYLIN_SALES;
 */

public class SumBasicOperatorRule extends RelOptRule {

    private static final Logger logger = LoggerFactory.getLogger(SumBasicOperatorRule.class);

    public static final SumBasicOperatorRule INSTANCE = new SumBasicOperatorRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !AggExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumBasicOperatorRule");

    public SumBasicOperatorRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        Aggregate oldAgg = ruleCall.rel(0);
        Project oldProject = ruleCall.rel(1);
        try {
            boolean matches = false;
            for (AggExpression sumExpr : AggExpressionUtil.collectSumExpressions(oldAgg, oldProject)) {
                if (checkExpressionSupported(sumExpr)) {
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
            Aggregate oldAgg = ruleCall.rel(0);
            Project oldProject = ruleCall.rel(1);
            RelBuilder relBuilder = ruleCall.builder();
            relBuilder.push(oldProject.getInput());

            ContextUtil.dumpCalcitePlan("old plan", oldAgg, logger);

            List<AggExpression> aggExpressions = AggExpressionUtil.collectSumExpressions(oldAgg, oldProject);
            Pair<List<GroupExpression>, ImmutableList<ImmutableBitSet>> groups = AggExpressionUtil
                    .collectGroupExprAndGroup(oldAgg, oldProject);
            List<GroupExpression> groupExpressions = groups.getFirst();
            ImmutableList<ImmutableBitSet> newGroupSets = groups.getSecond();

            // #1 Build bottom project
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

            List<AggregateCall> bottomAggregates = buildBottomAggregate(relBuilder, aggExpressions,
                    bottomAggGroupSet.cardinality());

            relBuilder.aggregate(groupKey, bottomAggregates);

            // #3 ReBuild sum expr project
            for (GroupExpression groupExpression : groupExpressions) {
                for (int i = 0; i < groupExpression.getTopProjInput().length; i++) {
                    int groupIdx = groupExpression.getBottomAggInput()[i];
                    groupExpression.getTopProjInput()[i] = bottomAggGroupSet.indexOf(groupIdx);
                }
            }
            List<RexNode> topProjectList = buildTopProjectList(relBuilder, oldProject, aggExpressions,
                    groupExpressions);

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
            logger.error("sql cannot apply sum multiply rule ", e);
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

        for (AggExpression sumExpr : aggExpressions) {
            if (checkExpressionSupported(sumExpr)) {
                List<RexNode> sumColumn = Arrays.stream(sumExpr.getBottomProjInput())
                        .mapToObj(input -> relBuilder.getRexBuilder().makeInputRef(oldProject.getInput(), input))
                        .collect(Collectors.toList());
                if (sumExpr.getBottomAggInput().length != 0)
                    sumExpr.getBottomAggInput()[0] = bottomProjectList.size();
                bottomProjectList.addAll(sumColumn);
            } else if (sumExpr.getExpression() != null) {
                sumExpr.getBottomAggInput()[0] = bottomProjectList.size();
                bottomProjectList.add(sumExpr.getExpression());
            }
        }
        return bottomProjectList;
    }

    private List<AggregateCall> buildBottomAggregate(RelBuilder relBuilder, List<AggExpression> aggExpressions,
            int bottomAggOffset) {
        int sumOpIndex = 0;
        List<AggregateCall> bottomAggregates = Lists.newArrayList();
        for (AggExpression aggExpression : aggExpressions) {
            AggregateCall aggCall;
            if (checkExpressionSupported(aggExpression)) {
                AggExpressionUtil.assertCondition(aggExpression.getBottomProjInput().length == 1,
                        "SumBasicOperatorRule only handles aggregation of single source column");
                String aggName = "SUM_OP$" + (sumOpIndex++);
                List<Integer> aggList = Lists.newArrayList(aggExpression.getBottomAggInput()[0]);
                aggCall = AggregateCall.create(SqlStdOperatorTable.SUM, false, false, aggList, -1, bottomAggOffset,
                        relBuilder.peek(), null, aggName);
            } else {
                AggregateCall oldAggCall = aggExpression.getAggCall();
                List<Integer> args = Arrays.stream(aggExpression.getBottomAggInput()).boxed()
                        .collect(Collectors.toList());
                int filterArg = oldAggCall.filterArg;
                aggCall = oldAggCall.copy(args, filterArg);
            }
            aggExpression.getTopProjInput()[0] = bottomAggOffset + bottomAggregates.size();
            bottomAggregates.add(aggCall);
        }
        return bottomAggregates;
    }

    private List<RexNode> buildTopProjectList(RelBuilder relBuilder, Project oldProject,
            List<AggExpression> aggExpressions, List<GroupExpression> groupExpressions) {
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
            if (checkExpressionSupported(sumExpr)) {
                RexNode expr = sumExpr.getExpression();
                int[] adjustments = AggExpressionUtil.generateAdjustments(sumExpr.getBottomProjInput(),
                        sumExpr.getTopProjInput());
                rexNode = expr.accept(new RelOptUtil.RexInputConverter(relBuilder.getRexBuilder(),
                        oldProject.getInput().getRowType().getFieldList(),
                        relBuilder.peek().getRowType().getFieldList(), adjustments));
                rexNode = relBuilder.getRexBuilder().ensureType(sumExpr.getAggCall().getType(), rexNode, false);
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
            String aggName = "AGG$" + aggIndex;
            SqlAggFunction aggFunction = SqlKind.COUNT == aggCall.getAggregation().getKind() //
                    ? SqlStdOperatorTable.SUM
                    : aggCall.getAggregation();
            topAggregates.add(AggregateCall.create(aggFunction, false, false,
                    Lists.newArrayList(groupOffset + aggIndex), -1, aggCall.getType(), aggName));
        }
        return topAggregates;
    }

    private boolean checkExpressionSupported(AggExpression aggExpression) {
        if (aggExpression.isSumCase()) {
            throw new SumExprUnSupportException("SumBasicOperatorRule is unable to handle sum case expression.");
        }

        AggregateCall aggCall = aggExpression.getAggCall();
        RexNode expr = aggExpression.getExpression();
        SqlKind aggType = aggCall.getAggregation().getKind();
        if (!AggExpressionUtil.isSum(aggType)) {
            return false;
        }

        if (!(expr instanceof RexCall)) {
            return false;
        }

        if (!isBasicOperand(expr)) {
            return false;
        }

        checkUnSupportOperands(expr);

        return true;
    }

    /**
     *  + - * /
     */
    private boolean isBasicOperand(RexNode expr) {
        if (expr instanceof RexLiteral || expr instanceof RexInputRef)
            return true;

        if (SqlKind.PLUS == expr.getKind() || SqlKind.MINUS == expr.getKind() || SqlKind.TIMES == expr.getKind()
                || KapRuleUtils.isDivide(expr)) {
            if (!(expr instanceof RexCall)) {
                return false;
            }

            RexCall exprCall = (RexCall) expr;
            RexNode left = exprCall.getOperands().get(0);
            RexNode right = exprCall.getOperands().get(1);
            return isBasicOperand(left) && isBasicOperand(right);
        }
        return false;
    }

    private void checkUnSupportOperands(RexNode expr) {
        if (!(expr instanceof RexCall)) {
            return;
        }

        RexCall exprCall = (RexCall) expr;
        verify(exprCall);

        for (RexNode exprNode : exprCall.getOperands()) {
            if (exprNode instanceof RexCall) {
                checkUnSupportOperands(exprNode);
            }
        }
    }

    private void verify(RexCall exprCall) {
        switch (exprCall.getKind()) {
        case PLUS:
        case MINUS:
            verifyPlusOrMinus(exprCall);
            break;
        case TIMES:
            verifyMultiply(exprCall);
            break;
        case DIVIDE:
            verifyDivide(exprCall);
            break;
        default:
        }

        if (KapRuleUtils.isDivide(exprCall)) {
            verifyDivide(exprCall);
        }
    }

    private void verifyPlusOrMinus(RexCall exprCall) {
        // plus or minus does not support SUM EXPRESSION caused by null values
        // please see https://github.com/Kyligence/KAP/issues/14627
        throw new SumExprUnSupportException("That PLUS/MINUS of the columns is not supported for sum expression");
    }

    private void verifyMultiply(RexCall exprCall) {
        RexNode left = exprCall.getOperands().get(0);
        RexNode right = exprCall.getOperands().get(1);
        if (!isConstant(left) && !isConstant(right)) {
            // left/right side are both column, not support sum (col * col)
            throw new SumExprUnSupportException(
                    "That both of the two sides of the columns is not supported for " + exprCall.getKind().toString());
        }

    }

    private void verifyDivide(RexCall exprCall) {
        RexNode right = exprCall.getOperands().get(1);
        if (!isConstant(right)) {
            // right side is a column, not support sum ( col / col)
            throw new SumExprUnSupportException(
                    "That the right side of the columns is not supported for " + exprCall.getKind().toString());
        }
    }

    private boolean isConstant(RexNode expr) {
        return extractColumn(expr).isEmpty();
    }

    private List<RexNode> extractColumn(RexNode expr) {
        List<RexNode> values = Lists.newArrayList();
        if (expr instanceof RexInputRef) {
            values.add(expr);
        }

        if (expr instanceof RexCall) {
            RexCall exprCall = (RexCall) expr;
            for (RexNode exprNode : exprCall.getOperands()) {
                values.addAll(extractColumn(exprNode));
            }
        }

        return values;
    }
}
