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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.measure.corr.CorrMeasureType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CorrReduceFunctionRule extends RelOptRule {
    public static final CorrReduceFunctionRule INSTANCE = new CorrReduceFunctionRule(operand(Aggregate.class, any()),
            RelFactories.LOGICAL_BUILDER, "CorrReduceFunctionRule");

    public CorrReduceFunctionRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String discription) {
        super(operand, relBuilderFactory, discription);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return containsCorrCall(((Aggregate) call.rels[0]).getAggCallList());

    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
        reduceAggs(ruleCall, oldAggRel);
    }

    private void reduceAggs(RelOptRuleCall call, Aggregate oldAggRel) {
        final List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
        final int groupCount = oldAggRel.getGroupCount();
        final int indicatorCount = oldAggRel.getIndicatorCount();

        // pass through group key (+ indicators if present)
        final List<RexNode> projList = Lists.newArrayList();
        RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        for (int i = 0; i < groupCount + indicatorCount; ++i) {
            projList.add(rexBuilder.makeInputRef(getFieldType(oldAggRel, i), i));
        }

        // List of input expressions. If a particular aggregate needs more, it
        // will add an expression to the end, and we will create an extra
        // project.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(oldAggRel.getInput());
        final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

        // create new aggregate function calls and rest of project list together
        final List<AggregateCall> newCalls = Lists.newArrayList();
        final Map<AggregateCall, RexNode> aggCallMapping = Maps.newHashMap();
        for (AggregateCall oldCall : oldCalls) {
            projList.add(reduceAgg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
        }

        final int extraArgCount = inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
        if (extraArgCount > 0) {
            relBuilder.project(inputExprs, CompositeList.of(relBuilder.peek().getRowType().getFieldNames(),
                    Collections.<String> nCopies(extraArgCount, null)));
        }
        relBuilder.aggregate(
                relBuilder.groupKey(oldAggRel.getGroupSet(), oldAggRel.indicator, oldAggRel.getGroupSets()), newCalls);

        relBuilder.project(projList, oldAggRel.getRowType().getFieldNames());
        call.transformTo(relBuilder.build());

    }

    private boolean containsCorrCall(List<AggregateCall> aggCallList) {
        for (AggregateCall call : aggCallList) {
            if (CorrMeasureType.FUNC_CORR.equals(call.getAggregation().getName())) {
                return true;
            }
        }
        return false;
    }

    private RexNode reduceAgg(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs) {
        if (CorrMeasureType.FUNC_CORR.equals(oldCall.getAggregation().getName())) {
            return reduceCORR(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
        } else {
            // anything else:  preserve original call
            RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
            List<RelDataType> oldArgTypes = SqlTypeUtil.projectTypes(oldAggRel.getInput().getRowType(),
                    oldCall.getArgList());
            return rexBuilder.addAggCall(oldCall, oldAggRel.getGroupCount(), oldAggRel.indicator, newCalls,
                    aggCallMapping, oldArgTypes);
        }
    }

    /**
     * NOTE:  these references are with respect to the output of newAggRel
     *
     * CORR(x，y) ==>
     *  （count(x) * sum(x * y) - sum(x) * sum(y)）
     *      / power(
     *        (sum(x * x) * count(x) - sum(x) * sum(x)) * (sum(y * y) * count(x) - sum(y) * sum(y)),
     *        0.5)
     */
    private RexNode reduceCORR(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs) {
        final int oldNGroups = oldAggRel.getGroupCount();
        RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

        final Function<RexNode, RexNode> castToDouble = rexNode -> rexBuilder.makeCast(DOUBLE_TYPE, rexNode);

        if (oldCall.getArgList() == null || oldCall.getArgList().size() < 2) {
            throw new IllegalArgumentException("CORR must have 2 argument parameters");
        }
        int iInputX = oldCall.getArgList().get(0);
        int iInputY = oldCall.getArgList().get(1);
        RelDataType argInputXType = getFieldType(oldAggRel.getInput(), iInputX);
        final RexNode argX = inputExprs.get(iInputX);
        final RexNode argY = inputExprs.get(iInputY);

        // build RexNode of sum(x) and sum(y)
        final AggregateCall sumXCall = AggregateCall.create(SqlStdOperatorTable.SUM, oldCall.isDistinct(),
                ImmutableIntList.of(iInputX), oldCall.filterArg, oldAggRel.getGroupCount(), oldAggRel.getInput(), null,
                null);
        final RexNode sumX = castToDouble.apply(rexBuilder.addAggCall(sumXCall, oldNGroups, oldAggRel.indicator,
                newCalls, aggCallMapping, ImmutableList.of(inputExprs.get(iInputX).getType())));
        final AggregateCall sumYCall = AggregateCall.create(SqlStdOperatorTable.SUM, oldCall.isDistinct(),
                ImmutableIntList.of(iInputY), oldCall.filterArg, oldAggRel.getGroupCount(), oldAggRel.getInput(), null,
                null);
        final RexNode sumY = castToDouble.apply(rexBuilder.addAggCall(sumYCall, oldNGroups, oldAggRel.indicator,
                newCalls, aggCallMapping, ImmutableList.of(inputExprs.get(iInputY).getType())));

        // build RexNode of sum(x) * sum(x), sum(x) * sum(y) and sum(y) * sum(y)
        final RexNode sumNodeXSquared = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumX);
        final RexNode sumNodeXY = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumX, sumY);
        final RexNode sumNodeYSquared = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumY, sumY);

        // build RexNode of sum(x * x), sum(y * y) and sum(x * y)
        final RexNode sumArgXSquared = castToDouble
                .apply(buildMultiplyRexNode(oldAggRel, oldCall, inputExprs, newCalls, aggCallMapping, argX, argX));
        final RexNode sumArgYSquared = castToDouble
                .apply(buildMultiplyRexNode(oldAggRel, oldCall, inputExprs, newCalls, aggCallMapping, argY, argY));
        final RexNode sumArgXY = castToDouble
                .apply(buildMultiplyRexNode(oldAggRel, oldCall, inputExprs, newCalls, aggCallMapping, argX, argY));

        // build count()
        final AggregateCall countAggCall = AggregateCall.create(SqlStdOperatorTable.COUNT, oldCall.isDistinct(),
                Lists.<Integer> newArrayList(), oldCall.filterArg, oldAggRel.getGroupCount(), oldAggRel.getInput(),
                null, null);
        final RexNode countArg = rexBuilder.addAggCall(countAggCall, oldNGroups, oldAggRel.indicator, newCalls,
                aggCallMapping, ImmutableList.of(argInputXType));

        final RexNode covNode = rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
                rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, countArg, sumArgXY), sumNodeXY);
        final RexNode varianceXNode = rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
                rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumArgXSquared, countArg), sumNodeXSquared);
        final RexNode varianceYNode = rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
                rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, sumArgYSquared, countArg), sumNodeYSquared);

        final RexNode half = rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
        final RexNode divisor = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
                rexBuilder.makeCall(SqlStdOperatorTable.POWER, varianceXNode, half),
                rexBuilder.makeCall(SqlStdOperatorTable.POWER, varianceYNode, half));

        // to be consistant with spark
        // count = 0 -> null
        // correlation = 0 -> null
        final RexNode corr = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, castToDouble.apply(covNode),
                castToDouble.apply(divisor));
        final List<RexNode> caseWhenArgs = new LinkedList<>();
        caseWhenArgs.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countArg,
                rexBuilder.makeZeroLiteral(divisor.getType())));
        caseWhenArgs.add(rexBuilder.makeNullLiteral(DOUBLE_TYPE));
        caseWhenArgs.add(corr);
        final RexNode corrWithCountChecking = rexBuilder.makeCall(DOUBLE_TYPE, SqlStdOperatorTable.CASE, caseWhenArgs);

        return rexBuilder.makeCast(oldCall.getType(), corrWithCountChecking);
    }

    private RelDataType getFieldType(RelNode relNode, int i) {
        final RelDataTypeField inputField = relNode.getRowType().getFieldList().get(i);
        return inputField.getType();
    }

    private static <T> int lookupOrAdd(List<T> list, T element) {
        int ordinal = list.indexOf(element);
        if (ordinal == -1) {
            ordinal = list.size();
            list.add(element);
        }
        return ordinal;
    }

    private RexNode buildMultiplyRexNode(Aggregate oldAggRel, AggregateCall oldCall, List<RexNode> inputExprs,
            List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, RexNode rexNodeX,
            RexNode rexNodeY) {
        final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        final RexNode rexNodeXY = rexBuilder.makeCall(resultType(), SqlStdOperatorTable.MULTIPLY,
                Lists.<RexNode> newArrayList(rexNodeX, rexNodeY));
        final int argXYSquaredOrdinal = lookupOrAdd(inputExprs, rexNodeXY);
        final int nGroups = oldAggRel.getGroupCount();

        final Aggregate.AggCallBinding bindingXY = new Aggregate.AggCallBinding(oldAggRel.getCluster().getTypeFactory(),
                SqlStdOperatorTable.SUM, ImmutableList.of(inputExprs.get(argXYSquaredOrdinal).getType()),
                oldAggRel.getGroupCount(), oldCall.filterArg >= 0);
        final AggregateCall sumArgXYSquaredAggCall = AggregateCall.create(SqlStdOperatorTable.SUM, oldCall.isDistinct(),
                ImmutableIntList.of(argXYSquaredOrdinal), oldCall.filterArg,
                SqlStdOperatorTable.SUM.inferReturnType(bindingXY), null);
        return rexBuilder.addAggCall(sumArgXYSquaredAggCall, nGroups, oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(inputExprs.get(argXYSquaredOrdinal).getType()));
    }

    private static final RelDataType DOUBLE_TYPE = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
            .createSqlType(SqlTypeName.DOUBLE);

    private RelDataType resultType() {
        return DOUBLE_TYPE;
    }

}
