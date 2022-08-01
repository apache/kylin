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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Planner rule that pushes Join above and into the Filter node.
 */
public class JoinFilterRule extends RelOptRule {
    private final boolean pullLeft;
    private final boolean pullRight;

    private static Predicate<Join> innerJoinPredicate = join -> {
        Preconditions.checkArgument(join != null, "join MUST NOT be null");
        return join.getJoinType() == JoinRelType.INNER;
    };

    private static Predicate<Join> leftJoinPredicate = join -> {
        Preconditions.checkArgument(join != null, "join MUST NOT be null");
        return join.getJoinType() == JoinRelType.LEFT;
    };

    public static final JoinFilterRule JOIN_LEFT_FILTER = new JoinFilterRule(
            operand(Join.class, null, innerJoinPredicate, operand(Filter.class, any()), operand(RelNode.class, any())),
            RelFactories.LOGICAL_BUILDER, true, false);

    public static final JoinFilterRule JOIN_RIGHT_FILTER = new JoinFilterRule(
            operand(Join.class, null, innerJoinPredicate, operand(RelNode.class, any()), operand(Filter.class, any())),
            RelFactories.LOGICAL_BUILDER, false, true);

    public static final JoinFilterRule JOIN_BOTH_FILTER = new JoinFilterRule(
            operand(Join.class, null, innerJoinPredicate, operand(Filter.class, any()), operand(Filter.class, any())),
            RelFactories.LOGICAL_BUILDER, true, true);

    public static final JoinFilterRule LEFT_JOIN_LEFT_FILTER = new JoinFilterRule(
            operand(Join.class, null, leftJoinPredicate, operand(Filter.class, any()), operand(RelNode.class, any())),
            RelFactories.LOGICAL_BUILDER, true, false);

    public JoinFilterRule(RelOptRuleOperand operand, RelBuilderFactory builder, boolean pullLeft, boolean pullRight) {
        super(operand, builder, "JoinFilterRule:" + pullLeft + ":" + pullRight);
        this.pullLeft = pullLeft;
        this.pullRight = pullRight;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelNode joinLeft = call.rel(1);
        RelNode joinRight = call.rel(2);

        RelNode newJoinLeft = joinLeft;
        RelNode newJoinRight = joinRight;
        int leftCount = joinLeft.getRowType().getFieldCount();
        int rightCount = joinRight.getRowType().getFieldCount();

        RelBuilder builder = call.builder();
        List<RexNode> leftFilters = null;
        List<RexNode> rightFilters = null;

        if (pullLeft) {
            newJoinLeft = joinLeft.getInput(0);
            leftFilters = RelOptUtil.conjunctions(((Filter) joinLeft).getCondition());
        }

        if (pullRight) {
            newJoinRight = joinRight.getInput(0);
            rightFilters = RelOptUtil.conjunctions(((Filter) joinRight).getCondition());
            List<RexNode> shiftedFilters = Lists.newArrayList();
            for (RexNode filter : rightFilters) {
                shiftedFilters.add(shiftFilter(0, rightCount, leftCount, joinRight.getCluster().getRexBuilder(),
                        joinRight.getRowType().getFieldList(), rightCount, join.getRowType().getFieldList(), filter));
            }
            rightFilters = shiftedFilters;
        }

        leftFilters = leftFilters == null ? Lists.<RexNode> newArrayList() : leftFilters;
        rightFilters = rightFilters == null ? Lists.<RexNode> newArrayList() : rightFilters;

        // merge two filters
        leftFilters.addAll(rightFilters);

        RelNode newJoin = join.copy(join.getTraitSet(), Lists.newArrayList(newJoinLeft, newJoinRight));
        RelNode finalFilter = builder.push(newJoin).filter(leftFilters).build();
        call.transformTo(finalFilter);
    }

    private static RexNode shiftFilter(int start, int end, int offset, RexBuilder rexBuilder,
            List<RelDataTypeField> joinFields, int nTotalFields, List<RelDataTypeField> rightFields, RexNode filter) {
        int[] adjustments = new int[nTotalFields];
        for (int i = start; i < end; i++) {
            adjustments[i] = offset;
        }
        return filter.accept(new RelOptUtil.RexInputConverter(rexBuilder, joinFields, rightFields, adjustments));
    }
}
