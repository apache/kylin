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

package org.apache.kylin.query.optrule;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kylin.query.relnode.OlapFilterRel;
import org.apache.kylin.query.relnode.OlapJoinRel;

/**
 * If a CNF predicate expression appears in both the filter conditions and join conditions,
 * it is removed from the filter conditions.
 */
public class FilterJoinConditionMergeRule extends RelOptRule {

    public static final FilterJoinConditionMergeRule INSTANCE = new FilterJoinConditionMergeRule(
            operand(OlapFilterRel.class, operand(OlapJoinRel.class, RelOptRule.any())), RelFactories.LOGICAL_BUILDER,
            "FilterJoinConditionMergeRule");

    public FilterJoinConditionMergeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);
        RexBuilder rexBuilder = call.builder().getRexBuilder();

        List<RexNode> originFilters = RelOptUtil.conjunctions(filter.getCondition());
        int originFilterSize = originFilters.size(); // record origin size

        List<RexNode> inputFilters = originFilters.stream().map(rexBuilder::copy).collect(Collectors.toList());
        simplify(rexBuilder, originFilters, join, inputFilters);

        if (originFilters.size() == originFilterSize) {
            return;
        }

        RelBuilder relBuilder = call.builder();
        relBuilder.push(join);
        relBuilder.filter(originFilters);
        call.transformTo(relBuilder.build());
    }

    private void simplify(RexBuilder rexBuilder, List<RexNode> reservedJoinFilters, Join join,
            List<RexNode> filterConditions) {
        final List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        if (filterConditions.isEmpty()) {
            return;
        }

        final JoinRelType joinType = join.getJoinType();
        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();

        // The filterConditions may change when filters were pushed to the current join
        List<RexNode> backup = joinFilters.stream().map(rexBuilder::copy).collect(Collectors.toList());
        boolean filterPushed = RelOptUtil.classifyFilters(join, filterConditions, true,
                !joinType.generatesNullsOnLeft(), !joinType.generatesNullsOnRight(), joinFilters, leftFilters,
                rightFilters);

        if (filterPushed) {
            reservedJoinFilters.removeAll(backup);
            RelNode left = join.getLeft() instanceof HepRelVertex ? ((HepRelVertex) join.getLeft()).getCurrentRel()
                    : join.getLeft();
            RelNode right = join.getRight() instanceof HepRelVertex ? ((HepRelVertex) join.getRight()).getCurrentRel()
                    : join.getRight();
            if (left instanceof Join) {
                simplify(rexBuilder, reservedJoinFilters, (Join) left, leftFilters);
            }
            if (right instanceof Join) {
                simplify(rexBuilder, reservedJoinFilters, (Join) right, rightFilters);
            }
        }
    }
}
