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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kylin.query.relnode.KapFilterRel;
import org.apache.kylin.query.relnode.KapJoinRel;

import com.google.common.collect.ImmutableList;


public class FilterJoinConditionMergeRule extends RelOptRule {

    public static final FilterJoinConditionMergeRule INSTANCE = new FilterJoinConditionMergeRule(
            operand(KapFilterRel.class, operand(KapJoinRel.class, RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "FilterJoinConditionMergeRule");

    public FilterJoinConditionMergeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                        String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);

        List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
        final ImmutableList<RexNode> origAboveFilters = ImmutableList.copyOf(aboveFilters);
        List<RexNode> simpifliedFilters = simpifly(aboveFilters, join);

        if (simpifliedFilters.size() == origAboveFilters.size()) {
            return;
        }

        RelBuilder relBuilder = call.builder();
        relBuilder.push(join);
        relBuilder.filter(simpifliedFilters);
        call.transformTo(relBuilder.build());
    }

    private List<RexNode> simpifly(List<RexNode> filterConditions, Join join) {
        final List<RexNode> joinFilters =
                RelOptUtil.conjunctions(join.getCondition());
        if (filterConditions.isEmpty()) {
            return filterConditions;
        }

        final JoinRelType joinType = join.getJoinType();
        final List<RexNode> aboveFilters = filterConditions;
        final Map<RexNode, RexNode> shiftedMapping = new HashMap<>();

        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();

        boolean filterPushed = false;
        if (RelOptUtil.classifyFilters(
                join,
                aboveFilters,
                joinType,
                true,
                !joinType.generatesNullsOnLeft(),
                !joinType.generatesNullsOnRight(),
                joinFilters,
                leftFilters,
                rightFilters,
                shiftedMapping)) {
            filterPushed = true;
        }

        List<RexNode> leftSimplified = leftFilters;
        List<RexNode> rightSimplified = rightFilters;
        if (filterPushed) {
            RelNode left = join.getLeft() instanceof HepRelVertex
                    ? ((HepRelVertex) join.getLeft()).getCurrentRel() : join.getLeft();
            RelNode right = join.getRight() instanceof HepRelVertex
                    ? ((HepRelVertex) join.getRight()).getCurrentRel() : join.getRight();
            if (left instanceof Join) {
                leftSimplified = simpifly(leftFilters, (Join) left);
            }
            if (right instanceof Join) {
                rightSimplified = simpifly(rightFilters, (Join) right);
            }
        }

        leftSimplified.forEach(filter -> aboveFilters.add(shiftedMapping.get(filter)));
        rightSimplified.forEach(filter -> aboveFilters.add(shiftedMapping.get(filter)));
        return aboveFilters;
    }
}
