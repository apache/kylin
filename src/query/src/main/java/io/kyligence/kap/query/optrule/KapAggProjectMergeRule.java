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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapFilterRel;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.relnode.KapProjectRel;
import org.apache.kylin.query.util.RuleUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class KapAggProjectMergeRule extends RelOptRule {
    public static final KapAggProjectMergeRule AGG_PROJECT_JOIN = new KapAggProjectMergeRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, operand(KapJoinRel.class, any()))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectMergeRule:agg-project-join");

    public static final KapAggProjectMergeRule AGG_PROJECT_FILTER_JOIN = new KapAggProjectMergeRule(
            operand(KapAggregateRel.class,
                    operand(KapProjectRel.class, operand(KapFilterRel.class, operand(KapJoinRel.class, any())))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectMergeRule:agg-project-filter-join");

    public KapAggProjectMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public KapAggProjectMergeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public KapAggProjectMergeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final KapJoinRel joinRel = call.rel(2) instanceof KapFilterRel //
                ? call.rel(3)
                : call.rel(2);
        //Only one agg child of join is accepted
        if (!RuleUtils.isJoinOnlyOneAggChild(joinRel)) {
            return false;
        }

        final KapAggregateRel aggregate = call.rel(0);
        final KapProjectRel project = call.rel(1);
        ImmutableBitSet.Builder immutableBitSetBuilder = ImmutableBitSet.builder();
        immutableBitSetBuilder.addAll(aggregate.getGroupSet());
        aggregate.getAggCallList().forEach(aggregateCall -> {
            ImmutableBitSet args = ImmutableBitSet.of(aggregateCall.getArgList());
            immutableBitSetBuilder.addAll(args);
        });
        ImmutableBitSet hasUseInAgg = immutableBitSetBuilder.build();
        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode rexNode = project.getProjects().get(i);
            // group by column is RexInputRef or group by column is expression 
            // and the expression has not imported by topNode
            if (!(rexNode instanceof RexInputRef) && hasUseInAgg.get(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapProjectRel project = call.rel(1);
        RelNode x = apply(call, aggregate, project);
        if (x != null) {
            call.transformTo(x);
        }
    }

    public static RelNode apply(RelOptRuleCall call, Aggregate aggregate, Project project) {
        final List<Integer> newKeys = Lists.newArrayList();
        final Map<Integer, Integer> map = new HashMap<>();
        for (int key : aggregate.getGroupSet()) {
            final RexNode rex = project.getProjects().get(key);
            if (rex instanceof RexInputRef) {
                final int newKey = ((RexInputRef) rex).getIndex();
                newKeys.add(newKey);
                map.put(key, newKey);
            }
        }

        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            newGroupingSets = ImmutableBitSet.ORDERING
                    .immutableSortedCopy(ImmutableBitSet.permute(aggregate.getGroupSets(), map));
        }

        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
            for (int arg : aggregateCall.getArgList()) {
                final RexNode rex = project.getProjects().get(arg);
                if (!(rex instanceof RexInputRef)) {
                    // Cannot handle "AGG(expression)"
                    return null;
                }
                newArgs.add(((RexInputRef) rex).getIndex());
            }
            int newFilterArg = -1;
            if (aggregateCall.filterArg >= 0
                    && project.getProjects().get(aggregateCall.filterArg) instanceof RexInputRef) {
                newFilterArg = ((RexInputRef) project.getProjects().get(aggregateCall.filterArg)).getIndex();
            }
            aggCalls.add(aggregateCall.copy(newArgs.build(), newFilterArg));
        }

        final Aggregate newAggregate = aggregate.copy(aggregate.getTraitSet(), project.getInput(), aggregate.indicator,
                newGroupSet, newGroupingSets, aggCalls.build());

        // Add a project if the group set is not in the same order or
        // contains duplicates.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(newAggregate);
        processNewKeyNotExists(relBuilder, newKeys, newGroupSet, aggregate, newAggregate);

        return relBuilder.build();
    }

    private static void processNewKeyNotExists(RelBuilder relBuilder, List<Integer> newKeys,
            ImmutableBitSet newGroupSet, Aggregate aggregate, Aggregate newAggregate) {
        if (!newKeys.equals(newGroupSet.asList())) {
            final List<Integer> posList = Lists.newArrayList();
            for (int newKey : newKeys) {
                posList.add(newGroupSet.indexOf(newKey));
            }
            if (aggregate.indicator) {
                for (int newKey : newKeys) {
                    posList.add(aggregate.getGroupCount() + newGroupSet.indexOf(newKey));
                }
            }
            for (int i = newAggregate.getGroupCount() + newAggregate.getIndicatorCount(); i < newAggregate.getRowType()
                    .getFieldCount(); i++) {
                posList.add(i);
            }
            relBuilder.project(relBuilder.fields(posList));
        }
    }
}
