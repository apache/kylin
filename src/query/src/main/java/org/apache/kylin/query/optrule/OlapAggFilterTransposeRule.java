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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.calcite.KylinSumSplitter;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.kylin.query.relnode.OlapFilterRel;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.util.RuleUtils;

/**
 * agg-filter-join -> agg-filter-agg-join
 */
public class OlapAggFilterTransposeRule extends RelOptRule {
    public static final OlapAggFilterTransposeRule AGG_FILTER_JOIN = new OlapAggFilterTransposeRule(
            operand(OlapAggregateRel.class, operand(OlapFilterRel.class, operand(OlapJoinRel.class, RelOptRule.any()))),
            RelFactories.LOGICAL_BUILDER, "OlapAggFilterTransposeRule:agg-filter-join");

    public OlapAggFilterTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public OlapAggFilterTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public OlapAggFilterTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final OlapJoinRel joinRel = call.rel(2);

        //Only one agg child of join is accepted
        return RuleUtils.isJoinOnlyOneAggChild(joinRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final OlapAggregateRel aggregate = call.rel(0);
        final OlapFilterRel filter = call.rel(1);

        // Do the columns used by the filter appear in the output of the aggregate?
        final ImmutableBitSet filterColumns = RelOptUtil.InputFinder.bits(filter.getCondition());
        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().union(filterColumns);
        final RelNode input = filter.getInput();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final Boolean unique = mq.areColumnsUnique(input, newGroupSet);
        if (unique != null && unique) {
            // The input is already unique on the grouping columns, so there's little
            // advantage of aggregating again. More important, without this check,
            // the rule fires forever: A-F => A-F-A => A-A-F-A => A-A-A-F-A => ...
            return;
        }

        boolean allColumnsInAggregate = aggregate.getGroupSet().contains(filterColumns);

        final Aggregate newAggregate = aggregate.copy(aggregate.getTraitSet(), input, false, newGroupSet, null,
                aggregate.getAggCallList());
        final Mappings.TargetMapping mapping = Mappings.target(newGroupSet::indexOf, input.getRowType().getFieldCount(),
                newGroupSet.cardinality());
        final RexNode newCondition = RexUtil.apply(mapping, filter.getCondition());
        final Filter newFilter = filter.copy(filter.getTraitSet(), newAggregate, newCondition);
        if (allColumnsInAggregate && aggregate.getGroupType() == Aggregate.Group.SIMPLE) {
            // Everything needed by the filter is returned by the aggregate.
            assert newGroupSet.equals(aggregate.getGroupSet());
            call.transformTo(newFilter);
            return;
        }

        // If aggregate uses grouping sets, we always need to split it.
        // Otherwise, it means that grouping sets are not used, but the
        // filter needs at least one extra column, and now aggregate it away.
        final ImmutableBitSet.Builder topGroupSet = ImmutableBitSet.builder();
        for (int c : aggregate.getGroupSet()) {
            topGroupSet.set(newGroupSet.indexOf(c));
        }
        ImmutableList<ImmutableBitSet> newGroupingSets2 = null;
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            ImmutableList.Builder<ImmutableBitSet> newGroupingSetsBuilder = ImmutableList.builder();
            for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
                final ImmutableBitSet.Builder newGroupingSet = ImmutableBitSet.builder();
                for (int c : groupingSet) {
                    newGroupingSet.set(newGroupSet.indexOf(c));
                }
                newGroupingSetsBuilder.add(newGroupingSet.build());
            }
            newGroupingSets2 = newGroupingSetsBuilder.build();
        }
        final List<AggregateCall> topAggCallList = Lists.newArrayList();
        int i = newGroupSet.cardinality();
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final SqlAggFunction rollup = getRollupFunctionForTopAgg(aggregateCall.getAggregation());
            if (rollup == null) {
                // This aggregate cannot be rolled up.
                return;
            }
            if (aggregateCall.isDistinct()) {
                // Cannot roll up distinct.
                return;
            }
            topAggCallList.add(AggregateCall.create(rollup, aggregateCall.isDistinct(), aggregateCall.isApproximate(),
                    ImmutableList.of(i++), -1, aggregateCall.type, aggregateCall.name));
        }
        final Aggregate topAggregate = aggregate.copy(aggregate.getTraitSet(), newFilter, aggregate.indicator,
                topGroupSet.build(), newGroupingSets2, topAggCallList);
        call.transformTo(topAggregate);
    }

    private SqlAggFunction getRollupFunctionForTopAgg(SqlAggFunction func) {
        SqlAggFunction rollup = func.getRollup();
        if (rollup != null && rollup.getKind() == SqlKind.SUM) {
            rollup = KylinSumSplitter.KYLIN_SUM;
        }
        return rollup;
    }
}
