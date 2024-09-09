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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.AggregateMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.immutables.value.Value;

import lombok.extern.slf4j.Slf4j;

/**
 * A customized rule extends {@link AggregateMergeRule} and overrides necessary methods.
 * The codes mostly copied from the parent by changing the result of the aggregate call
 * that merged by top and bottom aggregates.
 * See {@link ExtendedAggregateMergeRule#sumSplitterSubstituteMerge(AggregateCall, AggregateCall)}
 * for more details.
 */
@Value.Enclosing
@Slf4j
public class ExtendedAggregateMergeRule extends AggregateMergeRule {

    public static final ExtendedAggregateMergeRule INSTANCE = new ExtendedAggregateMergeRule(Config.DEFAULT);

    protected ExtendedAggregateMergeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate topAgg = call.rel(0);
        final Aggregate bottomAgg = call.rel(1);
        if (topAgg.getGroupCount() > bottomAgg.getGroupCount()) {
            return;
        }

        final ImmutableBitSet bottomGroupSet = bottomAgg.getGroupSet();
        final Map<Integer, Integer> map = new HashMap<>();
        bottomGroupSet.forEach(v -> map.put(map.size(), v));
        for (int k : topAgg.getGroupSet()) {
            if (!map.containsKey(k)) {
                return;
            }
        }

        // top aggregate keys must be subset of lower aggregate keys
        final ImmutableBitSet topGroupSet = topAgg.getGroupSet().permute(map);
        if (!bottomGroupSet.contains(topGroupSet)) {
            return;
        }

        boolean hasEmptyGroup = topAgg.getGroupSets().stream().anyMatch(ImmutableBitSet::isEmpty);

        final List<AggregateCall> finalCalls = new ArrayList<>();
        for (AggregateCall topCall : topAgg.getAggCallList()) {
            if (!isAggregateSupported(topCall) || topCall.getArgList().size() == 0) {
                return;
            }
            // Make sure top aggregate argument refers to one of the aggregate
            int bottomIndex = topCall.getArgList().get(0) - bottomGroupSet.cardinality();
            if (bottomIndex >= bottomAgg.getAggCallList().size() || bottomIndex < 0) {
                return;
            }
            AggregateCall bottomCall = bottomAgg.getAggCallList().get(bottomIndex);
            // Should not merge if top agg with empty group keys and the lower agg
            // function is COUNT, because in case of empty input for lower agg,
            // the result is empty, if we merge them, we end up with 1 result with
            // 0, which is wrong.
            if (!isAggregateSupported(bottomCall) || (bottomCall.getAggregation() == SqlStdOperatorTable.COUNT
                    && topCall.getAggregation().getKind() != SqlKind.SUM0 && hasEmptyGroup)) {
                return;
            }
            AggregateCall finalCall = mergeAggregateCalls(topCall, bottomCall);
            // fail to merge the aggregate call, bail out
            if (finalCall == null) {
                return;
            }
            finalCalls.add(finalCall);
        }

        // re-map grouping sets
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if (topAgg.getGroupType() != Aggregate.Group.SIMPLE) {
            newGroupingSets = ImmutableBitSet.ORDERING
                    .immutableSortedCopy(ImmutableBitSet.permute(topAgg.getGroupSets(), map));
        }

        RelNode bottomAggInput = replaceBottomAggInputIfNecessary(bottomAgg, call);
        final Aggregate finalAgg = topAgg.copy(topAgg.getTraitSet(), bottomAggInput, topGroupSet, newGroupingSets,
                finalCalls);
        call.transformTo(finalAgg);
    }

    /**
     * The substitute merge method for top and bottom aggregates when their types are
     * both {@link SqlTypeName#DECIMAL}, which using top aggregate type instead of bottom
     * for the newly merged aggregate call.
     *
     * @param top the top aggregate to be merged
     * @param bottom the bottom aggregate to be merged
     * @return merged aggregate call
     */
    protected static AggregateCall sumSplitterSubstituteMerge(AggregateCall top, AggregateCall bottom) {
        SqlKind topKind = top.getAggregation().getKind();
        if (topKind == bottom.getAggregation().getKind() && (topKind == SqlKind.SUM || topKind == SqlKind.SUM0)) {

            RelDataType topType = top.getType();
            RelDataType bottomType = bottom.getType();
            RelDataType newAggCallType = (topType.getSqlTypeName() == SqlTypeName.DECIMAL
                    && bottomType.getSqlTypeName() == SqlTypeName.DECIMAL) ? topType : bottomType;

            return AggregateCall.create(bottom.getAggregation(), bottom.isDistinct(), bottom.isApproximate(), false,
                    bottom.getArgList(), bottom.filterArg, bottom.distinctKeys, bottom.getCollation(), newAggCallType,
                    top.getName());
        } else {
            return null;
        }
    }

    protected static RelNode replaceBottomAggInputIfNecessary(Aggregate bottomAgg, RelOptRuleCall call) {
        RelNode bottomAggInput = bottomAgg.getInput();
        RelNode ret = bottomAggInput;
        if (KylinRelDataTypeSystem.getProjectConfig().isImprovedSumDecimalPrecisionEnabled()) {
            RelBuilder relBuilder = call.builder();
            RexBuilder rexBuilder = call.builder().getRexBuilder();

            List<Integer> sumDecimalAggCallArgs = bottomAgg.getAggCallList().stream()
                    .filter(aggCall -> aggCall.getAggregation().getKind() == SqlKind.SUM
                            && SqlTypeUtil.isDecimal(aggCall.getType()))
                    .flatMap(aggCall -> aggCall.getArgList().stream()).collect(Collectors.toList());

            if (bottomAggInput instanceof RelSubset) {
                RelSubset bottomAggInputSubSet = (RelSubset) bottomAggInput;
                RelNode bestOrOriginal = bottomAggInputSubSet.getBestOrOriginal();
                if (bestOrOriginal instanceof Project) {
                    Project project = (Project) bestOrOriginal;
                    relBuilder.push(project.getInput());
                    List<RexNode> newProjects = new ArrayList<>();
                    RelDataTypeFactory typeFactory = call.builder().getTypeFactory();
                    RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();
                    for (int i = 0; i < project.getProjects().size(); i++) {
                        RexNode rex = project.getProjects().get(i);
                        if (sumDecimalAggCallArgs.contains(i)) {
                            RelDataType newType = typeSystem.deriveSumType(typeFactory, rex.getType());
                            rex = rexBuilder.makeCast(newType, rex);
                        }
                        newProjects.add(rex);
                    }
                    ret = relBuilder.project(newProjects).build();
                }
            }
        }
        return ret;
    }

    /**
     * Copied from {@link org.apache.calcite.rel.rules.AggregateMergeRule}
     *
     * @param aggCall the top or bottom aggregate call
     * @return whether the aggCall is splittable or mergeable
     */
    private static boolean isAggregateSupported(AggregateCall aggCall) {
        if (aggCall.isDistinct() || aggCall.hasFilter() || aggCall.isApproximate() || aggCall.getArgList().size() > 1) {
            return false;
        }
        return aggCall.getAggregation().maybeUnwrap(SqlSplittableAggFunction.class).isPresent();
    }

    private static AggregateCall mergeAggregateCalls(AggregateCall topCall, AggregateCall bottomCall) {
        SqlSplittableAggFunction splitter = bottomCall.getAggregation().unwrapOrThrow(SqlSplittableAggFunction.class);
        return (splitter instanceof SqlSplittableAggFunction.SumSplitter
                && KylinRelDataTypeSystem.getProjectConfig().isImprovedSumDecimalPrecisionEnabled())
                        ? sumSplitterSubstituteMerge(topCall, bottomCall)
                        : splitter.merge(topCall, bottomCall);
    }
}
