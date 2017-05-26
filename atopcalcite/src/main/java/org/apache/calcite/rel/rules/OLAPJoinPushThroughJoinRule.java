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
package org.apache.calcite.rel.rules;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Predicate;

/**
 * modified form org.apache.calcite.rel.rules.JoinPushThroughJoinRule.
 * The goal is to move joins with sub-queries after joins with tables,
 * so that pre-defined join with tables can be matched
 */
public class OLAPJoinPushThroughJoinRule extends RelOptRule {
    /**
     * Instance of the rule that works on logical joins only, and pushes to the
     * right.
     */
    public static final RelOptRule INSTANCE = new OLAPJoinPushThroughJoinRule("OLAPJoinPushThroughJoinRule", LogicalJoin.class, RelFactories.LOGICAL_BUILDER);

    public OLAPJoinPushThroughJoinRule(String description, Class<? extends Join> clazz, RelBuilderFactory relBuilderFactory) {
        super(operand(clazz,

                operand(clazz, operand(RelNode.class, any()), operand(RelNode.class, null, new Predicate<RelNode>() {
                    @Override
                    public boolean apply(@Nullable RelNode input) {
                        return !(input instanceof TableScan);
                    }
                }, any())),

                operand(TableScan.class, any())), relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        onMatchRight(call);
    }

    private void onMatchRight(RelOptRuleCall call) {
        final Join topJoin = call.rel(0);
        final Join bottomJoin = call.rel(1);
        final RelNode relC = call.rel(4);
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();
        final RelOptCluster cluster = topJoin.getCluster();
        //        Preconditions.checkState(relA == call.rel(2));
        //        Preconditions.checkState(relB == call.rel(3));

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet bBitSet = ImmutableBitSet.range(aCount, aCount + bCount);

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  B
        //    /    \
        //   A      C

        // If either join is not inner, we cannot proceed.
        // (Is this too strict?)
        //        if (topJoin.getJoinType() != JoinRelType.INNER || bottomJoin.getJoinType() != JoinRelType.INNER) {
        //            return;
        //        }

        // Split the condition of topJoin into a conjunction. Each of the
        // parts that does not use columns from B can be pushed down.
        final List<RexNode> intersecting = new ArrayList<>();
        final List<RexNode> nonIntersecting = new ArrayList<>();
        split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting);

        // If there's nothing to push down, it's not worth proceeding.
        if (nonIntersecting.isEmpty()) {
            return;
        }

        // Split the condition of bottomJoin into a conjunction. Each of the
        // parts that use columns from B will need to be pulled up.
        final List<RexNode> bottomIntersecting = new ArrayList<>();
        final List<RexNode> bottomNonIntersecting = new ArrayList<>();
        split(bottomJoin.getCondition(), bBitSet, bottomIntersecting, bottomNonIntersecting);

        // target: | A       | C      |
        // source: | A       | B | C      |
        //        final Mappings.TargetMapping bottomMapping = Mappings
        //            .createShiftMapping(aCount + bCount + cCount, 0, 0, aCount, aCount, aCount + bCount,
        //                cCount);

        final Mappings.TargetMapping bottomMapping = Mappings.createShiftMapping(aCount + bCount + cCount, 0, 0, aCount, aCount + cCount, aCount, bCount, aCount, aCount + bCount, cCount);
        final List<RexNode> newBottomList = new ArrayList<>();
        new RexPermuteInputsShuttle(bottomMapping, relA, relC).visitList(nonIntersecting, newBottomList);
        new RexPermuteInputsShuttle(bottomMapping, relA, relC).visitList(bottomNonIntersecting, newBottomList);
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition = RexUtil.composeConjunction(rexBuilder, newBottomList, false);
        final Join newBottomJoin = bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relA, relC, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone());

        // target: | A       | C      | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping = Mappings.createShiftMapping(aCount + bCount + cCount, 0, 0, aCount, aCount + cCount, aCount, bCount, aCount, aCount + bCount, cCount);
        final List<RexNode> newTopList = new ArrayList<>();
        new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB).visitList(intersecting, newTopList);
        new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB).visitList(bottomIntersecting, newTopList);
        RexNode newTopCondition = RexUtil.composeConjunction(rexBuilder, newTopList, false);
        @SuppressWarnings("SuspiciousNameCombination")
        final Join newTopJoin = topJoin.copy(topJoin.getTraitSet(), newTopCondition, newBottomJoin, relB, topJoin.getJoinType(), topJoin.isSemiJoinDone());

        assert !Mappings.isIdentity(topMapping);
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(newTopJoin);
        relBuilder.project(relBuilder.fields(topMapping));
        call.transformTo(relBuilder.build());
    }

    /**
     * Splits a condition into conjunctions that do or do not intersect with
     * a given bit set.
     */
    static void split(RexNode condition, ImmutableBitSet bitSet, List<RexNode> intersecting, List<RexNode> nonIntersecting) {
        for (RexNode node : RelOptUtil.conjunctions(condition)) {
            ImmutableBitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
            if (bitSet.intersects(inputBitSet)) {
                intersecting.add(node);
            } else {
                nonIntersecting.add(node);
            }
        }
    }
}
