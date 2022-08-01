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
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.util.KapQueryUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *    agg-join  ->  agg(CD)-agg(other-agg)-join
 *
 *   for example:
 *
 *   KapAggregateRel(group-set=[[1]], groups=[null], TEMP_Calculation_54915774428294=[SUM($2)], TEMP_Calculation_97108873613918=[COUNT(DISTINCT $1)], TEMP_Calculation_97108873613911=[COUNT(DISTINCT $4)], ctx=[])
 *     KapJoinRel(condition=[=($0, $3)], joinType=[inner], ctx=[])
 *       KapProjectRel(ORDER_ID=[$1], CAL_DT=[$2], SELLER_ID=[$7], ctx=[])
 *         KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]])
 *       KapProjectRel(ORDER_ID=[$1], CASE=[CASE(>($2, 0), $0, null)], ctx=[])
 *         KapAggregateRel(group-set=[[0, 1]], groups=[null], X_measure__0=[SUM($2)], ctx=[])
 *           KapProjectRel(LSTG_FORMAT_NAME=[$3], ORDER_ID=[$1], PRICE=[$8], ctx=[])
 *             KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]])
 *
 *   the above plan will be transformed into the following after agg-pushdown(contain KapCountDistinctJoinRule).
 *
 *   KapAggregateRel(group-set=[[0]], groups=[null], TEMP_Calculation_54915774428294=[SUM($2)], TEMP_Calculation_97108873613918=[COUNT(DISTINCT $0)], TEMP_Calculation_97108873613911=[COUNT(DISTINCT $1)], ctx=[])
 *     KapAggregateRel(group-set=[[0, 1]], groups=[null], TEMP_Calculation_54915774428294=[SUM($2)], ctx=[])
 *       KapProjectRel(CAL_DT=[$1], CASE=[$4], $f6=[*($2, $5)], ctx=[])
 *         KapJoinRel(condition=[=($0, $3)], joinType=[inner], ctx=[])
 *           KapAggregateRel(group-set=[[0, 1]], groups=[null], TEMP_Calculation_54915774428294=[SUM($2)], ctx=[])
 *             KapProjectRel(ORDER_ID=[$1], CAL_DT=[$2], SELLER_ID=[$7], ctx=[])
 *               KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]])
 *           KapAggregateRel(group-set=[[0, 1]], groups=[null], agg#0=[COUNT()], ctx=[])
 *             KapProjectRel(ORDER_ID=[$1], CASE=[CASE(>($2, 0), $0, null)], ctx=[])
 *               KapAggregateRel(group-set=[[0, 1]], groups=[null], X_measure__0=[SUM($2)], ctx=[])
 *                 KapProjectRel(LSTG_FORMAT_NAME=[$3], ORDER_ID=[$1], PRICE=[$8], ctx=[])
 *                   KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]])
 */
public class KapCountDistinctJoinRule extends RelOptRule {

    public static final KapCountDistinctJoinRule INSTANCE_COUNT_DISTINCT_JOIN_ONESIDEAGG = new KapCountDistinctJoinRule(
            operand(KapAggregateRel.class, operand(KapJoinRel.class, any())), RelFactories.LOGICAL_BUILDER,
            "KapCountDistinctJoinRule:agg(contain-count-distinct)-join-oneSideAgg");

    public KapCountDistinctJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapJoinRel join = call.rel(1);
        return aggregate.isContainCountDistinct() && KapQueryUtil.isJoinOnlyOneAggChild(join);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapJoinRel join = call.rel(1);

        // build bottom aggRelNode
        final ImmutableList.Builder<AggregateCall> bottomAggCallsBuilder = ImmutableList.builder();
        ImmutableBitSet.Builder bottomGroupSetBuilder = ImmutableBitSet.builder();
        bottomGroupSetBuilder.addAll(aggregate.getGroupSet());

        for (AggregateCall agg : aggregate.getAggCallList()) {
            if (agg.getAggregation().getKind() == SqlKind.COUNT && agg.isDistinct()) {
                bottomGroupSetBuilder.addAll(Lists.newArrayList(agg.getArgList()));
            } else {
                bottomAggCallsBuilder.add(agg.copy(Lists.newArrayList(agg.getArgList()), agg.filterArg));
            }
        }

        ImmutableBitSet bottomGroupSetBuild = bottomGroupSetBuilder.build();
        ImmutableList<AggregateCall> bottomAggCallsBuild = bottomAggCallsBuilder.build();
        List<Integer> bottomGroupSets = bottomGroupSetBuild.asList();
        final Aggregate bottomAggregate = aggregate.copy(aggregate.getTraitSet(), join, aggregate.indicator,
                bottomGroupSetBuild, null, bottomAggCallsBuild);

        // build top aggRelNode
        ImmutableBitSet.Builder topGroupSet = ImmutableBitSet.builder();
        List<Integer> topGroupSetList = new ArrayList<>();
        for (int i = 0; i < aggregate.getGroupSet().asList().size(); i++) {
            topGroupSetList.add(i);
        }
        topGroupSet.addAll(topGroupSetList);

        int topAggArgsIndex = bottomGroupSets.size();
        final ImmutableList.Builder<AggregateCall> topAggCalls = ImmutableList.builder();
        for (AggregateCall agg : aggregate.getAggCallList()) {
            if (agg.getAggregation().getKind() == SqlKind.COUNT && agg.isDistinct()) {
                ArrayList<Integer> aggArgsList = new ArrayList<>();
                for (Integer arg : agg.getArgList()) {
                    aggArgsList.add(bottomGroupSets.indexOf(arg));
                }
                topAggCalls.add(agg.copy(aggArgsList, agg.filterArg));
            } else {
                if (agg.getAggregation().getKind() == SqlKind.COUNT) {
                    topAggCalls.add(AggregateCall.create(SqlStdOperatorTable.SUM0, false, false,
                            Lists.newArrayList(topAggArgsIndex++), -1, agg.type, agg.name));
                } else {
                    topAggCalls.add(agg.copy(Lists.newArrayList(topAggArgsIndex++), agg.filterArg));
                }
            }
        }

        final Aggregate topAggregate = aggregate.copy(aggregate.getTraitSet(), bottomAggregate, aggregate.indicator,
                topGroupSet.build(), null, topAggCalls.build());

        call.transformTo(topAggregate);
    }
}
