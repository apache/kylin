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

package org.apache.kylin.query.util;

import java.util.Collection;

import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;

import io.kyligence.kap.query.optrule.CountDistinctCaseWhenFunctionRule;
import io.kyligence.kap.query.optrule.FilterJoinConditionMergeRule;
import io.kyligence.kap.query.optrule.FilterSimplifyRule;
import io.kyligence.kap.query.optrule.JoinFilterRule;
import io.kyligence.kap.query.optrule.KapAggFilterTransposeRule;
import io.kyligence.kap.query.optrule.KapAggJoinTransposeRule;
import io.kyligence.kap.query.optrule.KapAggProjectMergeRule;
import io.kyligence.kap.query.optrule.KapAggProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapAggSumCastRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapCountDistinctJoinRule;
import io.kyligence.kap.query.optrule.KapEquiJoinConditionFixRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapProjectMergeRule;
import io.kyligence.kap.query.optrule.KapProjectRule;
import io.kyligence.kap.query.optrule.KapSumCastTransposeRule;
import io.kyligence.kap.query.optrule.KapSumTransCastToThenRule;
import io.kyligence.kap.query.optrule.SumBasicOperatorRule;
import io.kyligence.kap.query.optrule.SumCaseWhenFunctionRule;
import io.kyligence.kap.query.optrule.SumConstantConvertRule;

/**
 * Hep planner help utils
 */
public class HepUtils {
    public static final ImmutableList<RelOptRule> CUBOID_OPT_RULES = ImmutableList.of(
            // Transpose Rule
            KapJoinProjectTransposeRule.BOTH_PROJECT, KapJoinProjectTransposeRule.LEFT_PROJECT,
            KapJoinProjectTransposeRule.RIGHT_PROJECT,
            KapJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.NON_EQUI_LEFT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.NON_EQUI_RIGHT_PROJECT_INCLUDE_OUTER,
            KapEquiJoinConditionFixRule.INSTANCE,
            KapProjectRule.INSTANCE, KapFilterRule.INSTANCE,
            JoinFilterRule.JOIN_LEFT_FILTER, JoinFilterRule.JOIN_RIGHT_FILTER, JoinFilterRule.JOIN_BOTH_FILTER,
            JoinFilterRule.LEFT_JOIN_LEFT_FILTER,
            // Merge Rule
            KapProjectMergeRule.INSTANCE, FilterMergeRule.INSTANCE, ProjectRemoveRule.INSTANCE);

    public static final ImmutableList<RelOptRule> SumExprRules = ImmutableList.of(
            SumCaseWhenFunctionRule.INSTANCE,
            SumBasicOperatorRule.INSTANCE,
            SumConstantConvertRule.INSTANCE,
            KapSumTransCastToThenRule.INSTANCE,
            KapSumCastTransposeRule.INSTANCE,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE,
            KapJoinRule.EQUAL_NULL_SAFE_INSTANT
    );

    public static final ImmutableList<RelOptRule> AggPushDownRules = ImmutableList.of(
            KapAggProjectMergeRule.AGG_PROJECT_JOIN,
            KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN,
            KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN,
            KapAggProjectTransposeRule.AGG_PROJECT_JOIN,
            KapAggFilterTransposeRule.AGG_FILTER_JOIN,
            KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG,
            KapCountDistinctJoinRule.INSTANCE_COUNT_DISTINCT_JOIN_ONESIDEAGG,
            KapCountDistinctJoinRule.INSTANCE_COUNT_DISTINCT_AGG_PROJECT_JOIN,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE,
            KapJoinRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> CountDistinctExprRules = ImmutableList.of(
            CountDistinctCaseWhenFunctionRule.INSTANCE,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE,
            KapJoinRule.EQUAL_NULL_SAFE_INSTANT
    );

    public static final ImmutableList<RelOptRule> SumCastDoubleRules = ImmutableList.of(
            KapAggSumCastRule.INSTANCE,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> FilterReductionRules = ImmutableList.of(
            FilterJoinConditionMergeRule.INSTANCE,
            FilterSimplifyRule.INSTANCE,
            KapFilterRule.INSTANCE
    );


    private HepUtils() {
    }

    public static RelNode runRuleCollection(RelNode rel, Collection<RelOptRule> ruleCollection) {
        return runRuleCollection(rel, ruleCollection, true);
    }

    public static RelNode runRuleCollection(RelNode rel, Collection<RelOptRule> ruleCollection,
            boolean alwaysGenerateNewRelNodes) {
        HepProgram program = HepProgram.builder().addRuleCollection(ruleCollection).build();
        HepPlanner planner = new HepPlanner(program, null, true, null, RelOptCostImpl.FACTORY);
        planner.setRoot(rel);
        if (alwaysGenerateNewRelNodes) {
            return planner.findBestExp();
        } else {
            long ts = planner.getRelMetadataTimestamp(rel);
            RelNode transformed = planner.findBestExp();
            return ts != planner.getRelMetadataTimestamp(rel) ? transformed : rel;
        }
    }
}
