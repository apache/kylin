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
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.query.optrule.AggregateProjectReduceRule;
import org.apache.kylin.query.optrule.CountDistinctCaseWhenFunctionRule;
import org.apache.kylin.query.optrule.FilterJoinConditionMergeRule;
import org.apache.kylin.query.optrule.FilterSimplifyRule;
import org.apache.kylin.query.optrule.JoinFilterRule;
import org.apache.kylin.query.optrule.OlapAggFilterTransposeRule;
import org.apache.kylin.query.optrule.OlapAggJoinTransposeRule;
import org.apache.kylin.query.optrule.OlapAggProjectMergeRule;
import org.apache.kylin.query.optrule.OlapAggProjectTransposeRule;
import org.apache.kylin.query.optrule.OlapAggSumCastRule;
import org.apache.kylin.query.optrule.OlapAggregateRule;
import org.apache.kylin.query.optrule.OlapCountDistinctJoinRule;
import org.apache.kylin.query.optrule.OlapEquivJoinConditionFixRule;
import org.apache.kylin.query.optrule.OlapFilterRule;
import org.apache.kylin.query.optrule.OlapJoinProjectTransposeRule;
import org.apache.kylin.query.optrule.OlapJoinRule;
import org.apache.kylin.query.optrule.OlapProjectMergeRule;
import org.apache.kylin.query.optrule.OlapProjectRule;
import org.apache.kylin.query.optrule.OlapSumCastTransposeRule;
import org.apache.kylin.query.optrule.OlapSumTransCastToThenRule;
import org.apache.kylin.query.optrule.ScalarSubqueryJoinRule;
import org.apache.kylin.query.optrule.SumBasicOperatorRule;
import org.apache.kylin.query.optrule.SumCaseWhenFunctionRule;
import org.apache.kylin.query.optrule.SumConstantConvertRule;

/**
 * Hep planner help utils
 */
public class HepUtils {
    public static final ImmutableList<RelOptRule> CUBOID_OPT_RULES = ImmutableList.of(
            // Transpose Rule
            OlapJoinProjectTransposeRule.BOTH_PROJECT, //
            OlapJoinProjectTransposeRule.LEFT_PROJECT, //
            OlapJoinProjectTransposeRule.RIGHT_PROJECT, //
            OlapJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER, //
            OlapJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER, //
            OlapJoinProjectTransposeRule.NON_EQUI_LEFT_PROJECT_INCLUDE_OUTER, //
            OlapJoinProjectTransposeRule.NON_EQUI_RIGHT_PROJECT_INCLUDE_OUTER, //
            OlapEquivJoinConditionFixRule.INSTANCE, //
            OlapProjectRule.INSTANCE, //
            OlapFilterRule.INSTANCE, //
            JoinFilterRule.JOIN_LEFT_FILTER, //
            JoinFilterRule.JOIN_RIGHT_FILTER, //
            JoinFilterRule.JOIN_BOTH_FILTER, //
            JoinFilterRule.LEFT_JOIN_LEFT_FILTER,
            // Merge Rule
            OlapProjectMergeRule.INSTANCE, //
            CoreRules.FILTER_MERGE, //
            CoreRules.PROJECT_REMOVE //
    );

    public static final ImmutableList<RelOptRule> SumExprRules = ImmutableList.of(
            // sum expression rules
            SumCaseWhenFunctionRule.INSTANCE, //
            SumBasicOperatorRule.INSTANCE, //
            SumConstantConvertRule.INSTANCE, //
            OlapSumTransCastToThenRule.INSTANCE, //
            OlapSumCastTransposeRule.INSTANCE, //
            OlapProjectRule.INSTANCE, //
            OlapAggregateRule.INSTANCE, //
            OlapJoinRule.EQUAL_NULL_SAFE_INSTANT //
    );

    public static final ImmutableList<RelOptRule> AggPushDownRules = ImmutableList.of(
            // aggregation push down rules
            OlapAggProjectMergeRule.AGG_PROJECT_JOIN, //
            OlapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN, //
            OlapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN, //
            OlapAggProjectTransposeRule.AGG_PROJECT_JOIN, //
            OlapAggFilterTransposeRule.AGG_FILTER_JOIN, //
            OlapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG, //
            OlapCountDistinctJoinRule.COUNT_DISTINCT_JOIN_ONE_SIDE_AGG, //
            OlapCountDistinctJoinRule.COUNT_DISTINCT_AGG_PROJECT_JOIN, //
            OlapProjectRule.INSTANCE, //
            OlapAggregateRule.INSTANCE, //
            OlapJoinRule.INSTANCE //
    );

    public static final ImmutableList<RelOptRule> ScalarSubqueryJoinRules = ImmutableList.of(
            // base rules
            OlapAggregateRule.INSTANCE, //
            OlapProjectRule.INSTANCE, //
            OlapJoinRule.INSTANCE, //
            // relative rules
            CoreRules.PROJECT_MERGE, //
            CoreRules.AGGREGATE_PROJECT_MERGE, //
            AggregateProjectReduceRule.INSTANCE, //
            // target rules
            ScalarSubqueryJoinRule.AGG_JOIN, //
            ScalarSubqueryJoinRule.AGG_PRJ_JOIN, //
            ScalarSubqueryJoinRule.AGG_FLT_JOIN, //
            ScalarSubqueryJoinRule.AGG_PRJ_FLT_JOIN //
    );

    public static final ImmutableList<RelOptRule> CountDistinctExprRules = ImmutableList.of(
            // count distinct rules
            CountDistinctCaseWhenFunctionRule.INSTANCE, //
            OlapProjectRule.INSTANCE, //
            OlapAggregateRule.INSTANCE, //
            OlapJoinRule.EQUAL_NULL_SAFE_INSTANT //
    );

    public static final ImmutableList<RelOptRule> SumCastDoubleRules = ImmutableList.of(
            // sum cast rules
            OlapAggSumCastRule.INSTANCE, //
            OlapProjectRule.INSTANCE, //
            OlapAggregateRule.INSTANCE //
    );

    public static final ImmutableList<RelOptRule> FilterReductionRules = ImmutableList.of(
            // filter reduction rules
            FilterJoinConditionMergeRule.INSTANCE, //
            FilterSimplifyRule.INSTANCE, //
            OlapFilterRule.INSTANCE //
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
