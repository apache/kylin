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

package org.apache.kylin.query.engine;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.base.Function;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.query.engine.meta.PlannerContext;
import org.apache.kylin.query.optrule.AggregateMultipleExpandRule;
import org.apache.kylin.query.optrule.AggregateProjectReduceRule;
import org.apache.kylin.query.optrule.CorrReduceFunctionRule;
import org.apache.kylin.query.optrule.OlapAggregateReduceFunctionsRule;
import org.apache.kylin.query.optrule.OlapAggregateRule;
import org.apache.kylin.query.optrule.OlapFilterJoinRule;
import org.apache.kylin.query.optrule.OlapFilterRule;
import org.apache.kylin.query.optrule.OlapJoinRule;
import org.apache.kylin.query.optrule.OlapLimitRule;
import org.apache.kylin.query.optrule.OlapMinusRule;
import org.apache.kylin.query.optrule.OlapModelViewRule;
import org.apache.kylin.query.optrule.OlapProjectMergeRule;
import org.apache.kylin.query.optrule.OlapProjectRule;
import org.apache.kylin.query.optrule.OlapReduceExpressionRule;
import org.apache.kylin.query.optrule.OlapSortRule;
import org.apache.kylin.query.optrule.OlapToEnumerableConverterRule;
import org.apache.kylin.query.optrule.OlapUnionRule;
import org.apache.kylin.query.optrule.OlapValuesRule;
import org.apache.kylin.query.optrule.OlapWindowRule;
import org.apache.kylin.query.optrule.RightJoinToLeftJoinRule;
import org.apache.kylin.query.relnode.ContextUtil;

/**
 * factory that create optimizers and register opt rules
 * TODO sort and register only necessary rules
 */
public class PlannerFactory {

    public static final List<RelOptRule> ENUMERABLE_RULES = ImmutableList.of(
            // enumerable rules
            EnumerableRules.ENUMERABLE_JOIN_RULE, //
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE, //
            EnumerableRules.ENUMERABLE_CORRELATE_RULE, //
            EnumerableRules.ENUMERABLE_PROJECT_RULE, //
            EnumerableRules.ENUMERABLE_FILTER_RULE, //
            EnumerableRules.ENUMERABLE_CALC_RULE, //
            EnumerableRules.ENUMERABLE_AGGREGATE_RULE, //
            EnumerableRules.ENUMERABLE_SORT_RULE, //
            EnumerableRules.ENUMERABLE_LIMIT_RULE, //
            EnumerableRules.ENUMERABLE_COLLECT_RULE, //
            EnumerableRules.ENUMERABLE_UNCOLLECT_RULE, //
            EnumerableRules.ENUMERABLE_MERGE_UNION_RULE, //
            EnumerableRules.ENUMERABLE_UNION_RULE, //
            EnumerableRules.ENUMERABLE_REPEAT_UNION_RULE, //
            EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE, //
            EnumerableRules.ENUMERABLE_INTERSECT_RULE, //
            EnumerableRules.ENUMERABLE_MINUS_RULE, //
            EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE, //
            EnumerableRules.ENUMERABLE_VALUES_RULE, //
            EnumerableRules.ENUMERABLE_WINDOW_RULE, //
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE, //
            EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE //
    );

    private static final List<RelOptRule> DEFAULT_RULES = ImmutableList.of(
            // default core rules
            CoreRules.AGGREGATE_STAR_TABLE, //
            CoreRules.AGGREGATE_PROJECT_STAR_TABLE, //
            TableScanRule.INSTANCE, //
            CoreRules.PROJECT_MERGE, //
            CoreRules.FILTER_SCAN, //
            CoreRules.PROJECT_FILTER_TRANSPOSE, //
            CoreRules.FILTER_PROJECT_TRANSPOSE, //
            CoreRules.FILTER_INTO_JOIN, //
            CoreRules.JOIN_PUSH_EXPRESSIONS, //
            CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES, //
            OlapAggregateReduceFunctionsRule.INSTANCE, //
            CoreRules.FILTER_AGGREGATE_TRANSPOSE, //
            CoreRules.PROJECT_WINDOW_TRANSPOSE, //
            CoreRules.JOIN_COMMUTE, //
            JoinPushThroughJoinRule.RIGHT, //
            JoinPushThroughJoinRule.LEFT, //
            CoreRules.SORT_PROJECT_TRANSPOSE, //
            CoreRules.SORT_JOIN_TRANSPOSE, //
            CoreRules.SORT_UNION_TRANSPOSE //
    );

    private final KylinConfig kylinConfig;

    public PlannerFactory(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public VolcanoPlanner createVolcanoPlanner(CalciteConnectionConfig connectionConfig) {
        VolcanoPlanner planner = new VolcanoPlanner(new PlannerContext(connectionConfig));
        registerDefaultRules(planner);
        registerCustomRules(planner);
        return planner;
    }

    private void registerDefaultRules(VolcanoPlanner planner) {
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        planner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRules(planner);
        // see https://olapio.atlassian.net/browse/KE-42056
        // Calcite 1.30 with this rule may cause cost error
        planner.removeRule(PruneEmptyRules.SORT_INSTANCE);
        for (RelOptRule rule : DEFAULT_RULES) {
            planner.addRule(rule);
        }
        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(CoreRules.PROJECT_TABLE_SCAN);
        planner.addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);
        for (RelOptRule rule : ENUMERABLE_RULES) {
            planner.addRule(rule);
        }
        planner.addRule(EnumerableRules.TO_INTERPRETER);

        for (RelOptRule rule : StreamRules.RULES) {
            planner.addRule(rule);
        }
    }

    private void registerCustomRules(VolcanoPlanner planner) {
        // force clear the query context before traversal relational operators
        ContextUtil.clearThreadLocalContexts();
        // register OLAP rules
        planner.addRule(OlapToEnumerableConverterRule.INSTANCE);
        planner.addRule(OlapFilterRule.INSTANCE);
        planner.addRule(OlapProjectRule.INSTANCE);
        planner.addRule(OlapAggregateRule.INSTANCE);
        planner.addRule(selectJoinRuleByConfig());
        planner.addRule(OlapLimitRule.INSTANCE);
        planner.addRule(OlapSortRule.INSTANCE);
        planner.addRule(OlapUnionRule.INSTANCE);
        planner.addRule(OlapWindowRule.INSTANCE);
        planner.addRule(OlapValuesRule.INSTANCE);
        planner.addRule(OlapMinusRule.INSTANCE);
        planner.addRule(OlapModelViewRule.INSTANCE);
        planner.removeRule(CoreRules.PROJECT_MERGE);
        planner.addRule(OlapProjectMergeRule.INSTANCE);

        // Support translate the grouping aggregate into union of simple aggregates
        // if it's the auto-modeling dry run, then do not add the CorrReduceFunctionRule
        // Todo cherry-pick CORR measure
        //        if (!KapConfig.getInstanceFromEnv().getSkipCorrReduceRule()) {
        //            planner.addRule(CorrReduceFunctionRule.INSTANCE);
        //        }
        if (KapConfig.getInstanceFromEnv().splitGroupSetsIntoUnion()) {
            planner.addRule(AggregateMultipleExpandRule.INSTANCE);
        }
        planner.addRule(AggregateProjectReduceRule.INSTANCE);

        // CalcitePrepareImpl.CONSTANT_REDUCTION_RULES
        if (kylinConfig.isReduceExpressionsRulesEnabled()) {
            planner.addRule(OlapReduceExpressionRule.PROJECT_INSTANCE);
            planner.addRule(OlapReduceExpressionRule.FILTER_INSTANCE);
            planner.addRule(OlapReduceExpressionRule.JOIN_INSTANCE);
            planner.addRule(OlapReduceExpressionRule.CALC_INSTANCE);
        }
        // the ValuesReduceRule breaks query test somehow...
        //   planner.addRule(ValuesReduceRule.FILTER_INSTANCE);
        //   planner.addRule(ValuesReduceRule.PROJECT_FILTER_INSTANCE);
        //   planner.addRule(ValuesReduceRule.PROJECT_INSTANCE);

        removeRules(planner, kylinConfig.getCalciteRemoveRule());
        if (!kylinConfig.isEnumerableRulesEnabled()) {
            for (RelOptRule rule : EnumerableRules.ENUMERABLE_RULES) {
                planner.removeRule(rule);
            }
        }
        // since join is the entry point, we can't push filter past join
        planner.removeRule(CoreRules.FILTER_INTO_JOIN);
        planner.removeRule(CoreRules.JOIN_CONDITION_PUSH);
        planner.addRule(OlapFilterJoinRule.OLAP_FILTER_ON_JOIN_JOIN);
        planner.addRule(OlapFilterJoinRule.OLAP_FILTER_ON_JOIN_SCAN);
        // since we don't have statistic of table, the optimization of join is too cost
        planner.removeRule(CoreRules.JOIN_COMMUTE);
        planner.removeRule(JoinPushThroughJoinRule.LEFT);
        planner.removeRule(JoinPushThroughJoinRule.RIGHT);

        // keep tree structure like filter -> aggregation -> project -> join/table scan,
        // implementOLAP() rely on this tree pattern
        //        planner.removeRule(AggregateJoinTransposeRule.INSTANCE);
        planner.removeRule(CoreRules.AGGREGATE_PROJECT_MERGE);
        planner.removeRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
        planner.removeRule(CoreRules.SORT_JOIN_TRANSPOSE);
        planner.removeRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
        planner.removeRule(CoreRules.SORT_UNION_TRANSPOSE);
        planner.removeRule(CoreRules.JOIN_LEFT_UNION_TRANSPOSE);
        planner.removeRule(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE);
        planner.removeRule(CoreRules.AGGREGATE_UNION_TRANSPOSE);
        planner.removeRule(DateRangeRules.FILTER_INSTANCE);
        planner.removeRule(CoreRules.JOIN_TO_SEMI_JOIN);
        planner.removeRule(CoreRules.PROJECT_TO_SEMI_JOIN);
        // distinct count will be split into a separated query that is joined with the left query
        planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);

        // see Dec 26th email @ http://mail-archives.apache.org/mod_mbox/calcite-dev/201412.mbox/browser
        planner.removeRule(AbstractConverter.ExpandConversionRule.INSTANCE);
        // convert all right joins to left join since we only support left joins in model
        planner.addRule(RightJoinToLeftJoinRule.INSTANCE);
        // UnionMergeRule may slow volcano planner optimization on large number of union clause
        // see KAP#16036
        planner.removeRule(CoreRules.UNION_MERGE);
        planner.removeRule(CoreRules.PROJECT_REMOVE);

        // skip corr expansion during model suggestion
        if (!KylinConfig.getInstanceFromEnv().getSkipCorrReduceRule()) {
            planner.addRule(CorrReduceFunctionRule.INSTANCE);
        }
    }

    private ConverterRule selectJoinRuleByConfig() {
        return (kylinConfig.isQueryNonEquiJoinModelEnabled() && !QueryContext.current().isForModeling())
                || (kylinConfig.isNonEquiJoinRecommendationEnabled() && QueryContext.current().isForModeling()) //
                        ? OlapJoinRule.NON_EQUI_INSTANCE
                        : OlapJoinRule.INSTANCE;
    }

    protected void removeRules(final RelOptPlanner planner, List<String> rules) {
        modifyRules(rules, new Function<RelOptRule, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable RelOptRule input) {
                planner.removeRule(input);
                return null;
            }
        });
    }

    private void modifyRules(List<String> rules, Function<RelOptRule, Void> func) {
        for (String rule : rules) {
            if (StringUtils.isEmpty(rule)) {
                continue;
            }
            String[] split = rule.split("#");
            if (split.length != 2) {
                throw new RuntimeException("Customized Rule should be in format <RuleClassName>#<FieldName>");
            }
            String clazz = split[0];
            String field = split[1];
            try {
                func.apply((RelOptRule) Class.forName(clazz).getDeclaredField(field).get(null));
            } catch (IllegalAccessException | ClassNotFoundException | NoSuchFieldException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
