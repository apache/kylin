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


package org.apache.kylin.query.rules;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.query.optrule.KapAggFilterTransposeRule;
import io.kyligence.kap.query.optrule.KapAggJoinTransposeRule;
import io.kyligence.kap.query.optrule.KapAggProjectMergeRule;
import io.kyligence.kap.query.optrule.KapAggProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapProjectRule;

public class AggPushdownRuleTest extends CalciteRuleTestBase {

    private final DiffRepository diff = DiffRepository.lookup(AggPushdownRuleTest.class);
    private final String project = "subquery";
    private final List<RelOptRule> rulesDefault = Lists.newArrayList();

    @Before
    public void setUp() {
        overwriteSystemProp("calcite.keep-in-clause", "false");
        createTestMetadata("src/test/resources/ut_meta/agg_push_down");
        rulesDefault.add(KapFilterRule.INSTANCE);
        rulesDefault.add(KapProjectRule.INSTANCE);
        rulesDefault.add(KapAggregateRule.INSTANCE);
        rulesDefault.add(KapJoinRule.INSTANCE);
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    @Test
    //Test with VolcanoPlanner for all rules
    public void testAddRules() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Pair<String, String>> queries = readALLSQLs(config, project, "query/sql_select_subquery");
        List<RelOptRule> rulesToAdd = Lists.newArrayList();
        rulesToAdd.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);
        rulesToAdd.add(KapAggProjectMergeRule.AGG_PROJECT_JOIN);
        rulesToAdd.add(KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN);
        rulesToAdd.add(KapAggProjectTransposeRule.AGG_PROJECT_JOIN);
        rulesToAdd.add(KapAggFilterTransposeRule.AGG_FILTER_JOIN);
        rulesToAdd.add(KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG);
        for (Pair<String, String> pair : queries) {
            RelNode relBefore = toCalcitePlan(project, pair.getSecond(), config);
            RelNode relAfter = toCalcitePlan(project, pair.getSecond(), config, null, rulesToAdd);
            checkPlanning(relBefore, relAfter, pair.getFirst());
        }
    }

    @Test
    //Test with HepPlanner for single rule
    public void testAggProjectMergeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query05.sql");

        List<RelOptRule> rules = Lists.newArrayList(rulesDefault);
        rules.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesDefault);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst());
    }

    @Test
    public void testAggProjectTransposeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query01.sql");

        List<RelOptRule> rules = Lists.newArrayList(rulesDefault);
        rules.add(KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesDefault);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst());
    }

    @Test
    public void testAggFilterTransposeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query05.sql");

        List<RelOptRule> rulesBefore = Lists.newArrayList(rulesDefault);
        rulesBefore.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);

        List<RelOptRule> rules = Lists.newArrayList(rulesBefore);
        rules.add(KapAggFilterTransposeRule.AGG_FILTER_JOIN);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesBefore);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst());
    }

    @Test
    public void testAggJoinTransposeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query05.sql");

        List<RelOptRule> rulesBefore = Lists.newArrayList(rulesDefault);
        rulesBefore.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);
        rulesBefore.add(KapAggFilterTransposeRule.AGG_FILTER_JOIN);

        List<RelOptRule> rules = Lists.newArrayList(rulesBefore);
        rules.add(KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesBefore);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst());
    }
}
