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
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.query.optrule.AggregateProjectReduceRule;
import org.apache.kylin.query.optrule.OlapAggregateRule;
import org.apache.kylin.query.optrule.OlapFilterRule;
import org.apache.kylin.query.optrule.OlapJoinRule;
import org.apache.kylin.query.optrule.OlapProjectRule;
import org.apache.kylin.query.optrule.OlapReduceExpressionRule;
import org.apache.kylin.query.optrule.ScalarSubqueryJoinRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScalarSubqueryJoinRuleTest extends CalciteRuleTestBase {

    private final String project = "default";
    private final String sqlFolder = "query/sql_scalar_subquery";
    //
    private final DiffRepository diff = DiffRepository.lookup(ScalarSubqueryJoinRuleTest.class);

    @Before
    public void setUp() {
        createTestMetadata();
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
    public void testPushDownAggregate() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Pair<String, String>> queries = readALLSQLs(config, project, sqlFolder);
        List<RelOptRule> rulesToAdd = getTransformRules();

        queries.forEach(p -> doCheckTransform(p.getFirst(), p.getSecond(), rulesToAdd));
    }

    private void doCheckTransform(String prefix, String sql, List<RelOptRule> rulesToAdd) {
        checkSQLPostOptimize(project, sql, prefix, null, rulesToAdd);
    }

    @Test
    @Ignore("Dev only")
    public void testSingle() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<RelOptRule> rulesToAdd = getTransformRules();
        Pair<String, String> q = readOneSQL(config, project, sqlFolder, "query03.sql");
        doCheckTransform(q.getFirst(), q.getSecond(), rulesToAdd);
    }

    @Test
    @Ignore("Dev only")
    public void dumpPlans() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), project, sqlFolder);
        CalciteRuleTestBase.StringOutput output = new CalciteRuleTestBase.StringOutput(false);
        final List<RelOptRule> rulesToAdd = getTransformRules();
        queries.forEach(p -> checkSQLPostOptimize(project, p.getSecond(), p.getFirst(), output, rulesToAdd));
        output.dump(log);
    }

    private List<RelOptRule> getTransformRules() {
        return ImmutableList.of(// basic rules
                OlapAggregateRule.INSTANCE, //
                OlapProjectRule.INSTANCE, //
                OlapFilterRule.INSTANCE, //
                OlapJoinRule.INSTANCE, //
                // relative rules
                CoreRules.PROJECT_MERGE, //
                CoreRules.AGGREGATE_PROJECT_MERGE, //
                AggregateProjectReduceRule.INSTANCE, //
                OlapReduceExpressionRule.PROJECT_INSTANCE, //
                // target rules
                ScalarSubqueryJoinRule.AGG_JOIN, //
                ScalarSubqueryJoinRule.AGG_PRJ_JOIN, //
                ScalarSubqueryJoinRule.AGG_FLT_JOIN, //
                ScalarSubqueryJoinRule.AGG_PRJ_FLT_JOIN);
    }
}
