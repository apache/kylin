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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.rules.CalciteRuleTestBase;
import org.apache.kylin.query.util.HepUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnhancedAggPushDownPlannerTest extends CalciteRuleTestBase {

    static final String defaultProject = "test_agg_pushdown";
    static final DiffRepository diff = DiffRepository.lookup(EnhancedAggPushDownPlannerTest.class);

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/enhanced_agg_pushdown");
        overwriteSystemProp("kylin.query.enhanced-agg-pushdown-enabled", "true");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    protected void checkSQL(String project, String sql, String prefix, StringOutput StrOut,
            Collection<RelOptRule>... ruleSets) {
        Collection<RelOptRule> rules = new HashSet<>();
        for (Collection<RelOptRule> ruleSet : ruleSets) {
            rules.addAll(ruleSet);
        }
        super.checkSQLAfterRule(project, sql, prefix, StrOut, rules);
    }

    @Test
    public void testEnhancedAggPushDown() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/enhanced_agg_pushdown");
        QueryContext.current().setProject(defaultProject);
        Collection<RelOptRule> postOptRules = new LinkedHashSet<>();
        postOptRules.addAll(HepUtils.SumExprRules);
        postOptRules.addAll(HepUtils.CountDistinctExprRules);
        postOptRules.addAll(HepUtils.AggPushDownRules);
        queries.forEach(e -> checkSQL(defaultProject, e.getSecond(), e.getFirst(), null, postOptRules));
    }

}
