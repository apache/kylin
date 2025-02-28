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
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.rules.CalciteRuleTestBase;
import org.apache.kylin.query.util.HepUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumCastDoublePlannerTest extends CalciteRuleTestBase {

    static final String defaultProject = "default";
    static final DiffRepository diff = DiffRepository.lookup(SumCastDoublePlannerTest.class);

    @Before
    public void setUp() {
        createTestMetadata();
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.optimized-sum-cast-double-rule-enabled", "true");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    protected void checkSQL(String project, String sql, String prefix, StringOutput StrOut,
            Collection<RelOptRule>... ruleSets) {
        Collection<RelOptRule> rules = new LinkedHashSet<>();
        for (Collection<RelOptRule> ruleSet : ruleSets) {
            rules.addAll(ruleSet);
        }
        super.checkSQLPostOptimize(project, sql, prefix, StrOut, rules);
    }

    @Test
    public void testSumCastDouble() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/sum_cast_double");
        queries.forEach(e -> checkSQL(defaultProject, e.getSecond(), e.getFirst(), null, HepUtils.SumCastDoubleRules));
    }

}
