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
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.query.optrule.FilterJoinConditionMergeRule;
import io.kyligence.kap.query.optrule.FilterSimplifyRule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FilterSimplifyRuleTest extends CalciteRuleTestBase {
    static final String defaultProject = "default";
    private final DiffRepository diff = DiffRepository.lookup(FilterSimplifyRuleTest.class);


    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    @Test
    public void test() throws IOException {
        final List<RelOptRule> rules = new ArrayList<>();
        rules.add(FilterSimplifyRule.INSTANCE);
        rules.add(FilterJoinConditionMergeRule.INSTANCE);
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/sql_filter_simplify");

        queries.forEach(e -> checkSQLPostOptimize(defaultProject, e.getSecond(), e.getFirst(), null, rules));
    }
}
