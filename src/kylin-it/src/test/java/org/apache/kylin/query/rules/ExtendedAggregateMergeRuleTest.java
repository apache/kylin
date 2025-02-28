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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.test.DiffRepository;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.optrule.ExtendedAggregateMergeRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExtendedAggregateMergeRuleTest extends CalciteRuleTestBase {

    private static final String PROJECT = "default";

    @Before
    public void before() {
        createTestMetadata();
        overwriteSystemProp("kylin.query.improved-sum-decimal-precision.enabled", "true");
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testAggMerge() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Pair<String, String>> queries = readALLSQLs(config, PROJECT, "query/sql_agg_merge");

        for (Pair<String, String> pair : queries) {
            final String queryKey = pair.getFirst();
            final String sql = pair.getSecond();
            RelNode relBefore = toCalcitePlan(PROJECT, sql, config,
                    Collections.singletonList(ExtendedAggregateMergeRule.INSTANCE), null);
            RelNode relAfter = toCalcitePlan(PROJECT, sql, config, null, null);
            checkPlanning(relBefore, relAfter, queryKey);
        }

    }

    @Test
    public void testCalciteAggregateMergeRuleFail() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, PROJECT, "query/sql_agg_merge", "query02.sql");

        final String sql = query.getSecond();
        try {
            toCalcitePlan(PROJECT, sql, config, Collections.singletonList(ExtendedAggregateMergeRule.INSTANCE),
                    Collections.singletonList(CoreRules.AGGREGATE_MERGE));
            Assert.fail("Should not reach here");
        } catch (Exception e) {
            Throwable th = ExceptionUtils.getRootCause(e);
            if (th != null) {
                String errorMsg = th.getMessage();
                log.info("actual error msg is: \n{}", errorMsg);
                Assert.assertTrue(errorMsg
                        .contains("Type mismatch:\n" + "rel rowtype: RecordType(DECIMAL(30, 4) EXPR$0) NOT NULL\n"
                                + "equiv rowtype: RecordType(DECIMAL(38, 4) EXPR$0) NOT NULL\n" + "Difference:\n"
                                + "EXPR$0: DECIMAL(30, 4) -> DECIMAL(38, 4)"));
            } else {
                Assert.fail("Should not reach here");
            }
        }
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return DiffRepository.lookup(this.getClass());
    }
}
