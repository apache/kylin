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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.rules.CalciteRuleTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class SqlToRelNodeTest extends CalciteRuleTestBase {

    private static final String PROJECT = "default";

    DiffRepository diff;
    KylinConfig config;
    QueryExec queryExec;

    private final String NL = System.getProperty("line.separator");

    @Before
    public void setup() {
        staticCreateTestMetadata();
        diff = DiffRepository.lookup(SqlToRelNodeTest.class);
        config = KylinConfig.getInstanceFromEnv();
        queryExec = new QueryExec(PROJECT, config);
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    @Test
    public void testConvertSqlToRelNode_whenManyUnionAndWith() throws Exception {
        Pair<String, String> sql = readOneSQL(config, PROJECT, "query/sql_union", "query07.sql");
        RelRoot relRoot = queryExec.sqlToRelRoot(sql.getSecond());
        RelNode rel = queryExec.optimize(relRoot).rel;
        final String realPlan = NL + RelOptUtil.toString(rel);

        // check rel node is meet except
        diff.assertEquals("query07.planExpect", "${query07.planExpect}", realPlan);

        // check rel node is valid
        RelValidityChecker checker = new RelValidityChecker();
        checker.go(rel);
        Assert.assertEquals(0, checker.invalidCount);
    }

    @Test
    public void testInNull() throws Exception {
        Pair<String, String> sql = readOneSQL(config, PROJECT, "query/sql_in", "query02.sql");
        checkSQLOptimize(PROJECT, sql.getSecond(), "query_sql_in_query02");
    }

    @Test
    public void testProjectMergeWithBloat() throws Exception {
        String sql = "select q.x + q.x from( select (p.v + p.v) as x from (select (case when TRANS_ID > 60 then 1 else 0 end) v from test_kylin_fact) p)q";

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-with-bloat-enabled", "false");
            QueryExec exec1 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec1.sqlToRelRoot(sql);
            RelNode rel = exec1.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_disabled", "${bloat_merge_sql.bloat_disabled}", realPlan);
        }

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-with-bloat-enabled", "true");
            QueryExec exec2 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec2.sqlToRelRoot(sql);
            RelNode rel = exec2.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_enabled", "${bloat_merge_sql.bloat_enabled}", realPlan);
        }

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-with-bloat-enabled", "true");
            config.setProperty("kylin.query.project-merge-bloat-threshold", "5");
            QueryExec exec3 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec3.sqlToRelRoot(sql);
            RelNode rel = exec3.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_enabled_bloat_5", "${bloat_merge_sql.bloat_enabled_bloat_5}", realPlan);
        }

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-bloat-threshold", "100");
            QueryExec exec4 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec4.sqlToRelRoot(sql);
            RelNode rel = exec4.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_disabled", "${bloat_merge_sql.bloat_disabled}", realPlan);
        }
    }

    /**
     * Visitor that checks that every {@link RelNode} in a tree is valid.
     *
     * @see RelNode#isValid(Litmus, RelNode.Context)
     */
    public static class RelValidityChecker extends RelVisitor
            implements RelNode.Context {
        int invalidCount;
        final Deque<RelNode> stack = new ArrayDeque<>();

        public Set<CorrelationId> correlationIds() {
            final ImmutableSet.Builder<CorrelationId> builder =
                    ImmutableSet.builder();
            for (RelNode r : stack) {
                builder.addAll(r.getVariablesSet());
            }
            return builder.build();
        }

        public void visit(RelNode node, int ordinal, RelNode parent) {
            try {
                stack.push(node);
                if (!node.isValid(Litmus.THROW, this)) {
                    ++invalidCount;
                }
                super.visit(node, ordinal, parent);
            } finally {
                stack.pop();
            }
        }
    }
}
