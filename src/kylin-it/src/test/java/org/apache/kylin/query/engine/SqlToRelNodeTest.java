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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.query.rules.CalciteRuleTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SqlToRelNodeTest extends CalciteRuleTestBase {

    private static final String PROJECT = "default";

    DiffRepository diff;
    KylinConfig config;
    QueryExec queryExec;

    private final String NL = System.getProperty("line.separator");

    @Before
    public void setUp() {
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
        String sql = "select q.x + q.x from( select (p.v + p.v) as x from ("
                + "select (case when TRANS_ID > 60 then 1 else 0 end) v from test_kylin_fact "
                + "left join TEST_ACCOUNT on SELLER_ID = ACCOUNT_ID) p)q";

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-bloat-threshold", "-1");
            QueryExec exec2 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec2.sqlToRelRoot(sql);
            System.out.println(RelOptUtil.toString(relRoot.rel));
            RelNode rel = exec2.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_with_minus1", "${bloat_merge_sql.bloat_with_minus1}", realPlan);
        }

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-bloat-threshold", "5");
            QueryExec exec3 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec3.sqlToRelRoot(sql);
            RelNode rel = exec3.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_with_5", "${bloat_merge_sql.bloat_with_5}", realPlan);
        }

        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.project-merge-bloat-threshold", "100");
            QueryExec exec4 = new QueryExec(PROJECT, config);
            RelRoot relRoot = exec4.sqlToRelRoot(sql);
            RelNode rel = exec4.optimize(relRoot).rel;
            String realPlan = NL + RelOptUtil.toString(rel);
            diff.assertEquals("bloat_merge_sql.bloat_with_100", "${bloat_merge_sql.bloat_with_100}", realPlan);
        }
    }

    @Test
    public void testBigDecimalQuotientWithScale() throws SqlParseException {
        String SQL = "select sum(a1/3.000000000000005) as a2 from ("
                + "select sum(price/2.000000000000000001) as a1 from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME" + ") t";
        RelRoot relRoot = queryExec.sqlToRelRoot(SQL);
        RelNode rel = queryExec.optimize(relRoot).rel;
        Assert.assertEquals(1, rel.getRowType().getFieldList().size());
        RelDataType relDataType = rel.getRowType().getFieldList().get(0).getValue();
        Assert.assertEquals(SqlTypeName.DECIMAL, relDataType.getSqlTypeName());
        Assert.assertEquals(38, relDataType.getPrecision());
        Assert.assertEquals(6, relDataType.getScale());
    }

    /**
     * Visitor that checks that every {@link RelNode} in a tree is valid.
     *
     * @see RelNode#isValid(Litmus, RelNode.Context)
     */
    public static class RelValidityChecker extends RelVisitor implements RelNode.Context {
        int invalidCount;
        final Deque<RelNode> stack = new ArrayDeque<>();

        public Set<CorrelationId> correlationIds() {
            final ImmutableSet.Builder<CorrelationId> builder = ImmutableSet.builder();
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

    @Test
    public void testSelectConstantSimplify() throws Exception {
        overwriteSystemProp("kylin.query.convert-in-to-or-threshold", "0");
        Pair<String, String> sql = readOneSQL(config, "ssb", "query/sql_sqlnode", "query01.sql");
        checkSQLOptimize("ssb", sql.getSecond(), "query01");
    }
}
