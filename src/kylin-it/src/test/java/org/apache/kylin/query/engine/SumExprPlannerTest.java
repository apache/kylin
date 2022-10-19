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
import java.util.List;

import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.rules.CalciteRuleTestBase;
import org.apache.kylin.query.util.HepUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.query.optrule.KapAggFilterTransposeRule;
import io.kyligence.kap.query.optrule.KapAggJoinTransposeRule;
import io.kyligence.kap.query.optrule.KapAggProjectMergeRule;
import io.kyligence.kap.query.optrule.KapAggProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapCountDistinctJoinRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapProjectRule;
import io.kyligence.kap.query.optrule.KapSumCastTransposeRule;
import io.kyligence.kap.query.optrule.KapSumTransCastToThenRule;
import io.kyligence.kap.query.optrule.SumBasicOperatorRule;
import io.kyligence.kap.query.optrule.SumCaseWhenFunctionRule;
import io.kyligence.kap.query.optrule.SumConstantConvertRule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumExprPlannerTest extends CalciteRuleTestBase {

    static final String defaultProject = "default";
    static final DiffRepository diff = DiffRepository.lookup(SumExprPlannerTest.class);

    private void openSumCaseWhen() {
        // we must make sure kap.query.enable-convert-sum-expression is TRUE to
        // avoid adding SumConstantConvertRule in PlannerFactory
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
    }

    private void closeSumCaseWhen() {
        // some sql failed in new SumConstantConvertRule
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "false");
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        closeSumCaseWhen();
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    private void checkSQL(String defaultProject, String second, String first, StringOutput output) {
        super.checkSQLPostOptimize(defaultProject, second, first, output, HepUtils.SumExprRules);
    }

    @Test
    @Ignore("For development")
    public void dumpPlans() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/sql_sum_expr");
        CalciteRuleTestBase.StringOutput output = new CalciteRuleTestBase.StringOutput(false);
        queries.forEach(e -> checkSQL(defaultProject, e.getSecond(), e.getFirst(), output));
        output.dump(log);
    }

    @Test
    public void testAllCases() throws IOException {
        openSumCaseWhen();
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/sql_sum_expr");
        queries.forEach(e -> checkSQL(defaultProject, e.getSecond(), e.getFirst(), null));
    }

    @Test
    public void testSimpleSQL() {
        openSumCaseWhen();
        String SQL = "SELECT " + "SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE 2 END) "
                + "FROM TEST_KYLIN_FACT";
        checkSQL(defaultProject, SQL, null, null);
    }

    /**
     * see https://olapio.atlassian.net/browse/KE-14512
     */
    @Test
    public void testWithAVG() {
        openSumCaseWhen();
        String SQL = "SELECT " + "AVG(PRICE) as price1 "
                + ",SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE 0 END) as total_price "
                + "from TEST_KYLIN_FACT";
        checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testKE13524() throws IOException {
        // see https://olapio.atlassian.net/browse/KE-13524 for details
        closeSumCaseWhen();
        String project = "newten";
        Pair<String, String> query = readOneSQL(KylinConfig.getInstanceFromEnv(), project, "sql_sinai_poc",
                "query15.sql");
        Assert.assertNotNull(query.getSecond());
        Assert.assertNotNull(toCalcitePlan(project, query.getSecond(), KylinConfig.getInstanceFromEnv()));

        String SQL = "select sum(2), sum(0), count(1) from XXXXXXXXX_XXXXXXXXX.X_XXXXXXXX_XX_XX";
        Assert.assertNotNull(toCalcitePlan(project, SQL, KylinConfig.getInstanceFromEnv()));

    }

    @Test
    public void testSumCastTransposeRule() {
        String SQL = "SELECT SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE LEAF_CATEG_ID END) FROM TEST_KYLIN_FACT";
        openSumCaseWhen();
        checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testSumCastTransposeRule2() {
        openSumCaseWhen();
        String SQL = "select sum(cast(price as bigint)) from TEST_KYLIN_FACT";
        checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testSumCastTransposeRule3() {
        String SQL = "select sum(cast(price as bigint)), LSTG_FORMAT_NAME  from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME";
        openSumCaseWhen();
        checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testSumCastTransposeRule4() {
        openSumCaseWhen();
        String SQL = "select sum(cast(price as bigint)) from TEST_KYLIN_FACT"
                + " INNER JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
        checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testSumCastTransposeRuleWithGroupby() {
        openSumCaseWhen();
        String SQL = "SELECT SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE LEAF_CATEG_ID END), "
                + "LSTG_FORMAT_NAME FROM TEST_KYLIN_FACT group by LSTG_FORMAT_NAME";
        checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testAggPushdown() {

        String SQL = "SELECT \"自定义 SQL 查询\".\"CAL_DT\" ,\n"
                + "       SUM (\"自定义 SQL 查询\".\"SELLER_ID\") AS \"TEMP_Calculation_54915774428294\",\n"
                + "            COUNT (\"自定义 SQL 查询\".\"CAL_DT\") AS \"TEMP_Calculation_97108873613918\",\n"
                + "               COUNT (DISTINCT \"自定义 SQL 查询\".\"CAL_DT\") AS \"TEMP_Calculation_97108873613918\",\n"
                + "                     COUNT (DISTINCT (CASE\n"
                + "                                          WHEN (\"t0\".\"x_measure__0\" > 0) THEN \"t0\".\"LSTG_FORMAT_NAME\"\n"
                + "                                          ELSE CAST (NULL AS VARCHAR (1))\n"
                + "                                      END)) AS \"TEMP_Calculation_97108873613911\"\n" + "FROM\n"
                + "  (SELECT *\n" + "   FROM TEST_KYLIN_FACT) \"自定义 SQL 查询\"\n" + "INNER JOIN\n"
                + "     (SELECT LSTG_FORMAT_NAME, ORDER_ID, SUM (\"PRICE\") AS \"X_measure__0\"\n"
                + "      FROM TEST_KYLIN_FACT  GROUP  BY LSTG_FORMAT_NAME, ORDER_ID) \"t0\" ON \"自定义 SQL 查询\".\"ORDER_ID\" = \"t0\".\"ORDER_ID\"\n"
                + "GROUP  BY \"自定义 SQL 查询\".\"CAL_DT\"\n";
        super.checkSQLPostOptimize(defaultProject, SQL, null, null,
                ImmutableList.of(KapAggProjectMergeRule.AGG_PROJECT_JOIN,
                        KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN,
                        KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN, KapAggProjectTransposeRule.AGG_PROJECT_JOIN,
                        KapAggFilterTransposeRule.AGG_FILTER_JOIN, KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG,
                        KapCountDistinctJoinRule.INSTANCE_COUNT_DISTINCT_JOIN_ONESIDEAGG, KapAggregateRule.INSTANCE,
                        KapJoinRule.INSTANCE, KapProjectRule.INSTANCE));
    }

    @Test
    public void testAggPushdownWithCrossJoin1() {

        String SQL = "SELECT\n"
                + " SUM(\n"
                + " (\n"
                + " CASE\n"
                + " WHEN (\n"
                + " CASE\n"
                + " WHEN (\"LINEORDER\".\"LO_ORDERDATE\" = \"t0\".\"X_measure__0\") THEN true\n"
                + " WHEN NOT (\"LINEORDER\".\"LO_ORDERDATE\" = \"t0\".\"X_measure__0\") THEN false\n"
                + " ELSE NULL\n"
                + " END\n"
                + " ) THEN \"LINEORDER\".\"LO_QUANTITY\"\n"
                + " ELSE CAST(NULL AS INTEGER)\n"
                + " END\n"
                + " )\n"
                + " ) AS \"sum_LO_QUANTITY_SUM______88\"\n"
                + "FROM\n"
                + " \"SSB\".\"LINEORDER\" \"LINEORDER\"\n"
                + " CROSS JOIN (\n"
                + " SELECT\n"
                + " MAX(\"LINEORDER\".\"LO_ORDERDATE\") AS \"X_measure__0\"\n"
                + " FROM\n"
                + " \"SSB\".\"LINEORDER\" \"LINEORDER\"\n"
                + " GROUP BY\n"
                + " 1.1000000000000001\n"
                + " ) \"t0\"\n"
                + "GROUP BY\n"
                + " 1.1000000000000001\n"
                + "LIMIT 500";
        super.checkSQLPostOptimize(defaultProject, SQL, null, null,
                ImmutableList.of(SumCaseWhenFunctionRule.INSTANCE,
                        SumBasicOperatorRule.INSTANCE,
                        SumConstantConvertRule.INSTANCE,
                        KapSumTransCastToThenRule.INSTANCE,
                        KapSumCastTransposeRule.INSTANCE,
                        KapProjectRule.INSTANCE,
                        KapAggregateRule.INSTANCE,
                        KapJoinRule.EQUAL_NULL_SAFE_INSTANT,
                        KapAggProjectMergeRule.AGG_PROJECT_JOIN,
                        KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN,
                        KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN,
                        KapAggProjectTransposeRule.AGG_PROJECT_JOIN,
                        KapAggFilterTransposeRule.AGG_FILTER_JOIN,
                        KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG,
                        KapCountDistinctJoinRule.INSTANCE_COUNT_DISTINCT_JOIN_ONESIDEAGG));
    }

    @Test
    public void testAggPushdownWithCrossJoin2() {

        String SQL = "SELECT\n"
                + " SUM(\n"
                + " (\n"
                + " CASE\n"
                + " WHEN (\n"
                + " CASE\n"
                + " WHEN (\"LINEORDER\".\"LO_ORDERDATE\" = \"t0\".\"X_measure__0\") THEN true\n"
                + " WHEN NOT (\"LINEORDER\".\"LO_ORDERDATE\" = \"t0\".\"X_measure__0\") THEN false\n"
                + " ELSE NULL\n"
                + " END\n"
                + " ) THEN \"LINEORDER\".\"LO_QUANTITY\"\n"
                + " ELSE CAST(NULL AS INTEGER)\n"
                + " END\n"
                + " )\n"
                + " ) AS \"sum_LO_QUANTITY_SUM______88\"\n"
                + "FROM\n"
                + " \"SSB\".\"LINEORDER\" \"LINEORDER\"\n"
                + " CROSS JOIN (\n"
                + " SELECT\n"
                + " MAX(\"LINEORDER\".\"LO_ORDERDATE\") AS \"X_measure__0\"\n"
                + " FROM\n"
                + " \"SSB\".\"LINEORDER\" \"LINEORDER\"\n"
                + " GROUP BY\n"
                + " 1.1000000000000001\n"
                + " ) \"t0\"\n"
                + "where \"LINEORDER\".\"LO_QUANTITY\" > 1 \n"
                + "GROUP BY\n"
                + " 1.1000000000000001\n"
                + "LIMIT 500";
        super.checkSQLPostOptimize(defaultProject, SQL, null, null,
                ImmutableList.of(SumCaseWhenFunctionRule.INSTANCE,
                        SumBasicOperatorRule.INSTANCE,
                        SumConstantConvertRule.INSTANCE,
                        KapSumTransCastToThenRule.INSTANCE,
                        KapSumCastTransposeRule.INSTANCE,
                        KapProjectRule.INSTANCE,
                        KapAggregateRule.INSTANCE,
                        KapJoinRule.EQUAL_NULL_SAFE_INSTANT,
                        KapAggProjectMergeRule.AGG_PROJECT_JOIN,
                        KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN,
                        KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN,
                        KapAggProjectTransposeRule.AGG_PROJECT_JOIN,
                        KapAggFilterTransposeRule.AGG_FILTER_JOIN,
                        KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG,
                        KapCountDistinctJoinRule.INSTANCE_COUNT_DISTINCT_JOIN_ONESIDEAGG));
    }
}
