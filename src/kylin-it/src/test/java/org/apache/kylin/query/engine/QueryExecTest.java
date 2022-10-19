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

import java.sql.SQLException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.relnode.KapRel;
import org.apache.kylin.query.runtime.CalciteToSparkPlaner;
import org.apache.kylin.query.util.QueryContextCutter;
import org.apache.kylin.query.util.QueryHelper;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.util.FieldUtils;

import lombok.val;
import scala.Function0;

public class QueryExecTest extends NLocalFileMetadataTestCase {

    final String project = "default";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    private Dataset<Row> check(String SQL) throws SQLException {
        SparderEnv.skipCompute();
        QueryExec qe = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        qe.executeQuery(SQL);
        Dataset<Row> dataset = SparderEnv.getDF();
        Assert.assertNotNull(dataset);
        SparderEnv.cleanCompute();
        return dataset;
    }

    /**
     *  <p>See also {@link org.apache.kylin.query.engine.QueryExecTest#testSumCaseWhenHasNull()}
     * @throws SQLException
     */
    @Test
    public void testWorkWithoutKapAggregateReduceFunctionsRule() throws SQLException {
        // Can not reproduce https://github.com/Kyligence/KAP/issues/15261 at 4.x
        // we needn't introduce KapAggregateReduceFunctionsRule as we did in 3.x
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
        String SQL = "select sum(t.a1 * 2)  from ("
                + "select sum(price/2) as a1, sum(ITEM_COUNT) as a2 from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"
                + ") t";
        Assert.assertNotNull(check(SQL));
    }

    /**
     * See {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}, it will rewrite <code>Sum(x)</code> to
     * <code>case COUNT(x) when 0 then null else SUM0(x) end</code>.
     *
     * <p>This rule doesn't consider situation where x is null, and still convert it to
     * <code>case COUNT(null) when 0 then null else SUM0(null) end</code>, which is incompatible with model section
     *
     * <p>See also {@link org.apache.kylin.query.engine.QueryExecTest#testWorkWithoutKapAggregateReduceFunctionsRule()}
     * @throws SqlParseException
     */
    @Test
    public void testSumCaseWhenHasNull() throws SQLException {
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
        String SQLWithZero = "select CAL_DT,\n"
                + "       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 else 0 end)\n"
                + "from TEST_KYLIN_FACT\n" + "group by CAL_DT";
        check(SQLWithZero);
        String SQLWithNull = "select CAL_DT,\n"
                + "       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 else null end)\n"
                + "from TEST_KYLIN_FACT\n" + "group by CAL_DT";
        check(SQLWithNull);
    }

    @Test
    public void testSingleQuery() throws SQLException {
        SparderEnv.skipCompute();
        try {
            String sql = "select CAL_DT, count(*) from TEST_KYLIN_FACT group by CAL_DT";
            SparkSession session = SparderEnv.getSparkSession();
            Dataset<Row> dataset = QueryHelper.sql(session, project, sql);
            Assert.assertNotNull(dataset);
        } finally {
            SparderEnv.cleanCompute();
        }
    }

    @Test
    public void testSingleQueryWithError() {
        SparderEnv.skipCompute();
        // useless, only for sonar condition coverage
        val prevRunLocalConf = System.getProperty("kylin.query.engine.run-constant-query-locally");
        Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
        Exception expectException = null;
        try {
            String sql = "select CAL_DT, count(*) from TEST_KYLIN_FACT group by CAL_DT_2";
            SparkSession session = SparderEnv.getSparkSession();
            Dataset<Row> dataset = QueryHelper.sql(session, project, sql);
            Assert.assertNotNull(dataset);
        } catch (Exception e) {
            expectException = e;
        } finally {
            Assert.assertTrue(expectException instanceof AnalysisException);
            SparderEnv.cleanCompute();
            if (prevRunLocalConf != null) {
                Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf);
            }
        }
    }

    @Test
    public void testSparkPlanWithoutCache() throws IllegalAccessException, SqlParseException {
        overwriteSystemProp("kylin.query.dataframe-cache-enabled", "false");
        String sql = "select count(*) from TEST_KYLIN_FACT group by seller_id";
        QueryExec qe = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode root = qe.parseAndOptimize(sql);
        SimpleDataContext dataContext = (SimpleDataContext) FieldUtils.getFieldValue(qe, "dataContext");
        QueryContextCutter.selectRealization(root, BackdoorToggles.getIsQueryFromAutoModeling());
        CalciteToSparkPlaner calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext) {
            @Override
            public Dataset<Row> actionWithCache(KapRel rel, Function0<Dataset<Row>> body) {
                throw new RuntimeException();
            }
        };
        try {
            calciteToSparkPlaner.go(root.getInput(0));
        } finally {
            calciteToSparkPlaner.cleanCache();
        }
    }

    @Test
    public void testSparkPlanWithCache() throws IllegalAccessException, SqlParseException {
        overwriteSystemProp("kylin.query.dataframe-cache-enabled", "true");
        String sql = "select count(*) from TEST_KYLIN_FACT group by seller_id";
        QueryExec qe = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode root = qe.parseAndOptimize(sql);
        SimpleDataContext dataContext = (SimpleDataContext) FieldUtils.getFieldValue(qe, "dataContext");
        QueryContextCutter.selectRealization(root, BackdoorToggles.getIsQueryFromAutoModeling());
        CalciteToSparkPlaner calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext) {
            @Override
            public Dataset<Row> actionWithCache(KapRel rel, Function0<Dataset<Row>> body) {
                throw new IllegalStateException();
            }
        };
        Exception expectException = null;
        try {
            calciteToSparkPlaner.go(root.getInput(0));
        } catch (Exception e) {
            expectException = e;
        } finally {
            calciteToSparkPlaner.cleanCache();
            Assert.assertTrue(expectException instanceof IllegalStateException);
        }
    }

}
