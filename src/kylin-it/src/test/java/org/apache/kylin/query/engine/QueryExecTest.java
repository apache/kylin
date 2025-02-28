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
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.util.QueryHelper;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(overlay = "src/test/resources/ut_meta/heterogeneous_segment_2")
class QueryExecTest {

    public String getProject() {
        return "default";
    }

    private Dataset<Row> check(String SQL) throws SQLException {
        SparderEnv.skipCompute();
        QueryExec qe = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv());
        qe.executeQuery(SQL);
        Dataset<Row> dataset = SparderEnv.getDF();
        Assertions.assertNotNull(dataset);
        SparderEnv.cleanCompute();
        return dataset;
    }

    /**
     * <p>See also {@link org.apache.kylin.query.engine.QueryExecTest#testSumCaseWhenHasNull()}
     *
     */
    @Test
    void testWorkWithoutKapAggregateReduceFunctionsRule() throws SQLException {
        // Can not reproduce https://github.com/Kyligence/KAP/issues/15261 at 4.x
        // we needn't introduce KapAggregateReduceFunctionsRule as we did in 3.x
        TestUtils.getTestConfig().setProperty("kylin.query.convert-sum-expression-enabled", "true");
        String SQL = "select sum(t.a1 * 2)  from ("
                + "select sum(price/2) as a1, sum(ITEM_COUNT) as a2 from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"
                + ") t";
        Assertions.assertNotNull(check(SQL));
    }

    /**
     * See {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}, it will rewrite <code>Sum(x)</code> to
     * <code>case COUNT(x) when 0 then null else SUM0(x) end</code>.
     *
     * <p>This rule doesn't consider situation where x is null, and still convert it to
     * <code>case COUNT(null) when 0 then null else SUM0(null) end</code>, which is incompatible with model section
     *
     * <p>See also {@link org.apache.kylin.query.engine.QueryExecTest#testWorkWithoutKapAggregateReduceFunctionsRule()}
     *
     */
    @Test
    void testSumCaseWhenHasNull() throws SQLException {
        TestUtils.getTestConfig().setProperty("kylin.query.convert-sum-expression-enabled", "true");
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
    void testSingleQuery() {
        SparderEnv.skipCompute();
        try {
            String sql = "select CAL_DT, count(*) from TEST_KYLIN_FACT group by CAL_DT";
            SparkSession session = SparderEnv.getSparkSession();
            Dataset<Row> dataset = QueryHelper.sql(session, getProject(), sql);
            Assertions.assertNotNull(dataset);
        } finally {
            SparderEnv.cleanCompute();
        }
    }

    @Test
    void testSingleQueryWithError() {
        SparderEnv.skipCompute();
        // useless, only for sonar condition coverage
        String prevRunLocalConf = System.getProperty("kylin.query.engine.run-constant-query-locally");
        Unsafe.clearProperty("kylin.query.engine.run-constant-query-locally");
        Exception expectException = null;
        try {
            String sql = "select CAL_DT, count(*) from TEST_KYLIN_FACT group by CAL_DT_2";
            SparkSession session = SparderEnv.getSparkSession();
            Dataset<Row> dataset = QueryHelper.sql(session, getProject(), sql);
            Assertions.assertNotNull(dataset);
        } catch (Exception e) {
            expectException = e;
        } finally {
            Assertions.assertInstanceOf(AnalysisException.class, expectException);
            SparderEnv.cleanCompute();
            if (prevRunLocalConf != null) {
                Unsafe.setProperty("kylin.query.engine.run-constant-query-locally", prevRunLocalConf);
            }
        }
    }

    @Test
    void testModelRealizationAndOutOfSegmentRange() {
        SparderEnv.skipCompute();
        try {
            // PART_DT>'2024-01-01', out of segment range
            String sql = "select PART_DT, count(*) from KYLIN_SALES inner join kylin_account on SELLER_ID=ACCOUNT_ID"
                    + " where PART_DT>'2024-01-01' group by PART_DT";
            SparkSession session = SparderEnv.getSparkSession();
            Dataset<Row> dataset = QueryHelper.sql(session, "heterogeneous_segment_2", sql);
            Assertions.assertNotNull(dataset);

            List<NativeQueryRealization> realizationList = ContextUtil.getNativeRealizations();
            Assertions.assertEquals(1, realizationList.size());
            NativeQueryRealization realization = realizationList.get(0);
            Assertions.assertNotNull(realization.getModelId());
            Assertions.assertNotNull(realization.getModelAlias());
            Assertions.assertNull(realization.getLayoutId());
            Assertions.assertNull(realization.getType());
        } finally {
            SparderEnv.cleanCompute();
        }
    }
}
