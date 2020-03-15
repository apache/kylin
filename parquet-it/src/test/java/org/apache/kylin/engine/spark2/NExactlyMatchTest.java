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

package org.apache.kylin.engine.spark2;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.runtime.AbstractFunction1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class NExactlyMatchTest extends LocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/agg_exact_match");
        DefaultScheduler scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        DefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Override
    public String getProject() {
        return "agg_match";
    }

    @Test
    public void testInClause() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long start = f.parse("2012-01-01").getTime();
        long end = f.parse("2015-01-01").getTime();
        buildCuboid("ci_inner_join_cube", new SegmentRange.TSRange(start, end));

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());

        String base = "select count(*) from TEST_KYLIN_FACT ";

        String in1 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) in ('O','B')";

        String in2 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) in ('A','F')"
                + " and substring(LSTG_FORMAT_NAME, 1, 1) in ('O','B')";

        String not_in1 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) not in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) not in ('O','B')";

        String not_in2 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) not in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) not in ('O','B')";

        System.setProperty("calcite.keep-in-clause", "true");
        Dataset<Row> df1 = NExecAndComp.queryCubeAndSkipCompute(getProject(), in1);
        Dataset<Row> df2 = NExecAndComp.queryCubeAndSkipCompute(getProject(), in2);
        Dataset<Row> df3 = NExecAndComp.queryCubeAndSkipCompute(getProject(), not_in2);
        Dataset<Row> df4 = NExecAndComp.queryCubeAndSkipCompute(getProject(), not_in2);

        Assert.assertTrue(existsIn(df1));
        Assert.assertTrue(existsIn(df2));
        Assert.assertTrue(existsIn(df3));
        Assert.assertTrue(existsIn(df4));
        ArrayList<String> querys = Lists.newArrayList(in1, in2, not_in1, not_in2);
        NExecAndComp.execAndCompareQueryList(querys, getProject(), NExecAndComp.CompareLevel.SAME, "left");

        System.setProperty("calcite.keep-in-clause", "false");
        Dataset<Row> df5 = NExecAndComp.queryCubeAndSkipCompute(getProject(), in1);
        Dataset<Row> df6 = NExecAndComp.queryCubeAndSkipCompute(getProject(), in2);
        Dataset<Row> df7 = NExecAndComp.queryCubeAndSkipCompute(getProject(), not_in2);
        Dataset<Row> df8 = NExecAndComp.queryCubeAndSkipCompute(getProject(), not_in2);

        Assert.assertFalse(existsIn(df5));
        Assert.assertFalse(existsIn(df6));
        Assert.assertFalse(existsIn(df7));
        Assert.assertFalse(existsIn(df8));
    }

    @Test
    public void testSkipAgg() throws Exception {
        fullBuildCube("ci_inner_join_cube");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());

        String exactly_match1 = "select sum(price) from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m1 = NExecAndComp.queryCubeAndSkipCompute(getProject(), exactly_match1);
        Assert.assertFalse(existsAgg(m1));

        String exactly_match2 = "select count(*) from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m2 = NExecAndComp.queryCubeAndSkipCompute(getProject(), exactly_match2);
        Assert.assertFalse(existsAgg(m2));

        String exactly_match3 = "select LSTG_FORMAT_NAME,sum(price),CAL_DT from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m3 = NExecAndComp.queryCubeAndSkipCompute(getProject(), exactly_match3);
        String[] fieldNames = m3.schema().fieldNames();
        Assert.assertFalse(existsAgg(m3));
        Assert.assertTrue(fieldNames[0].contains("LSTG_FORMAT_NAME"));
        Assert.assertTrue(fieldNames[1].contains("GMV_SUM"));
        Assert.assertTrue(fieldNames[2].contains("CAL_DT"));

        // assert results
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", exactly_match3));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");


        String not_match1 = "select count(*) from TEST_KYLIN_FACT";
        Dataset<Row> n1 = NExecAndComp.queryCubeAndSkipCompute(getProject(), not_match1);
        Assert.assertTrue(existsAgg(n1));
    }

    private boolean existsAgg(Dataset<Row> m1) {
        return !m1.logicalPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                return v1 instanceof Aggregate;
            }
        }).isEmpty();
    }

    private boolean existsIn(Dataset<Row> m1) {
        Option<LogicalPlan> option = m1.logicalPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                return v1 instanceof Filter;
            }
        });

        if (option.isDefined()) {
            Filter filter = (Filter) option.get();
            return filter.condition().find(new AbstractFunction1<Expression, Object>() {
                @Override
                public Object apply(Expression v1) {
                    return v1 instanceof In;
                }
            }).isDefined();
        } else {
            return false;
        }
    }
}
