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

package org.apache.kylin.newten;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
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

public class ExactlyMatchTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/agg_exact_match");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "agg_match";
    }

    @Test
    public void testInClause() throws Exception {
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String base = "select count(*) from TEST_KYLIN_FACT ";

        String in1 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) in ('O','B')";

        String in2 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) in ('A','F')"
                + " and substring(LSTG_FORMAT_NAME, 1, 1) in ('O','B')";

        String not_in1 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) not in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) not in ('O','B')";

        String not_in2 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) not in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) not in ('O','B')";

        overwriteSystemProp("calcite.keep-in-clause", "true");
        Dataset<Row> df1 = ExecAndComp.queryModelWithoutCompute(getProject(), in1);
        Dataset<Row> df2 = ExecAndComp.queryModelWithoutCompute(getProject(), in2);
        Dataset<Row> df3 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);
        Dataset<Row> df4 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);

        Assert.assertTrue(existsIn(df1));
        Assert.assertTrue(existsIn(df2));
        Assert.assertTrue(existsIn(df3));
        Assert.assertTrue(existsIn(df4));
        ArrayList<String> querys = Lists.newArrayList(in1, in2, not_in1, not_in2);
        ExecAndComp.execAndCompareQueryList(querys, getProject(), ExecAndComp.CompareLevel.SAME, "left");

        overwriteSystemProp("calcite.keep-in-clause", "false");
        Dataset<Row> df5 = ExecAndComp.queryModelWithoutCompute(getProject(), in1);
        Dataset<Row> df6 = ExecAndComp.queryModelWithoutCompute(getProject(), in2);
        Dataset<Row> df7 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);
        Dataset<Row> df8 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);

        Assert.assertFalse(existsIn(df5));
        Assert.assertFalse(existsIn(df6));
        Assert.assertFalse(existsIn(df7));
        Assert.assertFalse(existsIn(df8));
    }

    @Test
    public void testSkipAgg() throws Exception {
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String exactly_match1 = "select count (distinct price) as a from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME having count (distinct price)  > 0 ";
        Dataset<Row> m1 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match1);
        Assert.assertFalse(existsAgg(m1));

        String exactly_match2 = "select count(*) from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m2 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match2);
        Assert.assertFalse(existsAgg(m2));

        String exactly_match3 = "select LSTG_FORMAT_NAME,sum(price),CAL_DT from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m3 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match3);
        String[] fieldNames = m3.schema().fieldNames();
        Assert.assertFalse(existsAgg(m3));
        Assert.assertTrue(fieldNames[0].contains("LSTG_FORMAT_NAME"));
        Assert.assertTrue(fieldNames[1].contains("GMV_SUM"));
        Assert.assertTrue(fieldNames[2].contains("CAL_DT"));

        String exactly_match4 = "select count (distinct price) as a from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME having count (distinct price)  > 0 ";
        Dataset<Row> m4 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match1);
        Assert.assertFalse(existsAgg(m4));
        // assert results
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", exactly_match3));
        query.add(Pair.newPair("", exactly_match4));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");

        String not_match1 = "select count(*) from TEST_KYLIN_FACT";
        Dataset<Row> n1 = ExecAndComp.queryModelWithoutCompute(getProject(), not_match1);
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
