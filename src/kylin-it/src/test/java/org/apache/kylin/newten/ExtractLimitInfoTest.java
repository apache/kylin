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

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.query.runtime.plan.ResultPlan;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.common.GlutenTestConfig;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import lombok.val;

public class ExtractLimitInfoTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "false");
        sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "1");
        GlutenTestConfig.configGluten(sparkConf);
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/join_opt");
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/join_opt" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testExtractLimitInfo() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "100");
        fullBuild("8c670664-8d05-466a-802f-83c023b56c77");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        SparkPlan sparkPlan;
        AtomicLong accumRowsCounter;

        val sql1 = "select * from "
                + "(select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME) lside left join "
                + "(select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME) rside "
                + "on lside.TRANS_ID = rside.TRANS_ID limit 10";
        sparkPlan = getSparkExecutedPlan(sql1);
        accumRowsCounter = new AtomicLong();
        ResultPlan.extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter);
        Assert.assertEquals(10010, accumRowsCounter.get());

        val sql2 = "select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME limit 10";
        sparkPlan = getSparkExecutedPlan(sql2);
        accumRowsCounter = new AtomicLong();
        ResultPlan.extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter);
        Assert.assertEquals(10, accumRowsCounter.get());

        val sql3 = "select count(*) from (select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME limit 10)";
        sparkPlan = getSparkExecutedPlan(sql3);
        accumRowsCounter = new AtomicLong();
        ResultPlan.extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter);
        Assert.assertEquals(10, accumRowsCounter.get());
    }

    private SparkPlan getSparkExecutedPlan(String sql) throws SQLException {
        return ExecAndComp.queryModel(getProject(), sql).queryExecution().executedPlan();
    }

    @Override
    public String getProject() {
        return "join_opt";
    }

}
