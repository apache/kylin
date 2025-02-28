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

package org.apache.kylin.query.runtime.plan;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.BigQueryException;
import org.apache.kylin.common.exception.NewQueryRefuseException;
import org.apache.kylin.common.state.StateSwitchConstant;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.BigQueryThresholdUpdater;
import org.apache.kylin.metadata.state.QueryShareStateManager;
import org.apache.kylin.query.MockContext;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.pushdown.SparkSqlClient;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class TestResultPlan extends NLocalFileMetadataTestCase {

    private SlowQueryDetector slowQueryDetector = null;
    SparkSession ss;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=");
        getTestConfig().setProperty("kylin.query.share-state-switch-implement", "jdbc");
        getTestConfig().setProperty("kylin.query.big-query-source-scan-rows-threshold", "100000000");
        ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        SparderEnv.registerListener(ss.sparkContext());
        SparderEnv.setSparkSession(ss);
        StructType schema = new StructType();
        schema = schema.add("TRANS_ID", DataTypes.LongType, false);
        schema = schema.add("ORDER_ID", DataTypes.LongType, false);
        schema = schema.add("CAL_DT", DataTypes.DateType, false);
        schema = schema.add("LSTG_FORMAT_NAME", DataTypes.StringType, false);
        schema = schema.add("LEAF_CATEG_ID", DataTypes.LongType, false);
        schema = schema.add("LSTG_SITE_ID", DataTypes.IntegerType, false);
        schema = schema.add("SLR_SEGMENT_CD", DataTypes.FloatType, false);
        schema = schema.add("SELLER_ID", DataTypes.LongType, false);
        schema = schema.add("PRICE", DataTypes.createDecimalType(19, 4), false);
        schema = schema.add("ITEM_COUNT", DataTypes.DoubleType, false);
        schema = schema.add("TEST_COUNT_DISTINCT_BITMAP", DataTypes.StringType, false);
        ss.read().schema(schema).csv("../../examples/test_case_data/localmeta/data/DEFAULT.TEST_KYLIN_FACT.csv")
                .createOrReplaceTempView("TEST_KYLIN_FACT");
        slowQueryDetector = new SlowQueryDetector(100000, 5 * 1000);
        slowQueryDetector.start();
    }

    @After
    public void after() throws Exception {
        slowQueryDetector.interrupt();
        ss.stop();
        cleanupTestMetadata();
        SparderEnv.clean();
    }

    @Test
    public void testRefuseNewBigQuery() {
        QueryShareStateManager.getInstance().setState(Collections.singletonList(AddressUtil.concatInstanceName()),
                StateSwitchConstant.QUERY_LIMIT_STATE, "true");
        QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics()
                .addAccumSourceScanRows(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
        String sql = "select * from TEST_KYLIN_FACT";
        try {
            ResultPlan.getResult(ss.sql(sql), null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NewQueryRefuseException);
        }
    }

    @Test
    public void testCancelQuery() throws InterruptedException {
        overwriteSystemProp("kylin.query.use-iterable-collect", "true");
        AtomicReference<SparkListenerJobEnd> sparkJobEnd = new AtomicReference<>();
        CountDownLatch isJobEnd = new CountDownLatch(1);
        ss.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onTaskStart(SparkListenerTaskStart taskStart) {
                for (SlowQueryDetector.QueryEntry e : SlowQueryDetector.getRunningQueries().values()) {
                    e.setStopByUser(true);
                    e.getThread().interrupt();
                    return;
                }
                Assert.fail("no running query is found");
            }

            @Override
            public void onJobEnd(SparkListenerJobEnd jobEnd) {
                sparkJobEnd.set(jobEnd);
                isJobEnd.countDown();
            }
        });
        Thread queryThread = new Thread(() -> {
            try {
                slowQueryDetector.queryStart("foo");
                QueryShareStateManager.getInstance().setState(
                        Collections.singletonList(AddressUtil.concatInstanceName()),
                        StateSwitchConstant.QUERY_LIMIT_STATE, "false");
                QueryContext.current().getMetrics().addAccumSourceScanRows(
                        KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
                String sql = "select * from TEST_KYLIN_FACT";
                ResultPlan.getResult(ss.sql(sql), null);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof UserStopQueryException);
            } finally {
                slowQueryDetector.queryEnd();
            }
        });
        queryThread.start();
        queryThread.join();
        isJobEnd.await(10, TimeUnit.SECONDS);
        Assert.assertTrue(sparkJobEnd.get().jobResult() instanceof JobFailed);
        Assert.assertTrue(((JobFailed) sparkJobEnd.get().jobResult()).exception().getMessage()
                .contains("cancelled part of cancelled job group"));

        Thread queryThread2 = new Thread(() -> {
            try {
                slowQueryDetector.queryStart("foo");
                QueryShareStateManager.getInstance().setState(
                        Collections.singletonList(AddressUtil.concatInstanceName()),
                        StateSwitchConstant.QUERY_LIMIT_STATE, "true");
                QueryContext.current().getMetrics().addAccumSourceScanRows(
                        KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() - 1);
                String sql = "select * from TEST_KYLIN_FACT";
                ResultPlan.getResult(ss.sql(sql), null);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof UserStopQueryException);
            } finally {
                slowQueryDetector.queryEnd();
            }
        });
        queryThread2.start();
        queryThread2.join();
    }

    @Test
    public void testSetQueryFairSchedulerPool() {
        long fakeScanRows = 10000;
        int fakePartitionNum = 5;
        String pool;

        SparkConf sparkConf = new SparkConf();
        QueryContext queryContext = QueryContext.current();

        long bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold();
        queryContext.getQueryTagInfo().setHighPriorityQuery(true);
        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                fakePartitionNum);
        Assert.assertEquals("vip_tasks", pool);
        queryContext.getQueryTagInfo().setHighPriorityQuery(false);

        queryContext.getQueryTagInfo().setTableIndex(true);
        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                fakePartitionNum);
        Assert.assertEquals("extreme_heavy_tasks", pool);
        queryContext.getQueryTagInfo().setTableIndex(false);

        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                fakePartitionNum);
        Assert.assertEquals("heavy_tasks", pool);
        fakePartitionNum = SparderEnv.getTotalCore() - 1;
        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                fakePartitionNum);
        Assert.assertEquals("lightweight_tasks", pool);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.query-limit-enabled", "true");
            sparkConf.set("spark.dynamicAllocation.enabled", "true");
            sparkConf.set("spark.dynamicAllocation.maxExecutors", "1");
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "-1");

            BigQueryThresholdUpdater.resetBigQueryThreshold();
            Assert.assertEquals(-1, BigQueryThresholdUpdater.getBigQueryThreshold());
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", String.valueOf(fakeScanRows + 1));
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(1, 1);
            bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold();
            Assert.assertEquals(fakeScanRows + 1, bigQueryThreshold);
            pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                    fakePartitionNum);
            Assert.assertEquals("lightweight_tasks", pool);

            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", String.valueOf(fakeScanRows - 1));
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(1, 1);
            bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold();
            Assert.assertEquals(fakeScanRows - 1, bigQueryThreshold);
            pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                    fakePartitionNum);
            Assert.assertEquals("heavy_tasks", pool);

            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", String.valueOf(-1));
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(1, 1);
            bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold();
            Assert.assertEquals(12891053, bigQueryThreshold);
            pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, bigQueryThreshold, fakeScanRows,
                    fakePartitionNum);
            Assert.assertEquals("lightweight_tasks", pool);
        }
    }

    @Test
    public void testAsyncQueryWriteParquet() {
        QueryContext queryContext = QueryContext.current();
        queryContext.getQueryTagInfo().setAsyncQuery(true);
        queryContext.getQueryTagInfo().setFileFormat("parquet");
        queryContext.getQueryTagInfo().setFileEncode("utf-8");
        String sql = "select * from TEST_KYLIN_FACT";
        val resultType = MockContext.current().getRelDataType();
        ResultPlan.getResult(ss.sql(sql), resultType);
    }

    @Test
    public void testBigQueryException() {
        QueryShareStateManager.getInstance().setState(Collections.singletonList(AddressUtil.concatInstanceName()),
                StateSwitchConstant.QUERY_LIMIT_STATE, "true");
        QueryContext queryContext = QueryContext.current();
        queryContext.setIfBigQuery(true);
        queryContext.getMetrics()
                .addAccumSourceScanRows(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
        String sql = "select * from TEST_KYLIN_FACT";
        try {
            ResultPlan.getResult(ss.sql(sql), null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof BigQueryException);
        }
    }

    @Test
    public void testNoBigQueryException() {
        QueryContext queryContext = QueryContext.current();
        queryContext.setIfBigQuery(true);
        queryContext.getMetrics()
                .addAccumSourceScanRows(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
        String sql = "select * from TEST_KYLIN_FACT";
        try {
            ResultPlan.getResult(ss.sql(sql), null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof BigQueryException);
        }
    }
    @Test
    public void testSparkSqlClient() {

        QueryContext queryContext = QueryContext.current();
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).createProject("test", "ADMIN", "des",
                new LinkedHashMap<String, String>());
        String sql = "select * from TEST_KYLIN_FACT";
        overwriteSystemProp("kylin.query.use-iterable-collect", "true");
        val reuslt1 = SparkSqlClient.executeSqlToIterable(ss, sql, UUID.randomUUID(), "test");
        val queryResult1 = new QueryResult(reuslt1._1(), (int) reuslt1._2(), reuslt1._3());

        overwriteSystemProp("kylin.query.use-iterable-collect", "false");
        val reuslt2 = SparkSqlClient.executeSqlToIterable(ss, sql, UUID.randomUUID(), "test");
        val queryResult2 = new QueryResult(reuslt2._1(), (int) reuslt2._2(), reuslt2._3());
        CollectionUtils.isEqualCollection(queryResult1.getRows(), queryResult2.getRows());
    }

    @Test
    public void testSparkEnvDeleteBlock() {
        QueryContext queryContext = QueryContext.current();
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).createProject("test", "ADMIN", "des",
                new LinkedHashMap<String, String>());
        String sql = "select * from TEST_KYLIN_FACT t1 left join (select * from TEST_KYLIN_FACT limit 10) t2 on 1=1  ";
        overwriteSystemProp("kylin.query.use-iterable-collect", "true");
        val reuslt1 = SparkSqlClient.executeSqlToIterable(ss, sql, UUID.randomUUID(), "test");
        SparderEnv.deleteQueryTaskResultBlock(queryContext.getExecutionID());
        try {
            while (reuslt1._1().iterator().hasNext()) {
                reuslt1._1().iterator().next();
            }
        }catch (Exception e){
            assert e.getMessage().contains("Failed to fetch block after 1 fetch failures");
        }
    }

}
