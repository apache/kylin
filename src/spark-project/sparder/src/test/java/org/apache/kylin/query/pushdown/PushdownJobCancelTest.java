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

package org.apache.kylin.query.pushdown;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.util.SlowQueryDetector;
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

public class PushdownJobCancelTest extends NLocalFileMetadataTestCase {

    SparkSession ss;
    private SlowQueryDetector slowQueryDetector = null;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
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
    public void testCancelPushdownJob() throws InterruptedException {
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
                String sql = "select * from TEST_KYLIN_FACT";
                SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), "tpch");
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
    }

}
