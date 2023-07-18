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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.query.engine.PrepareSqlStateParam;
import org.apache.kylin.query.pushdown.SparkSqlClient;
import org.apache.kylin.query.util.PrepareSQLUtils;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.collect.Sets;

import scala.collection.JavaConversions;

@RunWith(TimeZoneTestRunner.class)
public class TimeZoneQueryTest extends NLocalWithSparkSessionTest {
    private static final Logger log = LoggerFactory.getLogger(TimeZoneQueryTest.class);

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC) {
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        }
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/timezone");
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
        return "timezone";
    }

    private String sql = "select TEST_ORDER.TEST_TIME_ENC as ts1, CAL_DT as dt1, cast (TEST_ORDER_STRING.TEST_TIME_ENC as timestamp) as ts2, cast(TEST_ORDER_STRING.TEST_DATE_ENC  as date) as dt2,TEST_ORDER.ORDER_ID, count(*) FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID LEFT JOIN TEST_ORDER_STRING on TEST_ORDER.ORDER_ID = TEST_ORDER_STRING.ORDER_ID group by TEST_ORDER.ORDER_ID ,TEST_ORDER_STRING.TEST_TIME_ENC , TEST_ORDER_STRING.TEST_DATE_ENC ,CAL_DT, TEST_ORDER.TEST_TIME_ENC order by TEST_ORDER.ORDER_ID ";

    @Test
    public void testDate() throws Exception {
        buildSegs("8c670664-8d05-466a-802f-83c023b56c77", 10001L);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Row> rows = ExecAndComp.queryModel(getProject(), sql).collectAsList();
        List<List<String>> calciteDf = transformToString(rows);
        List<List<String>> pushDown = SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), getProject()).getFirst();
        List<List<String>> jdbc = ExecAndComp.queryCubeWithJDBC(getProject(), sql);
        Assert.assertEquals(jdbc.size(), calciteDf.size());
        for (int i = 0; i < jdbc.size(); i++) {
            if (!ListUtils.isEqualList(calciteDf.get(i), pushDown.get(i))
                    && !ListUtils.isEqualList(calciteDf.get(i), jdbc.get(i))) {
                String expected = String.join(",", pushDown.get(i));
                String actual = String.join(",", jdbc.get(i));
                String actual2 = String.join(",", calciteDf.get(i));
                fail("expected: " + expected + ", actual: " + actual + ", actual2: " + actual2);
            }
        }
        System.out.println();

    }

    @Test
    public void testTimestampWithDynamicParam() throws Exception {
        String sqlOrign = "select TEST_ORDER.TEST_TIME_ENC as ts1, " + "CAL_DT as dt1, "
                + "cast (TEST_ORDER_STRING.TEST_TIME_ENC as timestamp) as ts2, "
                + "cast(TEST_ORDER_STRING.TEST_DATE_ENC  as date) as dt2," + "TEST_ORDER.ORDER_ID, " + "count(*) "
                + "FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT " + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "LEFT JOIN TEST_ORDER_STRING " + "ON TEST_ORDER.ORDER_ID = TEST_ORDER_STRING.ORDER_ID "
                + "where TEST_ORDER.TEST_TIME_ENC='2013-01-01 12:02:11' "
                + "group by TEST_ORDER.ORDER_ID ,TEST_ORDER_STRING.TEST_TIME_ENC , "
                + "TEST_ORDER_STRING.TEST_DATE_ENC ,CAL_DT, TEST_ORDER.TEST_TIME_ENC "
                + "order by TEST_ORDER.ORDER_ID ";
        String paramString = "2013-01-01 12:02:11";
        buildSegs("8c670664-8d05-466a-802f-83c023b56c77", 10001L);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        // benchmark
        List<List<String>> benchmark = ExecAndComp.queryCubeWithJDBC(getProject(), sqlOrign);
        // setTimestamp
        String sqlWithPlaceholder = sqlOrign.replace("where TEST_ORDER.TEST_TIME_ENC='2013-01-01 12:02:11' ",
                "where TEST_ORDER.TEST_TIME_ENC=? ");
        List<Row> rows = ExecAndComp.queryModel(getProject(), sqlWithPlaceholder,
                Arrays.asList(new Timestamp[]{Timestamp.valueOf(paramString)})).collectAsList();
        List<List<String>> setTimestampResults = transformToString(rows);
        // setTimestamp pushdown
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[]{
                new PrepareSqlStateParam(Timestamp.class.getCanonicalName(), paramString)};
        String sqlPushDown = PrepareSQLUtils.fillInParams(sqlWithPlaceholder, params);
        List<List<String>> setTimestampPushdownResults = SparkSqlClient
                .executeSql(ss, sqlPushDown, RandomUtil.randomUUID(), getProject()).getFirst();
        // setString
        List<Row> rows2 = ExecAndComp
                .queryModel(getProject(), sqlWithPlaceholder, Arrays.asList(new String[]{paramString}))
                .collectAsList();
        List<List<String>> setStringResults = transformToString(rows2);
        // setString pushdown
        PrepareSqlStateParam[] params2 = new PrepareSqlStateParam[]{
                new PrepareSqlStateParam(String.class.getCanonicalName(), paramString)};
        String sqlPushDown2 = PrepareSQLUtils.fillInParams(sqlWithPlaceholder, params2);
        List<List<String>> setStringPushdownResults = SparkSqlClient
                .executeSql(ss, sqlPushDown2, RandomUtil.randomUUID(), getProject()).getFirst();

        Assert.assertEquals(benchmark.size(), setTimestampResults.size());
        Assert.assertEquals(benchmark.size(), setTimestampPushdownResults.size());
        Assert.assertEquals(benchmark.size(), setStringResults.size());
        Assert.assertEquals(benchmark.size(), setStringPushdownResults.size());

        for (int i = 0; i < benchmark.size(); i++) {
            if (!ListUtils.isEqualList(benchmark.get(i), setTimestampResults.get(i))
                    && !ListUtils.isEqualList(benchmark.get(i), setTimestampPushdownResults.get(i))
                    && !ListUtils.isEqualList(benchmark.get(i), setStringResults.get(i))
                    && !ListUtils.isEqualList(benchmark.get(i), setStringPushdownResults.get(i))) {
                String expected = String.join(",", benchmark.get(i));
                String actual1 = String.join(",", setTimestampResults.get(i));
                String actual2 = String.join(",", setTimestampPushdownResults.get(i));
                String actual3 = String.join(",", setStringResults.get(i));
                String actual4 = String.join(",", setStringPushdownResults.get(i));
                fail("expected: " + expected + ", setTimestampResults: " + actual1 + ", setTimestampPushdownResults: "
                        + actual2 + ", setStringResults: " + actual3 + ", setStringPushdownResults: " + actual4);
            }
        }
    }

    @Test
    public void testConstantDate() throws Exception {
        String sql = "select date'2020-01-01', current_date";
        List<List<String>> pushDown = SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), getProject()).getFirst();
        List<List<String>> jdbc = ExecAndComp.queryCubeWithJDBC(getProject(), sql);
        for (int i = 0; i < jdbc.size(); i++) {
            Assert.assertEquals("Date literal doesn't match", pushDown.get(i), jdbc.get(i));
        }
    }

    @Test
    public void testConstantTimestamp() throws Exception {
        {
            String[] sqls = {"select current_timestamp", "select timestamp'2020-03-30 11:03:37'",
                    "select timestamp'2012-02-09 11:23:23.21'"};
            for (String sql : sqls) {
                // try matching timestamp to minutes mutilple times
                int max_try = 10;
                while (max_try-- > 0) {
                    List<List<String>> pushDown = SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), getProject())
                            .getFirst();
                    List<List<String>> jdbc = ExecAndComp.queryCubeWithJDBC(getProject(), sql);

                    // match timestamp to minute
                    String pushdownTS = pushDown.get(0).get(0).substring(0, 16);
                    String jdbcTS = jdbc.get(0).get(0).substring(0, 16);
                    if (pushdownTS.equals(jdbcTS)) {
                        return;
                    }

                    await().pollDelay(1000, TimeUnit.MILLISECONDS).until(() -> true);

                    if (max_try == 0) {
                        Assert.assertEquals("Current timestamp doesn't match", pushdownTS, jdbcTS);
                    }
                }
            }
        }
    }

    private List<List<String>> transformToString(List<Row> rows) {
        return rows.stream().map(row -> JavaConversions.seqAsJavaList(row.toSeq()).stream().map(r -> {
            if (r == null) {
                return null;
            } else {
                String s = r.toString();
                if (r instanceof Timestamp) {
                    return s.substring(0, s.length() - 2);
                } else {
                    return s;
                }
            }
        }).collect(Collectors.toList())).collect(Collectors.toList());
    }

    private void buildSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
    }
}
