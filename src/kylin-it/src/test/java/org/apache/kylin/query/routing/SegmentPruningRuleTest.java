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

package org.apache.kylin.query.routing;

import static org.awaitility.Awaitility.await;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.TypeSystem;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.util.OlapContextTestUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class SegmentPruningRuleTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC) {
            // for snappy
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");
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

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition_date_type");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/multi_partition_date_type" };
    }

    private List<NDataSegment> startRealizationPruner(NDataflowManager dataflowManager, String dataflowId, String sql,
            String project, KylinConfig kylinConfig) throws Exception {
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), sql);
        OlapContext context = olapContexts.get(0);
        CalciteSchema rootSchema = new QueryExec(project, kylinConfig).getRootSchema();
        SimpleDataContext dataContext = new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(),
                kylinConfig);
        context.getFirstTableScan().getCluster().getPlanner().setExecutor(new RexExecutorImpl(dataContext));
        Map<String, String> map = context.matchJoins(dataflow.getModel(), false, false);
        context.fixModel(dataflow.getModel(), map);
        return new SegmentPruningRule().pruneSegments(dataflow, context);
    }

    @After
    public void after() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testSegmentPruningPartitionDateColumnFilter() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e309";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= '2021-10-28' AND DATE_6 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningPartitionDateStrColumnFilter() throws Exception {
        val dataflowId = "00a91916-d31e-ed40-b1ba-4a86765072f6";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= '2021-12-20' AND STRING_DATE_20 < '2021-12-26'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS DATE) AND STRING_DATE_20 < '2021-12-26'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS DATE) AND STRING_DATE_20 < CAST('2021-12-26' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS TIMESTAMP) AND STRING_DATE_20 < CAST('2021-12-26' AS TIMESTAMP)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE CAST(STRING_DATE_20 AS TIMESTAMP)  >= '2021-12-20' AND CAST(STRING_DATE_20 AS TIMESTAMP) < '2021-12-26'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningPartitionDateStr2ColumnFilter() throws Exception {
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= '20211220' AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= '20211220' AND STRING_DATE2_24 < '20211224'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningDateType2Timestamp() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e310";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE DATE_6 >= '2021-10-28' AND DATE_6 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        DataType dateType = new DataType("date", 0, 0);
        ReflectionTestUtils.invokeMethod(SegmentPruningRule.class, "checkAndReformatDateType", "yyyy-MM-dd HH:mm:ss",
                Long.parseLong("1633928400000"), dateType);
    }

    @Test
    public void testSegmentPruningTimestampType2DateType() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e311";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= '2021-10-28' AND TIMESTAMP_10 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        DataType dateType = new DataType("date", 0, 0);
        ReflectionTestUtils.invokeMethod(SegmentPruningRule.class, "checkAndReformatDateType", "yyyy-MM-dd HH:mm:ss",
                Long.parseLong("1633928400000"), dateType);
    }

    @Test
    public void testCancelAndInterruptPruning() throws SqlParseException {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e312";
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("kylin.query.filter-condition-count", "99999");

        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE id = '121' AND (\n"
                + "(TIMESTAMP_10 >= '2021-11-03')\n" + "AND (TIMESTAMP_10 <= '2021-11-04')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-03')\n" + "AND (TIMESTAMP_10 <= '2021-11-05')\n" + ")\n" + "OR (\n" + "(\n"
                + "TIMESTAMP_10 >= '2021-11-03'\n" + ")\n" + "AND (TIMESTAMP_10 <= '2021-11-06')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-04')\n" + "AND (TIMESTAMP_10 <= '2021-11-04')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-04')\n" + "AND (TIMESTAMP_10 <= '2021-11-05')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-04')\n" + "AND (TIMESTAMP_10 <= '2021-11-06')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-07')\n" + "AND (TIMESTAMP_10 <= '2021-11-07')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-07')\n" + "AND (TIMESTAMP_10 <= '2021-11-08')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-07')\n" + "AND (TIMESTAMP_10 <= '2021-11-12')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-17')\n" + "AND (TIMESTAMP_10 <= '2021-11-17')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-17')\n" + "AND (TIMESTAMP_10 <= '2021-11-18')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-17')\n" + "AND (TIMESTAMP_10 <= '2021-11-19')\n" + ")\n" + "OR (\n"
                + "(TIMESTAMP_10 >= '2021-11-17')\n" + "AND (TIMESTAMP_10 <= '2021-11-17')\n" + ")";

        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), sql);
        OlapContext context = olapContexts.get(0);

        // append new segments
        Calendar calendar = Calendar.getInstance();
        calendar.set(2021, Calendar.DECEMBER, 6);

        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        final int newSegmentCount = 100_000;
        NDataSegment[] newSegments = new NDataSegment[newSegmentCount];
        for (int i = 0; i < newSegmentCount; i++) {
            long startTime = calendar.getTimeInMillis();
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            long endTime = calendar.getTimeInMillis();
            NDataSegment newSegment = new NDataSegment(dataflow,
                    new SegmentRange.TimePartitionedSegmentRange(startTime, endTime));
            newSegment.setStatus(SegmentStatusEnum.READY);
            newSegments[i] = newSegment;
        }

        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToAddSegs(newSegments);
        dataflowManager.updateDataflowWithoutIndex(update);

        dataflow = dataflowManager.getDataflow(dataflowId);

        CalciteSchema rootSchema = new QueryExec(project, kylinConfig).getRootSchema();
        SimpleDataContext dataContext = new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(),
                kylinConfig);
        context.getFirstTableScan().getCluster().getPlanner().setExecutor(new RexExecutorImpl(dataContext));
        Map<String, String> map = context.matchJoins(dataflow.getModel(), false, false);
        context.fixModel(dataflow.getModel(), map);

        Assert.assertTrue("Unexpected size " + dataflow.getQueryableSegments().size(),
                dataflow.getQueryableSegments().size() > newSegmentCount);

        testCancelQuery(dataflow, context);
        testCancelQuery(dataflow, context, queryEntry -> queryEntry.getPlannerCancelFlag().requestCancel());
        testCancelQuery(dataflow, context, queryEntry -> queryEntry.setStopByUser(true));
        testCancelAsyncQuery(dataflow, context);
        testInterrupt(dataflow, context);
        testTimeout(dataflow, context);
    }

    private void testCancelQuery(NDataflow dataflow, OlapContext context) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicReference<Segments<NDataSegment>> res = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                slowQueryDetector.set(new SlowQueryDetector(100, 10_000));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                res.set(new SegmentPruningRule().pruneSegments(dataflow, context));
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();
        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> queryEntry.get() != null);

        slowQueryDetector.get().stopQuery("pruning");

        Assert.assertFalse(queryEntry.get().isAsyncQuery());
        Assert.assertTrue(
                queryEntry.get().isStopByUser() && queryEntry.get().getPlannerCancelFlag().isCancelRequested());

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertNull(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(),
                exp.get() instanceof UserStopQueryException);
    }

    private void testCancelQuery(NDataflow dataflow, OlapContext context,
            Consumer<SlowQueryDetector.QueryEntry> updater) {

        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicReference<Segments<NDataSegment>> res = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                slowQueryDetector.set(new SlowQueryDetector(100, 10_000));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                res.set(new SegmentPruningRule().pruneSegments(dataflow, context));
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();
        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> queryEntry.get() != null);

        Assert.assertTrue(t.isAlive());

        updater.accept(queryEntry.get());

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertNull(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(),
                exp.get() instanceof UserStopQueryException);
        Assert.assertTrue(exp.get().getMessage().contains("inconsistent states"));
    }

    private void testCancelAsyncQuery(NDataflow dataflow, OlapContext context) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicReference<Segments<NDataSegment>> res = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
                slowQueryDetector.set(new SlowQueryDetector(100, 10_000));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                res.set(new SegmentPruningRule().pruneSegments(dataflow, context));
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> queryEntry.get() != null);

        String queryId = queryEntry.get().getQueryId();
        slowQueryDetector.get().stopQuery(queryId);

        Assert.assertTrue(queryEntry.get().isAsyncQuery());
        Assert.assertTrue(
                queryEntry.get().isStopByUser() && queryEntry.get().getPlannerCancelFlag().isCancelRequested());
        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertNull(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(),
                exp.get() instanceof UserStopQueryException);
    }

    private void testInterrupt(NDataflow dataflow, OlapContext context) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicReference<Segments<NDataSegment>> res = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                res.set(new SegmentPruningRule().pruneSegments(dataflow, context));
            } catch (Exception e) {
                exp.set(e);
            }
        });
        t.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(6, TimeUnit.SECONDS).until(t::isAlive);

        t.interrupt();

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertNull(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(), exp.get() instanceof KylinRuntimeException);
        Assert.assertTrue(exp.get().getCause() instanceof InterruptedException);
    }

    private void testTimeout(NDataflow dataflow, OlapContext context) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicReference<Segments<NDataSegment>> res = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                QueryContext.current().getQueryTagInfo().setAsyncQuery(false);
                slowQueryDetector.set(new SlowQueryDetector(10, 100));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                res.set(new SegmentPruningRule().pruneSegments(dataflow, context));
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS).until(() -> queryEntry.get() != null);

        Assert.assertTrue(queryEntry.get().setInterruptIfTimeout());

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        Assert.assertTrue(!t.isAlive() && queryEntry.get().getPlannerCancelFlag().isCancelRequested());

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertNull(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(), exp.get() instanceof KylinTimeoutException);
    }

    @Test
    public void testSegmentPruningTimestampType() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e312";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= '2021-10-28' AND TIMESTAMP_10 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        DataType dateType = new DataType("date", 0, 0);
        ReflectionTestUtils.invokeMethod(SegmentPruningRule.class, "checkAndReformatDateType", "yyyy-MM-dd HH:mm:ss",
                Long.parseLong("1633928400000"), dateType);
    }

    @Test
    public void testSegmentPruningMaxMeasureBeforeDaysSuccess() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        overwriteSystemProp("kylin.query.max-measure-segment-pruner-before-days", "0");
        sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(0, selectSegmentList.size());

        overwriteSystemProp("kylin.query.max-measure-segment-pruner-before-days", "2");
        sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), COUNT(*) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), COUNT(1) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), MIN(1) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT 1, MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT 1, MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY 1";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT STRING_DATE2_24, MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY STRING_DATE2_24";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT SUM(1), COUNT(DISTINCT 1), MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY STRING_DATE2_24";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningMaxMeasureBeforeDaysFail() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);

        // test non-max
        overwriteSystemProp("kylin.query.max-measure-segment-pruner-before-days", "2");
        String sql = "SELECT MIN(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test non time partition column
        sql = "SELECT MAX(STRING_DATE_20) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test multi aggregations
        sql = "SELECT MAX(STRING_DATE2_24), MIN(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test no aggregations
        sql = "SELECT STRING_DATE2_24 FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test multi group by column
        sql = "SELECT MAX(STRING_DATE2_24),STRING_DATE_20 FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY STRING_DATE_20";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test dataformat is null
        dataflowManager.getDataflow(dataflowId).getModel().getPartitionDesc().setPartitionDateFormat(null);
        sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(18, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningFilterAlwaysFalse() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);

        String sql = "SELECT * FROM (SELECT STRING_DATE2_24,STRING_DATE_20 FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 = 20211221) AS T1 WHERE STRING_DATE2_24 = 20211220";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(0, selectSegmentList.size());
    }

    @Test
    public void testPruningByDimRange() throws Exception {
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        val dataflowId = "1e12e297-ae63-4019-5e05-f571174ea157";
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);

        // happy pass
        String sql = "SELECT ID2 from TEST_DB.TEST_MEASURE WHERE ID2 = 332342343";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        overwriteSystemProp("kylin.query.dimension-range-filter-enabled", "true");
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        // because dimension_range_info_map is empty, its segments will not be pruned
        Assert.assertEquals(2, selectSegmentList.size());

        // test filter column is empty
        sql = "SELECT * from TEST_DB.TEST_MEASURE";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        // test min max is null, the condition {FLAG = 'TRUE'} will be ignored
        // Since the min and max of the FLAG column in the specified segment are null
        sql = "SELECT * from TEST_DB.TEST_MEASURE WHERE TIME1 = '2012-03-31' AND FLAG = 'TRUE'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(1, selectSegmentList.size());

        // test time partition filter is always false
        sql = "SELECT * from TEST_DB.TEST_MEASURE WHERE time1 > '2014-04-04'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(0, selectSegmentList.size());

        sql = "SELECT * from TEST_DB.TEST_MEASURE WHERE ID1 = 10000000172 AND ID2 = 332342344 AND ID3 = 1243 AND "
                + "ID4 = 19 AND PRICE1 = 31.336 AND PRICE2 = 123424.246 AND PRICE3 = 1436.24222343 AND PRICE5 = 7 AND "
                + "PRICE6 = 13 AND PRICE7 = 5 AND NAME1 = 'China' AND NAME2 = '中国深圳' AND NAME3 = '中国北京' AND "
                + "NAME4 = 14 AND TIME1 = '2014-04-03' AND TIME2 = '2012-04-02' AND FLAG = FALSE";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(0, selectSegmentList.size());
    }

    @Test
    public void testPruningWithDifferentConjunctions() throws Exception {
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        val dataflowId = "1e12e297-ae63-4019-5e05-f571174ea157";
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);

        // < MaxFilterConditionCnt
        overwriteSystemProp("kylin.query.filter-condition-count", "15");
        String sql = "SELECT NAME1 from TEST_DB.TEST_MEASURE where NAME1 in ('1137', '1153', '1173')";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        // > MaxFilterConditionCnt
        overwriteSystemProp("kylin.query.filter-condition-count", "5");
        sql = "SELECT NAME1 from TEST_DB.TEST_MEASURE where NAME1 in ('1137', '1153', '1173', '999999902') and NAME2 in ('Anhui', 'Jiangsu', 'Shanghai')";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
    }

    @Override
    public String getProject() {
        return "multi_partition_date_type";
    }

}
