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
package org.apache.kylin.streaming.app;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.common.StreamingTestConstant;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.metadata.cube.model.NCubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.CreateStreamingFlatTable;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.jobs.GracefulStopInterface;
import org.apache.kylin.streaming.jobs.StreamingJobUtils;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.AwaitUtils;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.apache.spark.sql.execution.streaming.OneTimeTrigger$;
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

@Slf4j
public class StreamingEntryTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBuild() {
        val config = KylinConfig.getInstanceFromEnv();

        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dfMgr = NDataflowManager.getInstance(config, PROJECT);
        var df = dfMgr.getDataflow(DATAFLOW_ID);
        // cleanup all segments first
        var update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        df = dfMgr.getDataflow(df.getId());
        val seg1 = dfMgr.appendSegmentForStreaming(df, createSegmentRange());
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan());
        val layouts = StreamingUtils.getToBuildLayouts(df);
        Assert.assertNotNull(layouts);
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "", "xx" };
        val entry = new StreamingEntry();
        entry.parseParams(args);
        val nSpanningTree = entry.createSpanningTree(df);
        Assert.assertNotNull(nSpanningTree);

        val ss = createSparkSession();
        val flatTable = CreateStreamingFlatTable.apply(flatTableDesc, null, nSpanningTree, ss, null, null, null);

        val ds = flatTable.generateStreamingDataset(config);
        Assert.assertEquals(1, ds.count());
        var model = flatTableDesc.getDataModel();
        var tableDesc = model.getRootFactTable().getTableDesc();
        var kafkaParam = tableDesc.getKafkaConfig().getKafkaParam();
        Assert.assertEquals("earliest", kafkaParam.get("startingOffsets"));

        val jobParams = new HashMap<String, String>();
        jobParams.put(StreamingConstants.STREAMING_KAFKA_STARTING_OFFSETS, "latest");
        val newConfig = StreamingJobUtils.getStreamingKylinConfig(config, jobParams, model.getId(), PROJECT);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        flatTable.generateStreamingDataset(newConfig);
        model = flatTableDesc.getDataModel();
        tableDesc = model.getRootFactTable().getTableDesc();
        kafkaParam = tableDesc.getKafkaConfig().getKafkaParam();
        Assert.assertEquals("latest", kafkaParam.get("startingOffsets"));
        ss.stop();
        Assert.assertEquals("LO_PARTITIONCOLUMN", flatTable.partitionColumn());
    }

    @Test
    public void testMain() {
        val config = getTestConfig();
        clearCheckpoint(DATAFLOW_ID);
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(true);
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "", "" };
        thrown.expect(Exception.class);
        StreamingEntry.main(args);
    }

    @Test
    public void testInitBuildEntry_EmptyCheckPoint() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-checkpoint-location", "");
        thrown.expectMessage("base checkpoint location must be configured,");
        new StreamingEntry();
    }

    @Test
    public void testInitBuildEntry_TriggerOnce() {
        {
            val config = getTestConfig();
            config.setProperty("kylin.engine.streaming-trigger-once", KylinConfigBase.TRUE);
            val entry = new StreamingEntry();
            val trigger = entry.trigger();
            Assert.assertNotNull(trigger);
            Assert.assertTrue(trigger instanceof OneTimeTrigger$);
        }

        {
            val config = getTestConfig();
            config.setProperty("kylin.engine.streaming-trigger-once", KylinConfigBase.FALSE);
            val entry = new StreamingEntry();
            val trigger = entry.trigger();
            Assert.assertNotNull(trigger);
            Assert.assertTrue(trigger instanceof ProcessingTimeTrigger);
        }
    }

    @Test
    public void testInitBuildEntry_DataFlow() {
        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);
        val dataflow = entry.dataflow();
        Assert.assertNotNull(dataflow);
    }

    @Test
    public void testExecute() {
        val config = getTestConfig();
        clearCheckpoint(DATAFLOW_ID);
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(true);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);
        entry.setSparkSession(createSparkSession());
        StreamingEntry.entry_$eq(entry);
        Assert.assertNotNull(StreamingEntry.entry());
        Mockito.when(entry.createRestSupport(config)).thenReturn(new RestSupport(config) {
            public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
                val jobId = DATAFLOW_ID + "_build";
                mgr.updateStreamingJob(jobId, copyForWrite -> {
                    copyForWrite.setJobExecutionId(1);
                });
                return RestResponse.ok("1");
            }
        });
        AwaitUtils.await(() -> {
        }, 5000, () -> {
            val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
            val jobId = DATAFLOW_ID + "_build";
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                copyForWrite.setAction(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN);
            });
        });
        try {
            entry.doExecute();
            Assert.assertTrue(entry.getSparkSession().sparkContext().isStopped());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        clearCheckpoint(DATAFLOW_ID);
    }

    @Test
    public void testExecute_IllegalProject() {
        val illegalProject = "xxxx";
        val args = new String[] { illegalProject, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);

        thrown.expectMessage("metastore can not find this project " + illegalProject);
        entry.doExecute();
    }

    @Test
    public void testExecute_EmptyFlatTableDataSet() {
        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);
        Mockito.doNothing().when(entry).registerStreamListener();
        Mockito.doReturn(new Tuple3<>(null, null, null)).when(entry).generateStreamQueryForOneModel();

        thrown.expectMessage(String.format(Locale.ROOT,
                "generate query for one model failed for project:  %s dataflowId: %s", PROJECT, DATAFLOW_ID));
        entry.doExecute();
    }

    @Test
    public void testExecute_EmptyTimeColumn() {
        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);
        Mockito.doNothing().when(entry).registerStreamListener();

        val sparkSession = createSparkSession();
        Mockito.doReturn(new Tuple3<>(sparkSession.range(1), null, null)).when(entry).generateStreamQueryForOneModel();

        thrown.expectMessage(String.format(Locale.ROOT,
                "streaming query must have time partition column for project:  %s dataflowId: %s", PROJECT,
                DATAFLOW_ID));
        entry.doExecute();
    }

    @Test
    public void testDimensionTableRefresh() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });
        entry.setSparkSession(createSparkSession());
        val tuple4 = entry.generateStreamQueryForOneModel();

        ReflectionUtils.setField(entry, "rateTriggerDuration", 1000L);
        Assert.assertEquals(1000L, ReflectionUtils.getField(entry, "rateTriggerDuration"));

        val flatTable = tuple4._3();
        flatTable.tableRefreshInterval_$eq(5L);
        Assert.assertEquals(dataflowId, flatTable.model().getId());
        try {
            entry.startTableRefreshThread(flatTable);
            AwaitUtils.await(() -> {
            }, 10000, () -> {
                entry.refreshTable(flatTable);
                Assert.assertEquals(0L, entry.tableRefreshAcc().get());
                StreamingEntry.stop();
                entry.ss.stop();
            });
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDimensionTableRefresh_Skip() {
        {
            val fakeStreamingTable = Mockito
                    .spy(new CreateStreamingFlatTable(null, null, null, null, null, null, null));
            val entry = Mockito.spy(new StreamingEntry());
            entry.refreshTable(fakeStreamingTable);
            Mockito.doReturn(false).when(fakeStreamingTable).shouldRefreshTable();
            Assert.assertFalse(fakeStreamingTable.shouldRefreshTable());
            entry.refreshTable(fakeStreamingTable);
        }

        {
            val fakeStreamingTable = Mockito
                    .spy(new CreateStreamingFlatTable(null, null, null, null, null, null, null));
            val entry = Mockito.spy(new StreamingEntry());
            entry.refreshTable(fakeStreamingTable);
            Mockito.doReturn(true).when(fakeStreamingTable).shouldRefreshTable();
            Mockito.doReturn(100L).when(fakeStreamingTable).tableRefreshInterval();
            Assert.assertTrue(fakeStreamingTable.shouldRefreshTable());
            Assert.assertTrue(entry.tableRefreshAcc().get() < fakeStreamingTable.tableRefreshInterval());
            entry.refreshTable(fakeStreamingTable);
        }
    }

    @Test
    public void testDimensionTableRefresh_SkipRefreshThread() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });
        entry.setSparkSession(createSparkSession());
        val tuple4 = entry.generateStreamQueryForOneModel();

        ReflectionUtils.setField(entry, "rateTriggerDuration", 1000L);
        Assert.assertEquals(1000L, ReflectionUtils.getField(entry, "rateTriggerDuration"));
        val flatTable = tuple4._3();
        flatTable.tableRefreshInterval_$eq(-1L);
        entry.startTableRefreshThread(flatTable);

        Assert.assertFalse(flatTable.shouldRefreshTable());
    }

    @Test
    public void testDimensionTableRefresh_Running() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });
        entry.setSparkSession(createSparkSession());
        val tuple4 = entry.generateStreamQueryForOneModel();

        ReflectionUtils.setField(entry, "rateTriggerDuration", 100L);
        Assert.assertEquals(100L, ReflectionUtils.getField(entry, "rateTriggerDuration"));
        val flatTable = tuple4._3();
        flatTable.tableRefreshInterval_$eq(1L);
        Assert.assertTrue(flatTable.shouldRefreshTable());
        entry.startTableRefreshThread(flatTable);

        Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> entry.tableRefreshAcc().get() > 0);
    }

    @Test
    public void testDimensionTableRefresh_NotRunning() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });
        entry.setSparkSession(createSparkSession());
        val tuple4 = entry.generateStreamQueryForOneModel();

        val refreshTableInterval = 100L;
        ReflectionUtils.setField(entry, "rateTriggerDuration", refreshTableInterval);
        Assert.assertEquals(refreshTableInterval, ReflectionUtils.getField(entry, "rateTriggerDuration"));
        val flatTable = tuple4._3();
        flatTable.tableRefreshInterval_$eq(1L);
        Assert.assertTrue(flatTable.shouldRefreshTable());
        entry.setStopFlag(true);
        entry.startTableRefreshThread(flatTable);

        AwaitUtils.sleep(Long.valueOf(10 * refreshTableInterval).intValue());
        Assert.assertEquals(0, entry.tableRefreshAcc().get());
    }

    @Test
    public void testPrepareKylinConfig() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });

        ReflectionUtils.invokeGetterMethod(entry, "prepareKylinConfig");
        Assert.assertEquals("xx", config.getMetadataUrl().toString());
    }

    @Test
    public void testInitMetaPathSet() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });

        val metaPathSet = (Set<String>) ReflectionUtils.invokeGetterMethod(entry, "initMetaPathSet");

        Assert.assertEquals(11, metaPathSet.size());
        Assert.assertTrue(metaPathSet.contains("/streaming_test/streaming/511a9163-7888-4a60-aa24-ae735937cc87_build"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/dataflow/511a9163-7888-4a60-aa24-ae735937cc87.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/index_plan/511a9163-7888-4a60-aa24-ae735937cc87.json"));
        Assert.assertTrue(metaPathSet.contains("/_global/project/streaming_test.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/model_desc/511a9163-7888-4a60-aa24-ae735937cc87.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/table/SSB.P_LINEORDER.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/kafka/SSB.P_LINEORDER.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/table/SSB.DATES.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/table/SSB.CUSTOMER.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/table/SSB.SUPPLIER.json"));
        Assert.assertTrue(metaPathSet.contains("/streaming_test/table/SSB.PART.json"));
    }

    @Test
    public void testPrepareBeforeExecute_Local() throws ExecuteException {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });

        val sparkConf = KylinBuildEnv.getOrCreate(getTestConfig()).sparkConf();
        Mockito.doNothing().when(entry).getOrCreateSparkSession(sparkConf);
        Mockito.doReturn(123).when(entry).reportApplicationInfo();
        entry.prepareBeforeExecute();
    }

    @Test
    public void testPrepareBeforeExecute_JobOnCluster() throws ExecuteException {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(new String[] { PROJECT, dataflowId, "5", "", "xx" });

        val sparkConf = KylinBuildEnv.getOrCreate(getTestConfig()).sparkConf();
        Mockito.doNothing().when(entry).getOrCreateSparkSession(sparkConf);
        Mockito.doReturn(true).when(entry).isJobOnCluster();
        Mockito.doReturn(123).when(entry).reportApplicationInfo();
        entry.prepareBeforeExecute();
    }

    private void clearCheckpoint(String dataflowId) {
        val config = getTestConfig();
        val checkpointFile = new File(config.getStreamingBaseCheckpointLocation() + "/" + dataflowId);
        var result = false;
        int retry = 0;
        while (!result && retry < 5 && checkpointFile.exists()) {
            result = checkpointFile.delete();
            StreamingUtils.sleep(5000);
            retry++;
        }
    }

    @Test
    public void testIsRunning() {
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        val stopFlag = new AtomicBoolean(true);
        ReflectionUtils.setField(entry, "gracefulStop", stopFlag);
        Assert.assertFalse(entry.isRunning());
        stopFlag.set(false);
        Assert.assertTrue(entry.isRunning());
        entry.getSparkSession().close();
        Assert.assertFalse(entry.isRunning());
    }

    @Test
    public void testStartJobExecutionIdCheckThread() {
        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);
        entry.setSparkSession(createSparkSession());
        val config = getTestConfig();
        config.setProperty("kylin.streaming.job-execution-id-check-interval", "0m");
        val counter = new AtomicInteger(0);
        Mockito.when(entry.createRestSupport(config)).thenReturn(new RestSupport(config) {
            public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                if (counter.getAndIncrement() < 3) {
                    return RestResponse.ok(0);
                } else {
                    return RestResponse.ok(1);
                }
            }
        });
        val stopFlag = new AtomicBoolean(false);
        ReflectionUtils.setField(entry, "gracefulStop", stopFlag);
        ReflectionUtils.setField(entry, "jobExecId", 0);
        AwaitUtils.await(entry::startJobExecutionIdCheckThread, 5000, () -> {
            stopFlag.set(true);
        });
        Assert.assertTrue(entry.getSparkSession().sparkContext().isStopped());
    }

    @Test
    public void testStartJobExecutionIdCheckThread_DiffJobExecId() {
        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "", "xx" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.parseParams(args);
        entry.setSparkSession(createSparkSession());
        val config = getTestConfig();
        config.setProperty("kylin.streaming.job-execution-id-check-interval", "0m");
        val counter = new AtomicInteger(0);
        Mockito.when(entry.createRestSupport(config)).thenReturn(new RestSupport(config) {
            public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                if (counter.getAndIncrement() < 3) {
                    return RestResponse.ok(0);
                } else {
                    return RestResponse.ok(1);
                }
            }
        });
        val stopFlag = new AtomicBoolean(false);
        ReflectionUtils.setField(entry, "gracefulStop", stopFlag);
        ReflectionUtils.setField(entry, "jobExecId", 3);
        AwaitUtils.await(entry::startJobExecutionIdCheckThread, 1000, () -> {
            stopFlag.set(true);
        });
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> entry.getSparkSession().sparkContext().isStopped());
    }

    @Test
    public void testGracefulStopInterface() {
        GracefulStopInterface gracefulStop = new StreamingEntry();
        gracefulStop.setStopFlag(true);
        Assert.assertTrue(gracefulStop.getStopFlag());

        gracefulStop.setStopFlag(false);
        Assert.assertFalse(gracefulStop.getStopFlag());
    }
}
