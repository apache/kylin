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
package org.apache.kylin.streaming;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataSegmentManager;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.streaming.app.StreamingMergeEntry;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.jobs.GracefulStopInterface;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingMergeEntryTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    /**
     * test l0 merge
     */
    @Test
    public void testMergeSegmentLayer0() {
        Callback callback = () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val config = getTestConfig();
                config.setProperty("kylin.engine.spark.cluster-manager-class-name",
                        "org.apache.kylin.streaming.util.MockClusterManager");
                StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());
                streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
                streamingMergeEntry.setNumberOfSeg(10);
                streamingMergeEntry.setSparkSession(createSparkSession());
                val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 11);
                df = setSegmentStorageSize(mgr, df, 1024);

                mockRestSupport(streamingMergeEntry, config, 0);
                streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
                streamingMergeEntry.getSparkSession().close();
                df = mgr.getDataflow(DATAFLOW_ID);
                Assert.assertEquals(2, df.getSegments().size());
                Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
                Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
                return null;
            }, PROJECT);
        };
        testWithRetry(callback);
    }

    /**
     * test normal merge: L0 merge & L1 merge
     */
    @Test
    public void testMergeSegment() {
        Callback callback = () -> {
            val config = getTestConfig();
            config.setProperty("kylin.engine.spark.cluster-manager-class-name",
                    "org.apache.kylin.streaming.util.MockClusterManager");
            StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());
            streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
            streamingMergeEntry.setNumberOfSeg(3);
            streamingMergeEntry.setSparkSession(createSparkSession());
            UnitOfWork.doInTransactionWithRetry(() -> {
                val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 10);
                setSegmentStorageSize(mgr, df, 1024);
                return true;
            }, PROJECT);

            mockRestSupport(streamingMergeEntry, config, 0);
            for (int i = 0; i < 4; i++) {
                streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
            }
            val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
            streamingMergeEntry.getSparkSession().close();
            NDataflow df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(2, df.getSegments().size());
            Assert.assertEquals("2", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
        };
        testWithRetry(callback);
    }

    /**
     * test no merge for L1 layer
     */
    @Test
    public void testMergeSegmentLayer1() {
        val config = getTestConfig();
        Callback callback = () -> {
            StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());
            streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
            streamingMergeEntry.setNumberOfSeg(3);
            streamingMergeEntry.setSparkSession(createSparkSession());
            val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
            NDataflow df = mgr.getDataflow(DATAFLOW_ID);
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            mgr.updateDataflow(update);
            df = mgr.getDataflow(DATAFLOW_ID);
            df = createSegments(mgr, df, 10, 1);
            df = setSegmentStorageSize(mgr, df, 1024);

            mockRestSupport(streamingMergeEntry, config, 0);
            streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
            streamingMergeEntry.getSparkSession().close();
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(10, df.getSegments().size());
            df.getSegments().stream()
                    .forEach(item -> Assert.assertEquals("1", item.getAdditionalInfo().get("file_layer")));
        };
        testWithRetry(callback);
    }

    @Test
    public void testMergeSegmentOfCatchup1() {
        val config = getTestConfig();

        StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());
        streamingMergeEntry.setThresholdOfSegSize(20 * 1024);
        streamingMergeEntry.setNumberOfSeg(3);
        streamingMergeEntry.setSparkSession(createSparkSession());

        Callback callback = () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 16);
                setSegmentStorageSize(mgr, df, 1024);
                mockRestSupport(streamingMergeEntry, config, 0);
                return null;
            }, PROJECT);

            val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
            NDataflow df;
            streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
            streamingMergeEntry.getSparkSession().close();
            df = mgr.getDataflow(DATAFLOW_ID);
            Assert.assertEquals(2, df.getSegments().size());
            Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
            Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
        };
        testWithRetry(callback);
    }

    @Test
    public void testMergeSegmentOfCatchup2() {
        Callback callback = () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val config = getTestConfig();
                config.setProperty("kylin.engine.streaming-segment-merge-ratio", "1");
                StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());

                streamingMergeEntry.setThresholdOfSegSize(14 * 1024);
                streamingMergeEntry.setNumberOfSeg(3);
                streamingMergeEntry.setSparkSession(createSparkSession());
                val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 16);
                Assert.assertEquals(16, df.getSegments().size());
                setSegmentStorageSize(mgr, df, 1024);

                mockRestSupport(streamingMergeEntry, config, 0);
                streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
                streamingMergeEntry.getSparkSession().close();
                df = mgr.getDataflow(DATAFLOW_ID);
                Assert.assertEquals(3, df.getSegments().size());
                Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
                Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
                Assert.assertTrue(df.getSegments().get(2).getAdditionalInfo().isEmpty());
                return null;
            }, PROJECT);
        };
        testWithRetry(callback);
    }

    @Test
    public void testMergeSegmentOfCatchup3() {
        Callback callback = () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val config = getTestConfig();
                config.setProperty("kylin.engine.streaming-segment-merge-ratio", "1");
                StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());
                streamingMergeEntry.setThresholdOfSegSize(30 * 1024);
                streamingMergeEntry.setNumberOfSeg(3);
                streamingMergeEntry.setSparkSession(createSparkSession());
                val mgr = NDataflowManager.getInstance(config, PROJECT);
                NDataSegmentManager segManager = config.getManager(PROJECT, NDataSegmentManager.class);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 21, null, copyForWrite -> {
                    for (int i = 0; i < 2; i++) {
                        val segId = copyForWrite.getSegments().get(i).getUuid();
                        segManager.update(segId, segment -> segment.getAdditionalInfo().put("file_layer", "2"));
                    }
                    for (int i = 2; i < 5; i++) {
                        val segId = copyForWrite.getSegments().get(i).getUuid();
                        segManager.update(segId, segment -> segment.getAdditionalInfo().put("file_layer", "1"));
                    }
                });
                setSegmentStorageSize(mgr, df, 1024);

                mockRestSupport(streamingMergeEntry, config, 0);
                streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
                streamingMergeEntry.getSparkSession().close();
                df = mgr.getDataflow(DATAFLOW_ID);
                Assert.assertEquals(2, df.getSegments().size());
                Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
                Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
                return null;
            }, PROJECT);
        };
        testWithRetry(callback);
    }

    @Test
    public void testMergeSegmentOfCatchup4() {

        Callback callback = () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val config = getTestConfig();
                config.setProperty("kylin.engine.streaming-segment-merge-ratio", "1");
                StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());

                streamingMergeEntry.setThresholdOfSegSize(16 * 1024);
                streamingMergeEntry.setNumberOfSeg(3);
                streamingMergeEntry.setSparkSession(createSparkSession());
                val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
                NDataSegmentManager segManager = config.getManager(PROJECT, NDataSegmentManager.class);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 19, null, copyForWrite -> {
                    for (int i = 0; i < 1; i++) {
                        val segId = copyForWrite.getSegments().get(i).getUuid();
                        segManager.update(segId, segment -> segment.getAdditionalInfo().put("file_layer", "2"));
                    }
                    for (int i = 1; i < 2; i++) {
                        val segId = copyForWrite.getSegments().get(i).getUuid();
                        segManager.update(segId, segment -> segment.getAdditionalInfo().put("file_layer", "1"));
                    }
                });
                setSegmentStorageSize(mgr, df, 1024);

                mockRestSupport(streamingMergeEntry, config, 0);
                streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
                streamingMergeEntry.getSparkSession().close();
                df = mgr.getDataflow(DATAFLOW_ID);
                Assert.assertEquals(4, df.getSegments().size());
                Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
                Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
                Assert.assertTrue(df.getSegments().get(2).getAdditionalInfo().isEmpty());
                Assert.assertTrue(df.getSegments().get(3).getAdditionalInfo().isEmpty());
                return null;
            }, PROJECT);

        };
        testWithRetry(callback);
    }

    @Test
    public void testMergeSegmentOfPeak1() {
        val config = getTestConfig();
        Callback callback = () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                StreamingMergeEntry streamingMergeEntry = Mockito.spy(new StreamingMergeEntry());

                streamingMergeEntry.setThresholdOfSegSize(5 * 1024);
                streamingMergeEntry.setNumberOfSeg(5);
                streamingMergeEntry.setSparkSession(createSparkSession());
                Assert.assertNotNull(streamingMergeEntry.getSparkSession());
                val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
                NDataSegmentManager segManager = config.getManager(PROJECT, NDataSegmentManager.class);
                NDataflow df = mgr.getDataflow(DATAFLOW_ID);
                NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
                update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                mgr.updateDataflow(update);
                df = mgr.getDataflow(DATAFLOW_ID);
                df = createSegments(mgr, df, 6, null, copyForWrite -> {
                    for (int i = 0; i < 2; i++) {
                        val segId = copyForWrite.getSegments().get(i).getUuid();
                        segManager.update(segId, segment -> segment.getAdditionalInfo().put("file_layer", "2"));
                    }
                    for (int i = 2; i < 4; i++) {
                        val segId = copyForWrite.getSegments().get(i).getUuid();
                        segManager.update(segId, segment -> segment.getAdditionalInfo().put("file_layer", "1"));
                    }
                    for (int i = 4; i < 6; i++) {
                        val seg = copyForWrite.getSegments().get(i);
                    }
                });
                for (int i = 0; i < 4; i++) {
                    val seg = df.getSegments().get(i);
                    setSegmentStorageSize(seg, 2048L);
                }
                for (int i = 4; i < 6; i++) {
                    val seg = df.getSegments().get(i);
                    setSegmentStorageSize(seg, 5 * 1024L);
                }
                mgr.getDataflow(df.getId());
                mockRestSupport(streamingMergeEntry, config, 0);
                streamingMergeEntry.process(PROJECT, DATAFLOW_ID);
                streamingMergeEntry.getSparkSession().stop();

                df = mgr.getDataflow(DATAFLOW_ID);
                Assert.assertEquals(2, df.getSegments().size());
                Assert.assertEquals("1", df.getSegments().get(0).getAdditionalInfo().get("file_layer"));
                Assert.assertTrue(df.getSegments().get(1).getAdditionalInfo().isEmpty());
                return null;
            }, PROJECT);
        };
        testWithRetry(callback);
    }

    @Test
    public void testRemoveLastL0Segment_EmptySegment() {
        val entry = Mockito.spy(new StreamingMergeEntry());
        val segments = new Segments<NDataSegment>();
        ReflectionTestUtils.invokeMethod(entry, "removeLastL0Segment", segments);
        Assert.assertTrue(segments.isEmpty());
    }

    @Test
    public void testRemoveLastL0Segment_AddInfo_Null() {
        val entry = Mockito.spy(new StreamingMergeEntry());

        val segments = new Segments<NDataSegment>();
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        var dataflow = mgr.getDataflow(DATAFLOW_ID);
        for (int i = 0; i < 3; i++) {
            val start = LocalDate.parse("2000-01-01").plusMonths(i);
            val end = start.plusMonths(1);
            val seg = new NDataSegment();
            val segRange = new SegmentRange.TimePartitionedSegmentRange(start.toString(), end.toString());
            seg.setId(RandomUtil.randomUUIDStr());
            seg.setName(Segments.makeSegmentName(segRange));
            seg.setCreateTimeUTC(System.currentTimeMillis());
            seg.setSegmentRange(segRange);
            seg.setStatus(SegmentStatusEnum.READY);
            seg.setAdditionalInfo(null);
            seg.setDataflow(dataflow);
            segments.add(seg);
        }

        ReflectionTestUtils.invokeMethod(entry, "removeLastL0Segment", segments);
        Assert.assertEquals(3, segments.size());
    }

    @Test
    public void testRemoveLastL0Segment_FileLayer_Null() {
        val entry = Mockito.spy(new StreamingMergeEntry());

        val segments = new Segments<NDataSegment>();
        val addInfo = new HashMap<String, String>();
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        var dataflow = mgr.getDataflow(DATAFLOW_ID);
        addInfo.put("abc", "2");
        for (int i = 0; i < 3; i++) {
            val start = LocalDate.parse("2000-01-01").plusMonths(i);
            val end = start.plusMonths(1);
            val seg = new NDataSegment();
            val segRange = new SegmentRange.TimePartitionedSegmentRange(start.toString(), end.toString());
            seg.setId(RandomUtil.randomUUIDStr());
            seg.setName(Segments.makeSegmentName(segRange));
            seg.setCreateTimeUTC(System.currentTimeMillis());
            seg.setSegmentRange(segRange);
            seg.setStatus(SegmentStatusEnum.READY);
            seg.setAdditionalInfo(addInfo);
            seg.setDataflow(dataflow);
            segments.add(seg);
        }

        ReflectionTestUtils.invokeMethod(entry, "removeLastL0Segment", segments);
        Assert.assertEquals(2, segments.size());
    }

    @Test
    public void testRemoveLastL0Segment_FileLayer_NotNull() {
        val entry = Mockito.spy(new StreamingMergeEntry());

        val segments = new Segments<NDataSegment>();
        val addInfo = new HashMap<String, String>();
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        var dataflow = mgr.getDataflow(DATAFLOW_ID);
        addInfo.put(StreamingConstants.FILE_LAYER, "2");
        for (int i = 0; i < 3; i++) {
            val start = LocalDate.parse("2000-01-01").plusMonths(i);
            val end = start.plusMonths(1);
            val seg = new NDataSegment();
            val segRange = new SegmentRange.TimePartitionedSegmentRange(start.toString(), end.toString());
            seg.setId(RandomUtil.randomUUIDStr());
            seg.setName(Segments.makeSegmentName(segRange));
            seg.setCreateTimeUTC(System.currentTimeMillis());
            seg.setSegmentRange(segRange);
            seg.setStatus(SegmentStatusEnum.READY);
            seg.setAdditionalInfo(addInfo);
            seg.setDataflow(dataflow);
            segments.add(seg);
        }

        ReflectionTestUtils.invokeMethod(entry, "removeLastL0Segment", segments);
        Assert.assertEquals(3, segments.size());
    }

    @Test
    public void testScheduleException() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "1");
        val args = new String[] { PROJECT, DATAFLOW_ID + "-err", "5k", "5", "xx" };
        try {
            createSparkSession();
            val entry = new StreamingMergeEntry() {
                public RestSupport createRestSupport(KylinConfig config) {
                    return new RestSupport(config) {
                        public RestResponse<String> execute(HttpRequestBase httpReqBase, Object param) {
                            return RestResponse.ok("001");
                        }
                    };
                }
            };
            entry.execute(args);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ExecuteException);
        }
    }

    @Test
    public void testGetSegment() {
        val entry = Mockito.spy(new StreamingMergeEntry());
        val mergedSeg = "c380dd2a-43b8-4268-b73d-2a5f76236633";
        val config = getTestConfig();
        val mgr = NDataflowManager.getInstance(config, PROJECT);
        NDataSegmentManager segManager = config.getManager(PROJECT, NDataSegmentManager.class);

        var dataflow = mgr.getDataflow(DATAFLOW_ID);
        mgr.updateDataflow(dataflow.getId(), copyForWrite -> {
            val segId = copyForWrite.getSegment(mergedSeg).getUuid();
            segManager.update(segId, segment -> segment.setStatus(SegmentStatusEnum.WARNING));
        });
        dataflow = mgr.getDataflow(DATAFLOW_ID);
        val warningSeg = entry.getSegment(dataflow.getSegments(), dataflow.getSegment(mergedSeg), PROJECT, DATAFLOW_ID);
        Assert.assertEquals(SegmentStatusEnum.WARNING, warningSeg.getStatus());

        val segId = dataflow.getSegment(mergedSeg).getUuid();
        segManager.update(segId, segment -> segment.setStatus(SegmentStatusEnum.NEW));
        dataflow = mgr.getDataflow(DATAFLOW_ID);
        val newSeg = entry.getSegment(dataflow.getSegments(), dataflow.getSegment(mergedSeg), PROJECT, DATAFLOW_ID);
        Assert.assertEquals(SegmentStatusEnum.NEW, newSeg.getStatus());

        thrown.expect(KylinException.class);
        val empSeg = NDataSegment.empty();
        entry.getSegment(dataflow.getSegments(), empSeg, PROJECT, DATAFLOW_ID);
    }

    @Test
    public void testRemoveSegment() {
        val entry = Mockito.spy(new StreamingMergeEntry());
        val config = getTestConfig();
        mockRestSupport(entry, config, "new-seg-123456");
        val seg = NDataSegment.empty();
        entry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "32m", "3", "xx" });
        entry.removeSegment(PROJECT, DATAFLOW_ID, seg);
        val dataflow = NDataflowManager.getInstance(config, PROJECT).getDataflow(DATAFLOW_ID);
        Assert.assertNull(dataflow.getSegment(seg.getId()));
    }

    @Test
    public void testMergeSegmentsException() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "0s");
        val entry = Mockito.spy(new StreamingMergeEntry() {
            public NDataSegment allocateSegment(String project, String dataflowId, List<NDataSegment> retainSegments,
                    int currLayer) {
                throw new KylinException(ServerErrorCode.SEGMENT_MERGE_FAILURE, "merge Exception");
            }
        });
        val retainSegments = Arrays.asList(new NDataSegment());
        thrown.expect(KylinException.class);
        entry.mergeSegments(PROJECT, DATAFLOW_ID, retainSegments, 1);
    }

    @Test
    public void testMergeSegmentsDoExecute_ManualGracefulShutDown() throws ExecuteException {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "0s");
        val entry = Mockito.spy(new StreamingMergeEntry());
        entry.setSparkSession(createSparkSession());
        val shutdownThread = new Thread(() -> {
            await().pollDelay(3, TimeUnit.SECONDS).until(() -> true);
            entry.setStopFlag(true);
        });
        shutdownThread.start();
        entry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "32m", "3", "xx" });
        Mockito.doNothing().when(entry).process(PROJECT, DATAFLOW_ID);
        entry.doExecute();
    }

    @Test
    public void testMergeSegmentsDoExecute_GracefulShutDown() throws ExecuteException {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "0s");
        val entry = Mockito.spy(new StreamingMergeEntry());
        entry.setSparkSession(createSparkSession());
        val jobId = DATAFLOW_ID + "_merge";
        entry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "32m", "3", "xx" });
        Mockito.doNothing().when(entry).process(PROJECT, DATAFLOW_ID);
        Mockito.doReturn(true).when(entry).isGracefulShutdown(PROJECT, jobId);
        entry.doExecute();
        Assert.assertTrue(entry.getStopFlag());
    }

    @Test
    public void testMergeSegmentsDoExecute_KillApplication() throws ExecuteException {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-merge-interval", "0s");
        val entry = Mockito.spy(new StreamingMergeEntry());
        entry.setSparkSession(createSparkSession());
        val jobId = DATAFLOW_ID + "_merge";
        entry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "32m", "3", "xx" });
        Mockito.doThrow(new RuntimeException()).when(entry).process(PROJECT, DATAFLOW_ID);
        thrown.expect(ExecuteException.class);
        thrown.expectMessage("streaming merging segment error occured:");
        Mockito.doReturn(false).when(entry).isGracefulShutdown(PROJECT, jobId);
        entry.doExecute();
    }

    @Test
    public void testDoMergeStreamingSegment() {
        val entry = Mockito.spy(new StreamingMergeEntry());
        val config = getTestConfig();
        mockRestSupport(entry, config, "new-seg-123456");
        entry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "32m", "3", "xx" });
        val result = entry.doMergeStreamingSegment(PROJECT, DATAFLOW_ID, null, 1);
        Assert.assertNull(result);
    }

    @Test
    public void testNoClearHdfsFiles() {
        val config = getTestConfig();
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()), System.currentTimeMillis()));

        val start = new AtomicLong(System.currentTimeMillis() - 60000);
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(seg);
        mgr.updateDataflow(update);
        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds1.size());
    }

    @Test
    public void testClearHdfsFiles_ClearFiles() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-clean-interval", "0h");
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()), System.currentTimeMillis()));
        val start = new AtomicLong(System.currentTimeMillis() - 60000);
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(seg);
        UnitOfWork.doInTransactionWithRetry(() -> {
            mgr.updateDataflow(update);
            return null;
        }, PROJECT);

        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(0, removeSegIds1.size());
    }

    @Test
    public void testClearHdfsFiles_NotClearFiles() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-clean-interval", "1h");
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()), System.currentTimeMillis()));
        val start = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(seg);
        UnitOfWork.doInTransactionWithRetry(() -> {
            mgr.updateDataflow(update);
            return null;
        }, PROJECT);
        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds1.size());
    }

    @Test
    public void testClearHdfsFiles_CleanTooOldSeg() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-clean-interval", "1h");
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()),
                System.currentTimeMillis() - TimeUnit.HOURS.toMillis(30)));
        val start = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(seg);
        UnitOfWork.doInTransactionWithRetry(() -> {
            mgr.updateDataflow(update);
            return null;
        }, PROJECT);
        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(0, removeSegIds1.size());
    }

    @Test
    public void testClearHdfsFiles_NotDeletedSegId() {
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-segment-clean-interval", "0h");
        val mgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val seg = df.getSegments().get(0);
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.putHdfsFile(seg.getId(), new Pair<>(df.getSegmentHdfsPath(seg.getId()), System.currentTimeMillis()));
        val start = new AtomicLong(System.currentTimeMillis() - 60000);
        val removeSegIds = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds.size());
        entry.clearHdfsFiles(mgr.getDataflow(df.getId()), start);
        val removeSegIds1 = (Map<String, Pair<String, Long>>) ReflectionUtils.getField(entry, "removeSegIds");
        Assert.assertEquals(1, removeSegIds1.size());
    }

    @Test
    public void testCloseAuditLogStore() {
        StreamingMergeEntry entry = Mockito.spy(new StreamingMergeEntry());
        Mockito.when(entry.isJobOnCluster()).thenReturn(true);
        entry.closeAuditLogStore(createSparkSession());
    }

    @Test
    public void testCloseEntry() {
        StreamingMergeEntry entry = Mockito.spy(new StreamingMergeEntry());
        ReflectionTestUtils.invokeMethod(entry, "close", false);
    }

    @Test
    public void testCloseEntry_Error() {
        StreamingMergeEntry entry = Mockito.spy(new StreamingMergeEntry());
        ReflectionTestUtils.invokeMethod(entry, "close", true);
    }

    @Test
    public void testReportYarnApplicationInfo() {
        val entry = Mockito.spy(new StreamingMergeEntry());
        val config = getTestConfig();
        val targetPid = new AtomicLong();
        entry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "32m", "3", "xx" });
        entry.setSparkSession(createSparkSession());
        Mockito.when(entry.createRestSupport(config)).thenReturn(new RestSupport(config) {
            public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                targetPid.set(Long.parseLong(StreamingUtils.getProcessId()));
                return RestResponse.ok("0");
            }
        });
        val pid = StreamingUtils.getProcessId();
        entry.reportApplicationInfo();
        Assert.assertEquals(pid, String.valueOf(targetPid.get()));
    }

    private <T> void mockRestSupport(StreamingMergeEntry entry, KylinConfig config, T data) {
        Mockito.doReturn(new RestSupport(config) {
            public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
                    val jobId = DATAFLOW_ID + "_merge";
                    mgr.updateStreamingJob(jobId, copyForWrite -> {
                        copyForWrite.setJobExecutionId(0);
                    });
                    return null;
                }, PROJECT);
                return RestResponse.ok(data.toString());

            }
        }).when(entry).createRestSupport(any());
    }

    @Test
    public void tryReplaceHostAddress() {
        val url = "http://localhost:8080";
        StreamingMergeEntry entry = new StreamingMergeEntry();
        String host = entry.tryReplaceHostAddress(url);
        Assert.assertEquals("http://127.0.0.1:8080", host);

        val url1 = "http://unknow-host-9345:8080";
        val host1 = entry.tryReplaceHostAddress(url1);
        Assert.assertEquals(url1, host1);
    }

    @Test
    public void testIsJobOnCluster() {
        StreamingMergeEntry streamingMergeEntry = new StreamingMergeEntry();
        Assert.assertFalse(streamingMergeEntry.isJobOnCluster());
    }

    @Test
    public void testGetJobParams() {
        val jobId = StreamingUtils.getJobId(DATAFLOW_ID, JobTypeEnum.STREAMING_MERGE.name());
        val streamingJobMgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);

        val jobMeta = streamingJobMgr.getStreamingJobByUuid(jobId);
        val entry = new StreamingMergeEntry();
        val jobParams = entry.getJobParams(jobMeta);
        Assert.assertTrue(!jobParams.isEmpty());
    }

    @Test
    public void testIsGracefulShutdown() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val entry = new StreamingMergeEntry();
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        Assert.assertFalse(entry.isGracefulShutdown(PROJECT, buildJobId));
    }

    @Test
    public void testCreateRestSupport() {
        val config = getTestConfig();
        val entry = new StreamingMergeEntry();
        val rest = entry.createRestSupport(config);
        Assert.assertNotNull(rest);
        rest.close();
    }

    @Test
    public void testShutdown() {
        StreamingMergeEntry.stop();
        val stopFlag = (AtomicBoolean) ReflectionUtils.getField(StreamingMergeEntry.class, "gracefulStop");
        Assert.assertTrue(stopFlag.get());
    }

    @Test
    public void testGracefulStopInterface() {
        GracefulStopInterface gracefulStop = new StreamingMergeEntry();
        gracefulStop.setStopFlag(true);
        Assert.assertTrue(gracefulStop.getStopFlag());

        gracefulStop.setStopFlag(false);
        Assert.assertFalse(gracefulStop.getStopFlag());
    }
}
