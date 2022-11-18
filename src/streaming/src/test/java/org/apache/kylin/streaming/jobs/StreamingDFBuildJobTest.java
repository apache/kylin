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
package org.apache.kylin.streaming.jobs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StreamingTestConstant;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.engine.spark.job.BuildLayoutWithUpdate;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.streaming.app.StreamingEntry;
import org.apache.kylin.streaming.common.BuildJobEntry;
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

import com.google.common.collect.Lists;

import lombok.val;
import lombok.var;

public class StreamingDFBuildJobTest extends StreamingTestCase {

    private static final String PROJECT = "streaming_test";
    private static final String DATAFLOW_ID = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
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
    public void testStreamingBuild() {
        val config = getTestConfig();
        KylinBuildEnv.getOrCreate(config);
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val mgr = NDataflowManager.getInstance(config, PROJECT);
        var df = mgr.getDataflow(DATAFLOW_ID);

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(df.getId());

        val layoutEntitys = StreamingUtils.getToBuildLayouts(df);
        var nSpanningTree = NSpanningTreeFactory.fromLayouts(layoutEntitys, DATAFLOW_ID);
        val model = df.getModel();
        val builder = Mockito.spy(new StreamingDFBuildJob(PROJECT));
        val streamingEntry = new StreamingEntry();
        streamingEntry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "3000", "", "xx" });
        val ss = createSparkSession();
        streamingEntry.setSparkSession(ss);

        val tuple3 = streamingEntry.generateStreamQueryForOneModel();
        val batchDF = tuple3._1();
        val streamFlatTable = tuple3._3();

        val seg1 = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 10L,
                createKafkaPartitionsOffset(3, 100L), createKafkaPartitionsOffset(3, 200L)));
        seg1.setStatus(SegmentStatusEnum.READY);
        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToUpdateSegs(seg1);
        List<NDataLayout> layouts = Lists.newArrayList();
        val dfCopy = df;
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.getIndexPlan(DATAFLOW_ID).getAllLayouts().forEach(layout -> {
            layouts.add(NDataLayout.newDataLayout(dfCopy, seg1.getId(), layout.getId()));
        });
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        mgr.updateDataflow(update2);
        streamFlatTable.seg_$eq(seg1);
        val encodedStreamDataset = streamFlatTable.encodeStreamingDataset(seg1, model, batchDF);
        val batchBuildJob = new BuildJobEntry(ss, PROJECT, DATAFLOW_ID, 100L, seg1, encodedStreamDataset,
                nSpanningTree);
        try {
            val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
            var newDataflow = dfMgr.getDataflow(batchBuildJob.dataflowId());
            Assert.assertEquals(RealizationStatusEnum.OFFLINE, newDataflow.getStatus());
            Assert.assertEquals(4, newDataflow.getSegment(seg1.getId()).getLayoutsMap().size());
            val oldFileCount = newDataflow.getSegment(seg1.getId()).getStorageFileCount();
            val oldByteSize = newDataflow.getSegment(seg1.getId()).getStorageBytesSize();

            builder.streamBuild(batchBuildJob);
            newDataflow = dfMgr.getDataflow(batchBuildJob.dataflowId());
            Assert.assertEquals(RealizationStatusEnum.ONLINE, newDataflow.getStatus());
            Assert.assertEquals(4, newDataflow.getSegment(seg1.getId()).getLayoutsMap().size());
            Assert.assertTrue(newDataflow.getSegment(seg1.getId()).getStorageFileCount() > oldFileCount);
            Assert.assertTrue(newDataflow.getSegment(seg1.getId()).getStorageBytesSize() > oldByteSize);

            dfMgr.updateDataflow(batchBuildJob.dataflowId(), updater -> {
                updater.setStatus(RealizationStatusEnum.OFFLINE);
            });
            Mockito.when(builder.createRestSupport()).thenReturn(new RestSupport(config) {
                public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                    dfMgr.updateDataflow(batchBuildJob.dataflowId(), updater -> {
                        updater.setStatus(RealizationStatusEnum.ONLINE);
                    });
                    return RestResponse.ok();
                }
            });
            builder.updateSegment(batchBuildJob);
            newDataflow = dfMgr.getDataflow(batchBuildJob.dataflowId());
            Assert.assertEquals(RealizationStatusEnum.ONLINE, newDataflow.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetSegment() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val mgr = NDataflowManager.getInstance(config, PROJECT);
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        var df = mgr.getDataflow(dataflowId);
        val builder = new StreamingDFBuildJob(PROJECT);
        builder.setParam(NBatchConstants.P_DATAFLOW_ID, dataflowId);
        val seg = builder.getSegment("c380dd2a-43b8-4268-b73d-2a5f76236632");
        Assert.assertNotNull(seg);
        Assert.assertEquals("c380dd2a-43b8-4268-b73d-2a5f76236632", seg.getId());
    }

    @Test
    public void testShutdown() {
        StreamingDFBuildJob builder = new StreamingDFBuildJob(PROJECT);
        builder.shutdown();
        BuildLayoutWithUpdate buildLayout = (BuildLayoutWithUpdate) ReflectionUtils.getField(builder,
                "buildLayoutWithUpdate");
        val config = getTestConfig();
        try {
            buildLayout.submit(new BuildLayoutWithUpdate.JobEntity() {
                @Override
                public long getIndexId() {
                    return 0;
                }

                @Override
                public String getName() {
                    return null;
                }

                @Override
                public List<NDataLayout> build() throws IOException {
                    return null;
                }
            }, config);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RejectedExecutionException);
        }
    }
}
