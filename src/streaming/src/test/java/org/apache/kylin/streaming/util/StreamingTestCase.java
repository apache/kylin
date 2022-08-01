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
package org.apache.kylin.streaming.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.source.kafka.NSparkKafkaSource;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.streaming.app.StreamingMergeEntry;
import org.apache.kylin.streaming.common.MergeJobEntry;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.Assert;

import com.google.common.cache.Cache;

import lombok.val;

public class StreamingTestCase extends NLocalFileMetadataTestCase {
    protected static String MODEL_ALIAS = "stream_merge1";

    public NDataflow createSegments(NDataflowManager mgr, NDataflow df, int number) {
        return createSegments(mgr, df, number, null, null);
    }

    public NDataflow createSegments(NDataflowManager mgr, NDataflow df, int number, Integer layer) {
        return createSegments(mgr, df, number, layer, null);
    }

    public NDataflow createSegments(NDataflowManager mgr, NDataflow df, int number, Integer layer,
            NDataflowManager.NDataflowUpdater updater) {
        Assert.assertTrue(number > 0);
        for (long i = 0; i < number; i++) {
            val seg = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(i, i + 1,
                    createKafkaPartitionOffset(0, i * 100), createKafkaPartitionOffset(0, (i + 1) * 100)));
            seg.setStatus(SegmentStatusEnum.READY);
            if (layer != null && layer > 0) {
                seg.getAdditionalInfo().put("file_layer", layer + "");
            }
            val update = new NDataflowUpdate(df.getUuid());
            update.setToUpdateSegs(seg);
            mgr.updateDataflow(update);
        }
        if (updater != null) {
            mgr.updateDataflow(df.getId(), updater);
        }
        return mgr.getDataflow(df.getId());
    }

    public void setSegmentStorageSize(NDataSegment seg, long size) {
        ReflectionUtils.setField(seg, "storageSize", size);
    }

    public NDataflow setSegmentStorageSize(NDataflowManager mgr, NDataflow df, long size) {
        for (int i = 0; i < df.getSegments().size(); i++) {
            ReflectionUtils.setField(df.getSegments().get(i), "storageSize", size);
        }
        return mgr.getDataflow(df.getId());
    }

    public IndexPlan createIndexPlan(KylinConfig testConfig, String project, String modelId, String modelAlias) {
        val modelMgr = NDataModelManager.getInstance(testConfig, project);
        modelMgr.updateDataModel(modelId, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        NIndexPlanManager indexPlanMgr = NIndexPlanManager.getInstance(testConfig, project);
        final IndexPlan indexPlan = indexPlanMgr.getIndexPlanByModelAlias(modelAlias);

        val copy = indexPlan.copy();
        copy.setUuid(RandomUtil.randomUUIDStr());
        CubeTestUtils.createTmpModelAndCube(testConfig, copy, project, modelId);
        return copy;
    }

    protected void shutdownStreamingMergeJob(CountDownLatch latch) {
        new Thread(() -> {
            StreamingUtils.sleep(10 * 1000);
            StreamingMergeEntry.stop();
            latch.countDown();
        }).start();
    }

    protected MergeJobEntry createMergeJobEntry(NDataflowManager mgr, NDataflow df, SparkSession ss, String project) {
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias(MODEL_ALIAS);
        Assert.assertEquals(0, df.getSegments().size());

        df = createSegments(mgr, df, 10, null);
        val retainSegments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 10L,
                createKafkaPartitionOffset(0, 100L), createKafkaPartitionOffset(0, 10 * 100L));

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val afterMergeSeg = dfMgr.mergeSegments(dfMgr.getDataflow(df.getId()), rangeToMerge, true, 1, null);
        val df1 = dfMgr.getDataflow(df.getId());
        Assert.assertEquals(11, df1.getSegments().size());
        Assert.assertEquals(SegmentStatusEnum.NEW, df1.getSegment(afterMergeSeg.getId()).getStatus());
        Assert.assertEquals("1", df1.getSegment(afterMergeSeg.getId()).getAdditionalInfo().get("file_layer"));

        val updatedSegments = retainSegments.stream().map(seg -> {
            return df1.getSegment(seg.getId());
        }).collect(Collectors.toList());
        val globalMergeTime = new AtomicLong(System.currentTimeMillis());
        val mergeJobEntry = new MergeJobEntry(ss, project, df.getId(), 0L, globalMergeTime, updatedSegments,
                afterMergeSeg);
        return mergeJobEntry;
    }

    protected SparkSession createSparkSession() {
        return SparkSession.builder().master("local").appName("test").getOrCreate();
    }

    public SegmentRange.KafkaOffsetPartitionedSegmentRange createSegmentRange() {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 10L,
                createKafkaPartitionsOffset(3, 100L), createKafkaPartitionsOffset(3, 10 * 100L));
        return rangeToMerge;
    }

    public SegmentRange.KafkaOffsetPartitionedSegmentRange createSegmentRange(long startOffset, long endOffset,
            int partitions, long partitionStartOffset, long partitionEndOffset) {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(startOffset, endOffset,
                createKafkaPartitionsOffset(partitions, partitionStartOffset),
                createKafkaPartitionsOffset(partitions, partitionEndOffset));
        return rangeToMerge;
    }

    public NSparkKafkaSource createSparkKafkaSource(KylinConfig config) {
        val sourceAware = new ISourceAware() {
            @Override
            public int getSourceType() {
                return 1;
            }

            @Override
            public KylinConfig getConfig() {
                return config;
            }
        };
        val cache = (Cache<String, ISource>) ReflectionUtils.getField(SourceFactory.class, "sourceMap");
        cache.invalidateAll();
        val source = (NSparkKafkaSource) SourceFactory.getSource(sourceAware);
        assert source.supportBuildSnapShotByPartition();
        return source;
    }

    public void testWithRetry(Callback callback) {
        val assertMeet = new AtomicBoolean(false);
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            for (int i = 0; i < 3; i++) {
                try {
                    callback.call();
                    assertMeet.set(true);
                    break;
                } catch (Exception e) {
                    continue;
                }
            }
            return true;
        });
        if (!assertMeet.get()) {
            Assert.fail();
        }
    }

    public static interface Callback {
        public void call() throws Exception;
    }

}
