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

package org.apache.kylin.engine.spark.job.step.merge;

import java.util.Collections;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.job.SegmentJob;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.source.SourceFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import lombok.AllArgsConstructor;
import lombok.val;

@MetadataInfo
class MergeStageJavaTest {
    private static final String PROJECT = "default";

    @Test
    void test() {
        val modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, true, true, true, true);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            Assertions.assertTrue(merge.getColumnSourceBytes().isEmpty());
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, true, true, false, true);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertFalse(columnSourceBytes.isEmpty());
            Assertions.assertEquals(20, columnSourceBytes.size());
            columnSourceBytes.forEach((k, v) -> Assertions.assertEquals(5263, v));
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, true, true, true, false);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertFalse(columnSourceBytes.isEmpty());
            Assertions.assertEquals(20, columnSourceBytes.size());
            columnSourceBytes.forEach((k, v) -> Assertions.assertEquals(5263, v));
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, true, true, false, false);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertFalse(columnSourceBytes.isEmpty());
            Assertions.assertEquals(20, columnSourceBytes.size());
            columnSourceBytes.forEach((k, v) -> Assertions.assertEquals(10526, v));
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, false, true, false, false);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertFalse(columnSourceBytes.isEmpty());
            Assertions.assertEquals(20, columnSourceBytes.size());
            columnSourceBytes.forEach((k, v) -> Assertions.assertEquals(15789, v));
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, true, false, false, false);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertFalse(columnSourceBytes.isEmpty());
            Assertions.assertEquals(20, columnSourceBytes.size());
            columnSourceBytes.forEach((k, v) -> Assertions.assertEquals(21052, v));
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, false, false, false, false);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertFalse(columnSourceBytes.isEmpty());
            Assertions.assertEquals(20, columnSourceBytes.size());
            columnSourceBytes.forEach((k, v) -> Assertions.assertEquals(26315, v));
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, false, true, true, true);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertTrue(columnSourceBytes.isEmpty());
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
        {
            val dfm = NDataflowManager.getInstance(config, PROJECT);
            Result result = executeMergeColumnBytes(dfm, modelId, config, true, false, true, true);
            val merge = dfm.getDataflow(modelId).getSegment(result.segmentMerge.getId());
            val columnSourceBytes = merge.getColumnSourceBytes();
            Assertions.assertTrue(columnSourceBytes.isEmpty());
            NDataflowUpdate update = new NDataflowUpdate(modelId);
            update.setToRemoveSegs(result.segment, result.segment2, result.segmentMerge);
            dfm.updateDataflow(update);
        }
    }

    private Result executeMergeColumnBytes(NDataflowManager dfm, String modelId, KylinConfig config,
            boolean segment1EmptyColumnSourceBytes, boolean segment2EmptyColumnSourceBytes,
            boolean segment1ZeroSourceCount, boolean segment2ZeroSourceCount) {
        val df = dfm.getDataflow(modelId);
        val modelDesc = df.getModel();
        val table = NTableMetadataManager.getInstance(config, PROJECT).getTableDesc(modelDesc.getRootFactTableName());
        val segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange("662659200000", "662745600000");
        NDataSegment segment = dfm.appendSegment(df, segmentRangeToBuild, SegmentStatusEnum.READY,
                Collections.emptyList());
        val usageManager = SourceUsageManager.getInstance(config);
        if (!segment1ZeroSourceCount) {
            segment.setSourceBytesSize(1000000L);
            segment.setSourceCount(1000L);
        }
        if (!segment1EmptyColumnSourceBytes) {
            segment.setSourceBytesSize(2000000L);
            val avgColumnSourceBytes = usageManager.calcAvgColumnSourceBytes(segment);
            segment.setColumnSourceBytes(avgColumnSourceBytes);
            segment.setSourceBytesSize(1000000L);
        }

        val segmentRangeToBuild2 = SourceFactory.getSource(table).getSegmentRange("662745600000", "662832000000");
        NDataSegment segment2 = dfm.appendSegment(df, segmentRangeToBuild2, SegmentStatusEnum.READY,
                Collections.emptyList());
        if (!segment2ZeroSourceCount) {
            segment2.setSourceCount(1000L);
            segment2.setSourceBytesSize(1000000L);
        }
        if (!segment2EmptyColumnSourceBytes) {
            segment2.setSourceBytesSize(3000000L);
            val avgColumnSourceBytes = usageManager.calcAvgColumnSourceBytes(segment2);
            segment2.setColumnSourceBytes(avgColumnSourceBytes);
            segment2.setSourceBytesSize(1000000L);
        }
        val segmentRangeMerge = SourceFactory.getSource(table).getSegmentRange("662659200000", "662832000000");
        NDataSegment segmentMerge = dfm.appendSegment(df, segmentRangeMerge, SegmentStatusEnum.NEW,
                Collections.emptyList());

        val jobContext = Mockito.mock(SegmentJob.class);
        Mockito.when(jobContext.getConfig()).thenReturn(config);
        Mockito.when(jobContext.getDataflowId()).thenReturn(modelId);
        Mockito.when(jobContext.getUnmergedSegments(ArgumentMatchers.any()))
                .thenReturn(Lists.newArrayList(segment, segment2));

        val mergeStageMock = new MergeStageMock(jobContext, segmentMerge);

        mergeStageMock.mergeColumnBytes();
        return new Result(segment, segment2, segmentMerge);
    }
}

@AllArgsConstructor
class Result {
    NDataSegment segment;
    NDataSegment segment2;
    NDataSegment segmentMerge;
}

class MergeStageMock extends MergeStage {

    public MergeStageMock(SegmentJob jobContext, NDataSegment dataSegment) {
        super(jobContext, dataSegment);
    }

    @Override
    public String getStageName() {
        return "MergeStageMock";
    }

    @Override
    public void execute() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mergeColumnBytes() {
        super.mergeColumnBytes();
    }
}
