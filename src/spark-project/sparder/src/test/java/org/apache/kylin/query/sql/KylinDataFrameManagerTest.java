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
package org.apache.kylin.query.sql;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegmentManager;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.spark.sql.KylinDataFrameManager;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.util.ReflectionUtils;

import lombok.val;
import lombok.var;

@Disabled("Not for open source")
@MetadataInfo(project = "streaming_test")
class KylinDataFrameManagerTest {

    @Test
    void testCuboidTableOfFusionModel() {
        val ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, "streaming_test");
        var dataflow = dataflowManager.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Assertions.assertTrue(dataflow.isStreaming() && dataflow.getModel().isFusionModel());

        val kylinDataFrameManager = Mockito.spy(new KylinDataFrameManager(ss));
        kylinDataFrameManager.option("isFastBitmapEnabled", "false");
        {
            // condition: id != null && end != Long.MinValue
            val partitionTblCol = dataflow.getModel().getPartitionDesc().getPartitionDateColumnRef();
            val layoutEntity = Mockito.spy(new LayoutEntity());
            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();
            ImmutableBiMap<Integer, TblColRef> orderedDimensions = dimsBuilder.put(1, partitionTblCol).build();
            Mockito.when(layoutEntity.getOrderedDimensions()).thenReturn(orderedDimensions);
            val plan = kylinDataFrameManager.cuboidTable(dataflow, layoutEntity,
                    "3e560d22-b749-48c3-9f64-d4230207f120");
            Assertions.assertEquals(1, plan.output().size());
        }
        {
            // condition: id == null
            val plan = kylinDataFrameManager.cuboidTable(dataflow, new LayoutEntity(),
                    "3e560d22-b749-48c3-9f64-d4230207f120");
            Assertions.assertEquals(0, plan.output().size());
        }

        {
            // condition: end == Long.MinValue
            val partitionTblCol = dataflow.getModel().getPartitionDesc().getPartitionDateColumnRef();
            val layoutEntity = Mockito.spy(new LayoutEntity());
            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();
            ImmutableBiMap<Integer, TblColRef> orderedDimensions = dimsBuilder.put(1, partitionTblCol).build();
            Mockito.when(layoutEntity.getOrderedDimensions()).thenReturn(orderedDimensions);
            val fusionModel = FusionModelManager.getInstance(config, dataflow.getProject())
                    .getFusionModel(dataflow.getModel().getFusionId());
            val batchModelId = fusionModel.getBatchModel().getUuid();
            val batchDataflow = NDataflowManager.getInstance(config, dataflow.getProject()).getDataflow(batchModelId);

            NDataSegmentManager segmentManager = config.getManager(dataflow.getProject(), NDataSegmentManager.class);
            batchDataflow.getSegments().forEach(seg -> {
                segmentManager.update(seg.getUuid(), copyForWrite -> {
                    try {
                        val timeRange = copyForWrite.getTSRange();
                        val field = TimeRange.class.getDeclaredField("end");
                        field.setAccessible(true);
                        ReflectionUtils.setField(field, timeRange, Long.MIN_VALUE);
                        copyForWrite.setTimeRange(timeRange);
                    } catch (Exception e) {
                        Assertions.fail(e.getMessage());
                    }
                });
            });
            val plan = kylinDataFrameManager.cuboidTable(dataflow, layoutEntity,
                    "3e560d22-b749-48c3-9f64-d4230207f120");
            Assertions.assertEquals(1, plan.output().size());
        }
        ss.stop();
    }

    @Test
    void testCuboidTableOfBatchModel() {
        val ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, "streaming_test");
        val dataflow = dataflowManager.getDataflow("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");
        Assertions.assertFalse(dataflow.isStreaming());
        val kylinDataFrameManager = Mockito.spy(new KylinDataFrameManager(ss));
        kylinDataFrameManager.option("isFastBitmapEnabled", "false");
        val layoutEntity = new LayoutEntity();
        {
            val plan = kylinDataFrameManager.cuboidTable(dataflow, layoutEntity,
                    "027db8f2-145d-4e6c-6a1b-7139bb1fb5bc");
            Assertions.assertEquals(0, plan.output().size());
        }
        ss.stop();
    }
}
