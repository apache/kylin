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
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.spark.sql.KylinDataFrameManager;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.util.ReflectionUtils;

import com.google.common.collect.ImmutableBiMap;

import lombok.val;
import lombok.var;

@MetadataInfo(project = "streaming_test")
class KylinDataFrameManagerTest {

    @Test
    void testCuboidTableOfFusionModel() {
        val ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, "streaming_test");
        var dataflow = dataflowManager.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Assert.assertTrue(dataflow.isStreaming() && dataflow.getModel().isFusionModel());

        val kylinDataFrameManager = Mockito.spy(new KylinDataFrameManager(ss));
        kylinDataFrameManager.option("isFastBitmapEnabled", "false");
        {
            // condition: id != null && end != Long.MinValue
            val partitionTblCol = dataflow.getModel().getPartitionDesc().getPartitionDateColumnRef();
            val layoutEntity = Mockito.spy(new LayoutEntity());
            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();
            ImmutableBiMap<Integer, TblColRef> orderedDimensions = dimsBuilder.put(1, partitionTblCol).build();
            Mockito.when(layoutEntity.getOrderedDimensions()).thenReturn(orderedDimensions);
            val df = kylinDataFrameManager.cuboidTable(dataflow, layoutEntity, "3e560d22-b749-48c3-9f64-d4230207f120");
            Assert.assertEquals(1, df.columns().length);
        }
        {
            // condition: id == null
            val df = kylinDataFrameManager.cuboidTable(dataflow, new LayoutEntity(),
                    "3e560d22-b749-48c3-9f64-d4230207f120");
            Assert.assertEquals(0, df.columns().length);
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

            dataflowManager.updateDataflow(batchDataflow.getId(), updater -> {
                updater.getSegments().forEach(seg -> {
                    try {
                        val timeRange = seg.getTSRange();
                        val field = TimeRange.class.getDeclaredField("end");
                        field.setAccessible(true);
                        ReflectionUtils.setField(field, timeRange, Long.MIN_VALUE);
                        seg.setTimeRange(timeRange);
                    } catch (Exception e) {
                        Assert.fail(e.getMessage());
                    }
                });
            });
            val df = kylinDataFrameManager.cuboidTable(dataflow, layoutEntity, "3e560d22-b749-48c3-9f64-d4230207f120");
            Assert.assertEquals(1, df.columns().length);
        }
        ss.stop();
    }

    @Test
    void testCuboidTableOfBatchModel() {
        val ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, "streaming_test");
        val dataflow = dataflowManager.getDataflow("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");
        Assert.assertFalse(dataflow.isStreaming());
        val kylinDataFrameManager = Mockito.spy(new KylinDataFrameManager(ss));
        kylinDataFrameManager.option("isFastBitmapEnabled", "false");
        val layoutEntity = new LayoutEntity();
        {
            val df = kylinDataFrameManager.cuboidTable(dataflow, layoutEntity, "86b5daaa-e295-4e8c-b877-f97bda69bee5");
            Assert.assertEquals(0, df.columns().length);
        }
        ss.stop();
    }
}
