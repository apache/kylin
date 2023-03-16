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

package org.apache.kylin.engine.spark.job;

import com.clearspring.analytics.util.Lists;

import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class NSparkMergingJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Before
    public void setup() {
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        NDefaultScheduler.destroyInstance();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        config = getTestConfig();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testMultiPartitionMergeSegments() throws Exception {
        val project = "default";
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "d2edf0c5-5eb2-4968-9ad5-09efbf659324";
        val segmentId2 = "ff839b0b-2c23-4420-b332-0df70e36c343";

        val dataflowMgr = NDataflowManager.getInstance(config, project);
        val dataflow = dataflowMgr.getDataflow(modelId);
        val segment1 = dataflow.getSegment(segmentId1);
        val segment2 = dataflow.getSegment(segmentId2);

        fakeEmptyPartitionLayoutData(segment1);
        fakeEmptyPartitionLayoutData(segment2);

        val layoutList = segment1.getSegDetails()//
                .getLayouts().stream().map(NDataLayout::getLayout).collect(Collectors.toList());

        val partitionIdList = segment1.getAllPartitionIds();

        NDataSegment mergedSegment = dataflowMgr.mergeSegments(dataflow, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2020-11-03"), SegmentRange.dateToLong("2020-11-05")), false);

        val segmentId = mergedSegment.getId();
        val bucketStart = new AtomicLong(0);
        val jobBucketSet = layoutList.stream().flatMap(layout -> //
        partitionIdList.stream().map(partition -> //
        new JobBucket(segmentId, layout.getId(), bucketStart.incrementAndGet(), partition)))
                .collect(Collectors.toSet());

        NSparkMergingJob mergeJob = NSparkMergingJob.merge(mergedSegment, //
                Sets.newLinkedHashSet(layoutList), //
                "ADMIN", //
                RandomUtil.randomUUIDStr(), //
                Sets.newHashSet(partitionIdList), //
                jobBucketSet);

        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        execMgr.addJob(mergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(mergeJob));
        val merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(mergeJob.getSparkMergingStep());

        val dataflow1 = dataflowMgr.getDataflow(modelId);
        val dataPartitionSize = dataflow1.getSegment(segmentId).getMultiPartitions().size();
        Assert.assertEquals(segment1.getMultiPartitions().size(), dataPartitionSize);
        Assert.assertEquals(segment1.getMultiPartitions().size(), dataPartitionSize);
    }

    private void fakeEmptyPartitionLayoutData(NDataSegment segment) {

        Map<String, DataType> partitionMap = Maps.newLinkedHashMap();
        segment.getModel().getMultiPartitionDesc().getColumnRefs().forEach(ref -> //
        partitionMap.put(String.valueOf(segment.getModel() //
                .getColumnIdByColumnName(ref.getIdentity())), ref.getType()));

        segment.getSegDetails().getLayouts().forEach(ld -> {
            val layout = ld.getLayout();
            StructType schema0 = new StructType();
            Map<String, DataType> fields = Maps.newLinkedHashMap();
            // partition column
            fields.putAll(partitionMap);
            // dimesions
            layout.getOrderedDimensions().forEach((k, v) -> //
            fields.put(String.valueOf(k), v.getType()));

            // measures
            layout.getOrderedMeasures().forEach((k, v) -> //
            fields.put(String.valueOf(k), v.getFunction().getReturnDataType()));

            // schema
            for (Map.Entry<String, DataType> entry : fields.entrySet()) {
                schema0 = schema0.add(entry.getKey(), //
                        SparderTypeUtil.toSparkType(entry.getValue(), false));
            }

            val schema = schema0;
            ld.getMultiPartition().forEach(pd -> {
                val dataset = ss.createDataFrame(Lists.newArrayList(), schema);
                val path = NSparkCubingUtil.getStoragePath(segment, ld.getLayoutId(), pd.getBucketId());
                dataset.write().mode(SaveMode.Overwrite).parquet(path);
            });
        });
    }

    @Test
    public void testCancelJob() {
        NSparkMergingJob mergingJob = new NSparkMergingJob();
        mergingJob.setProject("default");
        NSparkMergingStep mergingStep = new NSparkMergingStep();
        mergingStep.setProject("default");
        mergingJob.addTask(mergingStep);

        mergingJob.cancelJob();

        mergingStep.setParam(NBatchConstants.P_SEGMENT_IDS, "");
        mergingStep.setParam(NBatchConstants.P_DATAFLOW_ID, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        mergingJob.cancelJob();
    }
}
