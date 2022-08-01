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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_INDEX_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;

/**
 *
 */
public class JobManagerTest extends NLocalFileMetadataTestCase {

    private final static String PROJECT = "default";

    private static JobManager jobManager;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        jobManager = JobManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        ExecutableUtils.initJobFactory();
    }

    private void assertExeption(Functions f, String msg) {
        try {
            f.process();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(msg, e.getMessage());
        }
    }

    @Test
    public void testPartitionBuildJob() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId = "73570f31-05a5-448f-973c-44209830dd01";
        val dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val df = dfm.getDataflow(modelId);
        JobParam param = new JobParam();
        param.withTargetSegments(Sets.newHashSet(segmentId));
        param.setModel(modelId);
        param.setOwner("ADMIN");
        param.setProject(PROJECT);

        // =========================== check partitions start ===============================
        assertExeption(() -> {
            // build none partition
            param.setTargetPartitions(Sets.newHashSet());
            jobManager.buildPartitionJob(param);
        }, JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY.getMsg());

        assertExeption(() -> {
            // build a partition already in segment
            param.setTargetPartitions(Sets.newHashSet(7L));
            jobManager.buildPartitionJob(param);
        }, JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getMsg());

        long originBucketId = df.getSegment(segmentId).getMaxBucketId();
        // success build partition
        param.setTargetPartitions(Sets.newHashSet(9L));
        jobManager.buildPartitionJob(param);
        List<AbstractExecutable> executables = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(executables.size(), 1);
        String bucketParam = executables.get(0).getParam(NBatchConstants.P_BUCKETS);
        Set<JobBucket> buckets = ExecutableParams.getBuckets(bucketParam);
        Assert.assertEquals(buckets.size(), 15);
        long nowBucketId = dfm.getDataflow(modelId).getSegment(segmentId).getMaxBucketId();
        Assert.assertEquals(nowBucketId - originBucketId, 15);
        // ====================== check partitions end ===============================

        // ====================== check layouts start ================================
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(param.getModel());
        indexPlan.getAllLayouts();
        Assert.assertEquals(executables.get(0).getLayoutIds().size(), 15);

        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        dataSegment1.getMultiPartitions().forEach(partition -> {
            partition.setStatus(PartitionStatusEnum.NEW);
        });
        val segments = Lists.newArrayList(dataSegment1);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[] {}));
        dfm.updateDataflow(update);

        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), false, true);
        });
        JobParam param2 = new JobParam(Sets.newHashSet(dataSegment1.getId()), null, modelId, "ADMIN",
                Sets.newHashSet(7L), null);
        param2.setProject(PROJECT);
        jobManager.buildPartitionJob(param2);
        List<AbstractExecutable> executables2 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(2, executables2.size());
        Assert.assertEquals(14, indexPlanManager.getIndexPlan(modelId).getAllLayouts().size());
        Assert.assertEquals(14, executables2.get(1).getLayoutIds().size());

        // Although a new layout is added, the layout of the previous job in the same segment is still used.
        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
            val newTableIndex = new IndexEntity();
            newTableIndex.setDimensions(Lists.newArrayList(1, 3));
            newTableIndex.setId(20_000_000_000L);
            val layout = new LayoutEntity();
            layout.setId(20_000_000_001L);
            layout.setColOrder(Lists.newArrayList(1, 3));
            newTableIndex.setLayouts(Arrays.asList(layout));
            List<IndexEntity> indexes = copyForWrite.getAllIndexes();
            indexes.add(newTableIndex);
            copyForWrite.setIndexes(indexes);
        });
        JobParam param3 = new JobParam(Sets.newHashSet(dataSegment1.getId()), null, modelId, "ADMIN",
                Sets.newHashSet(8L), null);
        jobManager.buildPartitionJob(param3);
        List<AbstractExecutable> executables3 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(3, executables3.size());
        Assert.assertEquals(15, indexPlanManager.getIndexPlan(modelId).getAllLayouts().size());
        Assert.assertEquals(14, executables3.get(1).getLayoutIds().size());
        Assert.assertEquals(14, executables3.get(2).getLayoutIds().size());
        // ====================== check layouts end ==================================

        checkConcurrent(param3);
    }

    @Test
    public void testPartitionJobNoIndex() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId = "73570f31-05a5-448f-973c-44209830dd01";
        val dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val df = dfm.getDataflow(modelId);
        JobParam param = new JobParam();
        param.withTargetSegments(Sets.newHashSet(segmentId));
        param.setModel(modelId);
        param.setOwner("ADMIN");
        param.setProject(PROJECT);
        val indexManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        UnitOfWork.doInTransactionWithRetry(() -> indexManager.updateIndexPlan(modelId, copyForWrite -> {
            Set<Long> layouts = copyForWrite.getAllLayoutIds(false);
            copyForWrite.removeLayouts(layouts, true, true);
        }), modelId);

        assertExeption(() -> {
            // build none partition
            param.setTargetPartitions(Sets.newHashSet(9L));
            jobManager.buildPartitionJob(param);
        }, JOB_CREATE_CHECK_INDEX_FAIL.getMsg());
    }

    @Test
    public void testIndexBuildJob() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);

        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        JobParam param = new JobParam(Sets.newHashSet(dataSegment1.getId()), null, modelId, "ADMIN", null, null);
        jobManager.addRelatedIndexJob(param);
        List<AbstractExecutable> executables = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(15, executables.get(0).getLayoutIds().size());
        Assert.assertEquals(2, executables.get(0).getTargetPartitions().size());

        NDataSegment dataSegment2 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-02-01",
                "2010-03-01", SegmentStatusEnum.READY);
        JobParam param2 = new JobParam(Sets.newHashSet(dataSegment2.getId()), Sets.newHashSet(1L), modelId, "ADMIN",
                null, null);
        jobManager.addRelatedIndexJob(param2);
        List<AbstractExecutable> executables2 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(2, executables2.size());
        Assert.assertEquals(1, executables2.get(1).getLayoutIds().size());
        checkConcurrent(param2);

        NDataSegment dataSegment3 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-03-01",
                "2010-04-01", SegmentStatusEnum.NEW);
        JobParam param3 = new JobParam(Sets.newHashSet(dataSegment3.getId()), Sets.newHashSet(1L), modelId, "ADMIN",
                null, null);

        try {
            jobManager.addRelatedIndexJob(param3);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getMessage(), "No segment is ready in this job.");
        }

        NDataSegment dataSegment4 = generateSegmentForMultiPartition(modelId, Lists.newArrayList(), "2010-04-01",
                "2010-05-01", SegmentStatusEnum.READY);
        JobParam param4 = new JobParam(Sets.newHashSet(dataSegment4.getId()), Sets.newHashSet(1L), modelId, "ADMIN",
                null, null);
        assertExeption(() -> {
            jobManager.addRelatedIndexJob(param4);
        }, JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY.getMsg());
    }

    @Test
    public void testSegmentBuildJob() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val df = dfm.getDataflow(modelId);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);

        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        JobParam param = new JobParam(Sets.newHashSet(dataSegment1.getId()), null, modelId, "ADMIN",
                Sets.newHashSet(7L), null);
        jobManager.addSegmentJob(param);
        List<AbstractExecutable> executables = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(1, executables.get(0).getTargetPartitions().size());
        Assert.assertEquals(15, executables.get(0).getLayoutIds().size());

        NDataSegment dataSegment2 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-02-01",
                "2010-03-01", SegmentStatusEnum.READY);
        JobParam param2 = new JobParam(Sets.newHashSet(dataSegment2.getId()), Sets.newHashSet(1L), modelId, "ADMIN",
                Sets.newHashSet(7L), null);
        jobManager.addSegmentJob(param2);
        List<AbstractExecutable> executables2 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(2, executables2.size());
        Assert.assertEquals(1, executables2.get(1).getTargetPartitions().size());
        Assert.assertEquals(1, executables2.get(1).getLayoutIds().size());

        checkConcurrent(param2);
    }

    @Test
    public void testSegmentRefreshJob() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "73570f31-05a5-448f-973c-44209830dd01";
        val segmentId2 = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        generateTableIndex(modelId, 20_000_000_001L);
        JobParam param = new JobParam(Sets.newHashSet(segmentId1), null, modelId, "ADMIN", null, null);
        jobManager.refreshSegmentJob(param, false);
        List<AbstractExecutable> executables1 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(1, executables1.size());
        Assert.assertEquals(15, executables1.get(0).getLayoutIds().size());

        // refresh all layouts
        JobParam param2 = new JobParam(Sets.newHashSet(segmentId2), null, modelId, "ADMIN", null, null);
        jobManager.refreshSegmentJob(param2, true);
        List<AbstractExecutable> executables2 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(2, executables2.size());
        Assert.assertEquals(16, executables2.get(1).getLayoutIds().size());
        Assert.assertEquals(1, executables2.get(0).getTargetPartitions().size());
        Assert.assertEquals(2, executables2.get(1).getTargetPartitions().size());

        checkConcurrent(param2);
    }

    @Test
    public void testPartitionRefreshJob() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "73570f31-05a5-448f-973c-44209830dd01";
        val segmentId2 = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        generateTableIndex(modelId, 20_000_000_001L);
        Set<Long> targetPartitions = Sets.newHashSet(7L);

        JobParam param = new JobParam(Sets.newHashSet(segmentId1), null, modelId, "ADMIN", targetPartitions, null);
        jobManager.refreshSegmentJob(param, false);
        List<AbstractExecutable> executables1 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(1, executables1.size());
        Assert.assertEquals(15, executables1.get(0).getLayoutIds().size());

        // refresh all layouts
        JobParam param2 = new JobParam(Sets.newHashSet(segmentId2), null, modelId, "ADMIN", targetPartitions, null);
        jobManager.refreshSegmentJob(param2, true);
        List<AbstractExecutable> executables2 = getRunningExecutables(PROJECT, modelId);
        Assert.assertEquals(2, executables2.size());
        Assert.assertEquals(16, executables2.get(1).getLayoutIds().size());
        Assert.assertEquals(1, executables2.get(0).getTargetPartitions().size());
        Assert.assertEquals(1, executables2.get(1).getTargetPartitions().size());

        checkConcurrent(param2);
    }

    @Test
    public void testQuotaLimitReached() {
        thrown.expect(KylinException.class);
        NDefaultScheduler defaultScheduler = NDefaultScheduler.getInstance(PROJECT);
        defaultScheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        defaultScheduler.getContext().setReachQuotaLimit(true);
        JobParam param = new JobParam(Sets.newHashSet(), null, null, "ADMIn", Sets.newHashSet(), null);
        try {
            jobManager.addJob(param);
        } finally {
            defaultScheduler.forceShutdown();
            defaultScheduler.getContext().setReachQuotaLimit(false);
        }
    }

    @Test
    public void testAddJob_throwsException() {
        KylinConfig kylinConfig = mock(KylinConfig.class);
        ReflectionTestUtils.setField(jobManager, "config", kylinConfig);
        when(kylinConfig.isJobNode()).thenReturn(false);
        when(kylinConfig.isUTEnv()).thenReturn(false);
        try {
            jobManager.addJob(null, null);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(JOB_CREATE_ABANDON.getCodeMsg(), e.toString());
        }
    }

    // Concurrent job exeption
    public void checkConcurrent(JobParam param) {
        assertExeption(() -> {
            jobManager.addSegmentJob(param);
        }, JOB_CREATE_CHECK_FAIL.getMsg());
    }

    private List<AbstractExecutable> getRunningExecutables(String project, String model) {
        List<AbstractExecutable> runningExecutables = NExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).getRunningExecutables(project, model);
        runningExecutables.sort(Comparator.comparing(AbstractExecutable::getCreateTime));
        return runningExecutables;
    }

    private void generateTableIndex(String modelId, long indexId) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
            val newTableIndex = new IndexEntity();
            newTableIndex.setDimensions(Lists.newArrayList(1, 3));
            newTableIndex.setId(indexId);
            val layout = new LayoutEntity();
            layout.setId(indexId);
            layout.setColOrder(Lists.newArrayList(1, 3));
            newTableIndex.setLayouts(Arrays.asList(layout));
            List<IndexEntity> indexes = copyForWrite.getAllIndexes();
            indexes.add(newTableIndex);
            copyForWrite.setIndexes(indexes);
        });
    }

    private NDataSegment generateSegmentForMultiPartition(String modelId, List<String> partitionValues, String start,
            String end, SegmentStatusEnum status) {
        val dfm = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val partitions = Lists.<String[]> newArrayList();
        partitionValues.forEach(value -> {
            partitions.add(new String[] { value });
        });
        long startTime = SegmentRange.dateToLong(start);
        long endTime = SegmentRange.dateToLong(end);
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);
        val df = dfm.getDataflow(modelId);
        val newSegment = dfm.appendSegment(df, segmentRange, status, partitions);
        newSegment.getMultiPartitions().forEach(partition -> {
            partition.setStatus(PartitionStatusEnum.READY);
        });
        return newSegment;
    }

    private NDataLayout generateLayoutForMultiPartition(String modelId, String segmentId, List<String> partitionValues,
            long layoutId) {
        val dfm = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);

        val model = modelManager.getDataModelDesc(modelId);
        val df = dfm.getDataflow(modelId);
        val partitions = Lists.<String[]> newArrayList();
        partitionValues.forEach(value -> {
            partitions.add(new String[] { value });
        });
        val partitionIds = model.getMultiPartitionDesc().getPartitionIdsByValues(partitions);
        NDataLayout layout = NDataLayout.newDataLayout(df, segmentId, layoutId);
        partitionIds.forEach(id -> {
            layout.getMultiPartition().add(new LayoutPartition(id));
        });
        return layout;
    }

    private interface Functions {
        void process();
    }

}
