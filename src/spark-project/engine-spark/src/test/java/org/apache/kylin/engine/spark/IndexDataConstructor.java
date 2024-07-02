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
package org.apache.kylin.engine.spark;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.job.NSparkCubingStep;
import org.apache.kylin.engine.spark.merger.AfterBuildResourceMerger;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataSegmentManager;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.junit.Assert;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

public class IndexDataConstructor {

    private String project;
    private int buildCount = 0;

    public IndexDataConstructor(String project) {
        this.project = project;
    }

    public static ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            ExecutableState status = job.getStatus();
            if (!status.isProgressing()) {
                return status;
            }
        }
    }

    public static boolean wait(List<? extends AbstractExecutable> jobs) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            val isFinished = jobs.stream().map(j -> !j.getStatus().isProgressing()).reduce(true,
                    (left, right) -> left && right);
            if (isFinished) {
                return jobs.stream().map(j -> j.getStatus() == ExecutableState.SUCCEED).reduce(true,
                        (left, right) -> left && right);
            }
        }
    }

    public static String firstFailedJobErrorMessage(ExecutableManager execMgr, ChainedExecutable job) {
        return job.getTasks().stream()
                .filter(abstractExecutable -> abstractExecutable.getStatus() == ExecutableState.ERROR).findFirst()
                .map(task -> execMgr.getOutputFromHDFSByJobId(job.getId(), task.getId(), Integer.MAX_VALUE)
                        .getVerboseMsg())
                .orElse("Unknown Error");
    }

    public void buildDataflow(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        UnitOfWork.doInTransactionWithRetry(
                () -> NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update),
                project);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(round1),
                true);
    }

    public void buildMultiSegmentPartitions(String dfName, String segStart, String segEnd, List<Long> layoutIds,
            List<Long> partitionIds) throws Exception {
        val config = NLocalFileMetadataTestCase.getTestConfig();
        val dfManager = NDataflowManager.getInstance(config, project);
        val df = dfManager.getDataflow(dfName);
        val partitionValues = df.getModel().getMultiPartitionDesc().getPartitionValuesById(partitionIds);

        // append segment
        long start = SegmentRange.dateToLong(segStart);
        long end = SegmentRange.dateToLong(segEnd);
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                () -> NDataflowManager.getInstance(NLocalFileMetadataTestCase.getTestConfig(), project)
                        .appendSegment(df, segmentRange, SegmentStatusEnum.NEW, partitionValues),
                project);

        Set<LayoutEntity> layouts = Sets.newHashSet();
        IndexPlan indexPlan = df.getIndexPlan();
        for (Long id : layoutIds) {
            layouts.add(indexPlan.getLayoutEntity(id));
        }
        buildSegment(dfName, dataSegment, layouts, true, partitionValues);
    }

    public void buildIndex(String dfName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts, boolean isAppend)
            throws Exception {
        buildIndex(dfName, segmentRange, toBuildLayouts, isAppend, null);
    }

    // return segment id
    public String buildIndex(String dfName, SegmentRange segmentRange, Set<LayoutEntity> toBuildLayouts,
            boolean isAppend, List<String[]> partitionValues) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = addSegment(dfName, segmentRange, SegmentStatusEnum.NEW, partitionValues);
        // If cached, will not build target segment, and return the cached segment
        buildSegment(dfName, oneSeg, toBuildLayouts, isAppend, partitionValues);
        Set<NDataSegment> segmentInCache = dsMgr.getDataflow(df.getId()).getSegments().stream()
                .filter(segment -> segment.getSegRange().equals(segmentRange)).collect(Collectors.toSet());
        if (segmentInCache.size() != 1) {
            throw new RuntimeException("Please check cached segment data for build. The environment may be outdated!"
                    + "Please manually delete the cache on the cloud.");
        }
        return segmentInCache.iterator().next().getId();
    }

    public NDataSegment addSegment(String dfName, SegmentRange segmentRange, List<String[]> partitionValues) {
        return addSegment(dfName, segmentRange, SegmentStatusEnum.NEW, partitionValues);
    }

    public NDataSegment addSegment(String dfName, SegmentRange segmentRange, SegmentStatusEnum status,
            List<String[]> partitionValues) {
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dsMgr.getDataflow(dfName);
            return dsMgr.appendSegment(df, segmentRange, status, partitionValues);
        }, project);
    }

    public void cleanSegments(String dfUuid) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dsMgr.getDataflow(dfUuid);
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            return dsMgr.updateDataflow(update);
        }, project);
    }

    public NDataSegment mergeSegment(String dfName, SegmentRange segmentRange, boolean force) {
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dsMgr.getDataflow(dfName);
            return dsMgr.mergeSegments(df, segmentRange, force);
        }, project);
    }

    public <T> T transactionWrap(String project, Callable<T> call) {
        try {
            return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(call::call, project);
        } catch (TransactionException e) {
            if (e.getCause() instanceof KylinException) {
                throw (KylinException) e.getCause();
            }
            throw e;
        }
    }

    public void buildSegment(String dfName, NDataSegment segment, Set<LayoutEntity> toBuildLayouts, boolean isAppend,
            List<String[]> partitionValues) throws InterruptedException {
        buildSegments(Lists.newArrayList(new BuildInfo(dfName, segment, toBuildLayouts, isAppend, partitionValues)));
    }

    public void buildSegments(List<BuildInfo> buildInfos) throws InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val indexDataRepo = new IndexDataWarehouse(config, project, buildCount + "");
        if (indexDataRepo.reuseBuildData()) {
            buildCount++;
            return;
        }
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);

        List<NSparkCubingJob> jobs = Lists.newArrayList();
        for (BuildInfo buildInfo : buildInfos) {
            val dfName = buildInfo.dataflowId;
            val partitionValues = buildInfo.partitionValues;
            val segment = buildInfo.segment;
            val toBuildLayouts = buildInfo.toBuildLayouts;
            Set<JobBucket> buckets = Sets.newHashSet();
            if (CollectionUtils.isNotEmpty(partitionValues)) {
                NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
                Set<Long> targetPartitions = modelManager.getDataModelDesc(dfName).getMultiPartitionDesc()
                        .getPartitionIdsByValues(partitionValues);
                val bucketStart = new AtomicLong(segment.getMaxBucketId());
                toBuildLayouts.forEach(layout -> {
                    targetPartitions.forEach(partition -> {
                        buckets.add(new JobBucket(segment.getUuid(), layout.getId(), bucketStart.incrementAndGet(),
                                partition));
                    });
                });
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                        () -> NDataSegmentManager.getInstance(KylinConfig.getInstanceFromEnv(), project).update(
                                segment.getUuid(), copyForWrite -> copyForWrite.setMaxBucketId(bucketStart.get())),
                        project);
            }
            NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), toBuildLayouts, "ADMIN", buckets);
            if (buildInfo.isAppend) {
                job.setJobType(JobTypeEnum.INC_BUILD);
            } else {
                job.setJobType(JobTypeEnum.INDEX_BUILD);
            }
            NSparkCubingStep sparkStep = job.getSparkCubingStep();
            StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
            Assert.assertEquals("hdfs", distMetaUrl.getScheme());
            Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

            // launch the job
            execMgr.addJob(job);
            jobs.add(job);
        }
        if (!wait(jobs)) {
            throw new IllegalStateException(firstFailedJobErrorMessage(execMgr, jobs.get(0)));
        }
        for (val job : jobs) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                KylinConfig conf = KylinConfig.getInstanceFromEnv();
                val merger = new AfterBuildResourceMerger(conf, project);
                val sparkStep = job.getSparkCubingStep();
                return merger.merge(job.getTargetModelId(), job.getSegmentIds(),
                        ExecutableUtils.getLayoutIds(sparkStep), ExecutableUtils.getRemoteStore(conf, sparkStep),
                        job.getJobType(), job.getTargetPartitions());
            }, project);
        }
        indexDataRepo.persistBuildData();
        buildCount++;
    }

    public void buildMultiPartition(List<BuildInfo> buildInfos) throws InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val indexDataRepo = new IndexDataWarehouse(config, project, buildCount + "");
        if (indexDataRepo.reuseBuildData()) {
            buildCount++;
            return;
        }
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);

        List<NSparkCubingJob> jobs = Lists.newArrayList();
        for (BuildInfo buildInfo : buildInfos) {
            val dfName = buildInfo.dataflowId;
            val partitionValues = buildInfo.partitionValues;
            val segment = buildInfo.segment;
            val toBuildLayouts = buildInfo.toBuildLayouts;
            Set<JobBucket> buckets = Sets.newHashSet();
            if (CollectionUtils.isNotEmpty(partitionValues)) {
                NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
                Set<Long> targetPartitions = modelManager.getDataModelDesc(dfName).getMultiPartitionDesc()
                        .getPartitionIdsByValues(partitionValues);
                val bucketStart = new AtomicLong(segment.getMaxBucketId());
                toBuildLayouts.forEach(layout -> {
                    targetPartitions.forEach(partition -> {
                        buckets.add(new JobBucket(segment.getUuid(), layout.getId(), bucketStart.incrementAndGet(),
                                partition));
                    });
                });
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                        () -> NDataSegmentManager.getInstance(KylinConfig.getInstanceFromEnv(), project).update(
                                segment.getUuid(), copyForWrite -> copyForWrite.setMaxBucketId(bucketStart.get())),
                        project);
            }
            NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), toBuildLayouts, "ADMIN", buckets);
            job.setJobType(JobTypeEnum.SUB_PARTITION_BUILD);
            NSparkCubingStep sparkStep = job.getSparkCubingStep();
            StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
            Assert.assertEquals("hdfs", distMetaUrl.getScheme());
            Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

            // launch the job
            execMgr.addJob(job);
            jobs.add(job);
        }
        if (!wait(jobs)) {
            throw new IllegalStateException(firstFailedJobErrorMessage(execMgr, jobs.get(0)));
        }
        for (val job : jobs) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                KylinConfig conf = KylinConfig.getInstanceFromEnv();
                val merger = new AfterBuildResourceMerger(conf, project);
                val sparkStep = job.getSparkCubingStep();
                return merger.merge(job.getTargetModelId(), job.getSegmentIds(),
                        ExecutableUtils.getLayoutIds(sparkStep), ExecutableUtils.getRemoteStore(conf, sparkStep),
                        job.getJobType(), job.getTargetPartitions());
            }, project);
        }
        indexDataRepo.persistBuildData();
        buildCount++;
    }

    public void buildMultiPartition(String dfName, String segmentId, Set<LayoutEntity> toBuildLayouts, boolean isAppend,
            List<String[]> partitionValues) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            // ready dataflow, segment, cuboid layout
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).appendPartitions(df.getId(),
                    segmentId, partitionValues);
            return null;
        }, project);

        NDataSegment segment = df.getSegment(segmentId);
        buildMultiPartition(
                Lists.newArrayList(new BuildInfo(dfName, segment, toBuildLayouts, isAppend, partitionValues)));
    }

    @Data
    @AllArgsConstructor
    public static class BuildInfo {
        String dataflowId;
        NDataSegment segment;
        Set<LayoutEntity> toBuildLayouts;
        boolean isAppend;
        List<String[]> partitionValues;
    }
}
