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

import static java.util.stream.Collectors.joining;
import static org.apache.kylin.job.factory.JobFactoryConstant.CUBE_JOB_FACTORY;
import static org.apache.kylin.engine.spark.stats.utils.HiveTableRefChecker.isNeedCleanUpTransactionalTableJob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.job.JobBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import lombok.val;

/**
 *
 */
public class NSparkCubingJob extends DefaultChainedExecutableOnModel {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);

    static {
        JobFactory.register(CUBE_JOB_FACTORY, new CubingJobFactory());
    }

    public NSparkCubingJob() {
        super();
    }

    public NSparkCubingJob(Object notSetId) {
        super(notSetId);
    }

    @VisibleForTesting
    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter,
            Set<JobBucket> buckets) {
        return create(segments, layouts, submitter, JobTypeEnum.INDEX_BUILD, RandomUtil.randomUUIDStr(), null, null,
                buckets);
    }

    @VisibleForTesting
    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter,
            JobTypeEnum jobType, String jobId, Set<String> ignoredSnapshotTables, Set<Long> partitions,
            Set<JobBucket> buckets) {
        val params = new JobFactory.JobBuildParams(segments, layouts, submitter, jobType, jobId, null,
                ignoredSnapshotTables, partitions, buckets, Maps.newHashMap());
        return innerCreate(params);
    }

    //used for JobFactory
    public static NSparkCubingJob create(JobFactory.JobBuildParams jobBuildParams) {

        NSparkCubingJob sparkCubingJob = innerCreate(jobBuildParams);
        if (CollectionUtils.isNotEmpty(jobBuildParams.getToBeDeletedLayouts())) {
            sparkCubingJob.setParam(NBatchConstants.P_TO_BE_DELETED_LAYOUT_IDS,
                    NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(jobBuildParams.getToBeDeletedLayouts())));
        }
        return sparkCubingJob;
    }

    private static NSparkCubingJob innerCreate(JobFactory.JobBuildParams params) {
        Set<NDataSegment> segments = params.getSegments();
        Set<LayoutEntity> layouts = params.getLayouts();
        String submitter = params.getSubmitter();
        JobTypeEnum jobType = params.getJobType();
        String jobId = params.getJobId();
        Set<String> ignoredSnapshotTables = params.getIgnoredSnapshotTables();
        Set<Long> partitions = params.getPartitions();
        Set<JobBucket> buckets = params.getBuckets();
        Map<String, String> extParams = params.getExtParams();
        Preconditions.checkArgument(!segments.isEmpty());
        Preconditions.checkArgument(submitter != null);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isUTEnv()) {
            Preconditions.checkArgument(!layouts.isEmpty());
        }
        NDataflow df = segments.iterator().next().getDataflow();
        NSparkCubingJob job = new NSparkCubingJob();

        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : segments) {
            startTime = Math.min(startTime, Long.parseLong(segment.getSegRange().getStart().toString()));
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        job.setParams(extParams);
        job.setId(jobId);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setTargetSubject(segments.iterator().next().getModel().getUuid());
        job.setTargetSegments(segments.stream().map(x -> String.valueOf(x.getId())).collect(Collectors.toList()));
        job.setProject(df.getProject());
        job.setSubmitter(submitter);
        if (CollectionUtils.isNotEmpty(partitions)) {
            job.setTargetPartitions(partitions);
            job.setParam(NBatchConstants.P_PARTITION_IDS,
                    job.getTargetPartitions().stream().map(String::valueOf).collect(joining(",")));
            checkIfNeedBuildSnapshots(job);
        }
        if (CollectionUtils.isNotEmpty(buckets)) {
            job.setParam(NBatchConstants.P_BUCKETS, ExecutableParams.toBucketParam(buckets));
        }
        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_PROJECT_NAME, df.getProject());
        job.setParam(NBatchConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(NBatchConstants.P_DATAFLOW_ID, df.getId());
        job.setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(layouts)));
        job.setParam(NBatchConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        job.setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(kylinConfig, df.getProject());
        Set<String> excludedTables = ruleManager.getExcludedTables();
        // if excludedTables contains factTable, remove factTable in excludedTables
        val rootFactTableName = df.getModel().getRootFactTableName();
        excludedTables.remove(rootFactTableName);
        job.setParam(NBatchConstants.P_EXCLUDED_TABLES, String.join(",", excludedTables));
        if (CollectionUtils.isNotEmpty(ignoredSnapshotTables)) {
            job.setParam(NBatchConstants.P_IGNORED_SNAPSHOT_TABLES, String.join(",", ignoredSnapshotTables));
        }
        KylinConfigExt config = df.getConfig();

        JobStepType.RESOURCE_DETECT.createStep(job, config);
        JobStepType.CUBING.createStep(job, config);
        JobStepType.UPDATE_METADATA.createStep(job, config);
        if (SecondStorageUtil.isModelEnable(df.getProject(), job.getTargetSubject())) {
            // can't refresh segment when second storage do rebalanced
            if (Objects.equals(jobType, JobTypeEnum.INDEX_REFRESH)) {
                SecondStorageUtil.validateProjectLock(df.getProject(),
                        Collections.singletonList(LockTypeEnum.LOAD.name()));
            }
            boolean hasBaseIndex = layouts.stream().anyMatch(SecondStorageUtil::isBaseTableIndex);
            if (Objects.equals(jobType, JobTypeEnum.INDEX_BUILD) || Objects.equals(jobType, JobTypeEnum.INC_BUILD)) {
                if (hasBaseIndex) {
                    JobStepType.SECOND_STORAGE_EXPORT.createStep(job, config);
                }
            } else if (Objects.equals(jobType, JobTypeEnum.INDEX_REFRESH) && hasBaseIndex) {
                val oldSegs = job.getTargetSegments().stream().map(segId -> {
                    val curSeg = df.getSegment(segId);
                    return Objects.requireNonNull(df.getSegments().stream()
                            .filter(seg -> seg.getSegRange().equals(curSeg.getSegRange()) && !seg.getId().equals(segId))
                            .findFirst().orElse(null)).getId();
                }).collect(Collectors.toList());
                job.setParam(SecondStorageConstants.P_OLD_SEGMENT_IDS, String.join(",", oldSegs));
                JobStepType.SECOND_STORAGE_REFRESH.createStep(job, config);
            }
        }

        Boolean isRangePartitionTable = df.getModel().getAllTableRefs().stream()
                .anyMatch(tableRef -> tableRef.getTableDesc().isRangePartition());
        Boolean isTransactionalTable = df.getModel().getAllTableRefs().stream()
                .anyMatch(tableRef -> tableRef.getTableDesc().isTransactional());

        if (isNeedCleanUpTransactionalTableJob(isTransactionalTable, isRangePartitionTable,
                kylinConfig.isReadTransactionalTableEnabled())) {
            JobStepType.CLEAN_UP_TRANSACTIONAL_TABLE.createStep(job, config);
        }
        return job;
    }

    public static void checkIfNeedBuildSnapshots(NSparkCubingJob job) {
        switch (job.getJobType()) {
        case INC_BUILD:
        case INDEX_REFRESH:
        case INDEX_BUILD:
            job.setParam(NBatchConstants.P_NEED_BUILD_SNAPSHOTS, "true");
            break;
        default:
            job.setParam(NBatchConstants.P_NEED_BUILD_SNAPSHOTS, "false");
            break;
        }
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return NDataflowManager.getInstance(config, getProject()) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
    }

    public NSparkCubingStep getSparkCubingStep() {
        return getTask(NSparkCubingStep.class);
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    SparkCleanupTransactionalTableStep getCleanIntermediateTableStep() {
        return getTask(SparkCleanupTransactionalTableStep.class);
    }

    @Override
    public void cancelJob() {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkCubingStep().getDataflowId());
        if (dataflow == null) {
            logger.debug("Dataflow is null, maybe model is deleted?");
            return;
        }
        List<NDataSegment> toRemovedSegments = new ArrayList<>();
        for (String id : getSparkCubingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && SegmentStatusEnum.READY != segment.getStatus()
                    && SegmentStatusEnum.WARNING != segment.getStatus()) {
                toRemovedSegments.add(segment);
            }
        }
        NDataSegment[] nDataSegments = toRemovedSegments.toArray(new NDataSegment[0]);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
        updatePartitionOnCancelJob();
    }

    public void updatePartitionOnCancelJob() {
        if (!isBucketJob()) {
            return;
        }
        NDataflowManager dfManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow df = dfManager.getDataflow(getSparkCubingStep().getDataflowId()).copy();
        Set<String> segmentIds = getSparkCubingStep().getSegmentIds();
        Set<Long> partitions = getSparkCubingStep().getTargetPartitions();
        switch (getJobType()) {
        case SUB_PARTITION_BUILD:
            for (String id : segmentIds) {
                NDataSegment segment = df.getSegment(id);
                if (segment == null) {
                    continue;
                }
                // remove partition in layouts
                dfManager.removeLayoutPartition(df.getId(), partitions, Sets.newHashSet(segment.getId()));
                // remove partition in segments
                dfManager.removeSegmentPartition(df.getId(), partitions, Sets.newHashSet(segment.getId()));
                logger.info(String.format(Locale.ROOT, "Remove partitions [%s] in segment [%s] cause to cancel job.",
                        partitions, id));
            }
            break;
        case SUB_PARTITION_REFRESH:
            for (String id : segmentIds) {
                NDataSegment segment = df.getSegment(id);
                if (segment == null) {
                    continue;
                }
                segment.getMultiPartitions().forEach(partition -> {
                    if (partitions.contains(partition.getPartitionId())
                            && PartitionStatusEnum.REFRESH == partition.getStatus()) {
                        partition.setStatus(PartitionStatusEnum.READY);
                    }
                });
                val dfUpdate = new NDataflowUpdate(df.getId());
                dfUpdate.setToUpdateSegs(segment);
                dfManager.updateDataflow(dfUpdate);
                logger.info(String.format(Locale.ROOT,
                        "Change partitions [%s] in segment [%s] status to READY cause to cancel job.", partitions, id));
            }
            break;
        default:
            break;
        }
    }

    @Override
    public boolean safetyIfDiscard() {
        if (checkSuicide() || this.getStatus().isFinalState() || this.getJobType() != JobTypeEnum.INC_BUILD) {
            return true;
        }

        val dataflow = NDataflowManager.getInstance(getConfig(), getProject())
                .getDataflow(getSparkCubingStep().getDataflowId());
        val segs = dataflow.getSegments().stream()
                .filter(nDataSegment -> !getTargetSegments().contains(nDataSegment.getId()))
                .collect(Collectors.toList());
        val toDeletedSeg = dataflow.getSegments().stream()
                .filter(nDataSegment -> getTargetSegments().contains(nDataSegment.getId()))
                .collect(Collectors.toList());
        val segHoles = NDataflowManager.getInstance(getConfig(), getProject())
                .calculateHoles(getSparkCubingStep().getDataflowId(), segs);

        for (NDataSegment segHole : segHoles) {
            for (NDataSegment deleteSeg : toDeletedSeg) {
                if (segHole.getSegRange().overlaps(deleteSeg.getSegRange())
                        || segHole.getSegRange().contains(deleteSeg.getSegRange())) {
                    return false;
                }

            }
        }

        return true;
    }

    static class CubingJobFactory extends JobFactory {

        private CubingJobFactory() {
        }

        @Override
        protected NSparkCubingJob create(JobBuildParams jobBuildParams) {
            return NSparkCubingJob.create(jobBuildParams);
        }
    }
}
