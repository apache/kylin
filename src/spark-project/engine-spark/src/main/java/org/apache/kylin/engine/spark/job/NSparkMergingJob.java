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
import static org.apache.kylin.job.factory.JobFactoryConstant.MERGE_JOB_FACTORY;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSparkMergingJob extends DefaultExecutableOnModel {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingJob.class);

    static {
        JobFactory.register(MERGE_JOB_FACTORY, new NSparkMergingJob.MergingJobFactory());
    }

    static class MergingJobFactory extends JobFactory {

        private MergingJobFactory() {
        }

        @Override
        protected NSparkMergingJob create(JobBuildParams jobBuildParams) {
            if (jobBuildParams.getSegments() == null || jobBuildParams.getSegments().size() != 1) {
                return null;
            }
            return merge(jobBuildParams.getSegments().iterator().next(), jobBuildParams.getLayouts(),
                    jobBuildParams.getSubmitter(), jobBuildParams.getJobId(), jobBuildParams.getPartitions(),
                    jobBuildParams.getBuckets());
        }
    }

    public NSparkMergingJob() {
        super();
    }

    public NSparkMergingJob(Object notSetId) {
        super(notSetId);
    }

    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<LayoutEntity> layouts, String submitter,
            String jobId) {
        return merge(mergedSegment, layouts, submitter, jobId, null, null);
    }

    /**
     * Merge the segments that are contained in the given mergedSegment
     *
     * @param mergedSegment, new segment that expect to merge, which should contains a couple of ready segments.
     * @param layouts,       user is allowed to specify the cuboids to merge. By default, it is null and merge all
     *                       the ready cuboids in the segments.
     */
    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<LayoutEntity> layouts, String submitter,
            String jobId, Set<Long> partitions, Set<JobBucket> buckets) {
        Preconditions.checkArgument(mergedSegment != null);
        Preconditions.checkArgument(submitter != null);

        NDataflow df = mergedSegment.getDataflow();
        if (layouts == null) {
            layouts = Sets.newHashSet(df.getIndexPlan().getAllLayouts());
        }

        NSparkMergingJob job = new NSparkMergingJob();
        job.setName(JobTypeEnum.INDEX_MERGE.toString());
        job.setJobType(JobTypeEnum.INDEX_MERGE);
        job.setId(jobId);
        job.setTargetSubject(mergedSegment.getModel().getUuid());
        job.setTargetSegments(Lists.newArrayList(String.valueOf(mergedSegment.getId())));
        job.setProject(mergedSegment.getProject());
        job.setSubmitter(submitter);

        if (CollectionUtils.isNotEmpty(partitions)) {
            job.setTargetPartitions(partitions);
            job.setParam(NBatchConstants.P_PARTITION_IDS,
                    job.getTargetPartitions().stream().map(String::valueOf).collect(joining(",")));
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
        job.setParam(NBatchConstants.P_DATA_RANGE_START, mergedSegment.getSegRange().getStart().toString());
        job.setParam(NBatchConstants.P_DATA_RANGE_END, mergedSegment.getSegRange().getEnd().toString());

        KylinConfig config = df.getConfig();
        StepEnum.RESOURCE_DETECT.create(job, config);
        StepEnum.MERGING.create(job, config);
        AbstractExecutable cleanStep = StepEnum.CLEANUP_AFTER_MERGE.create(job, config);
        final Segments<NDataSegment> mergingSegments = df.getMergingSegments(mergedSegment);
        cleanStep.setParam(NBatchConstants.P_SEGMENT_IDS,
                String.join(",", NSparkCubingUtil.toSegmentIds(mergingSegments)));
        StepEnum.UPDATE_METADATA.create(job, config);
        if (config.isIndexPreloadCacheEnabled()) {
            StepEnum.LOAD_GLUTEN_CACHE.create(job, config);
        }
        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return NDataflowManager.getInstance(config, getProject()) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
    }

    public NSparkMergingStep getSparkMergingStep() {
        return getTask(NSparkMergingStep.class);
    }

    public NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    public NSparkCleanupAfterMergeStep getCleanUpAfterMergeStep() {
        return getTask(NSparkCleanupAfterMergeStep.class);
    }

    @Override
    public void cancelJob() {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkMergingStep().getDataflowId());
        if (dataflow == null) {
            logger.debug("Dataflow is null, maybe model is deleted?");
            return;
        }
        List<NDataSegment> toRemovedSegments = new ArrayList<>();
        for (String id : getSparkMergingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && SegmentStatusEnum.READY != segment.getStatus()
                    && SegmentStatusEnum.WARNING != segment.getStatus()) {
                toRemovedSegments.add(segment);
            }
        }
        if (toRemovedSegments.isEmpty()) {
            logger.warn("Segment related to job {} can not be found, maybe job has been canceled.", getJobId());
            return;
        }
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(toRemovedSegments.toArray(new NDataSegment[0]));
        NDataflowManager.getInstance(getConfig(), getProject()).updateDataflow(nDataflowUpdate);
    }
}
