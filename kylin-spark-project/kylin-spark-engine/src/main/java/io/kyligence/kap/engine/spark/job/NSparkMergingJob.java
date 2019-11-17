/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;

public class NSparkMergingJob extends DefaultChainedExecutableOnModel {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingJob.class);

    /**
     * Merge the segments that are contained in the given mergedSegment
     *
     * @param mergedSegment, new segment that expect to merge, which should contains a couple of ready segments.
     * @param layouts,       user is allowed to specify the cuboids to merge. By default, it is null and merge all
     *                       the ready cuboids in the segments.
     */
    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<LayoutEntity> layouts, String submitter,
            String jobId) {
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

        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_PROJECT_NAME, df.getProject());
        job.setParam(NBatchConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(NBatchConstants.P_DATAFLOW_ID, df.getId());
        job.setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(layouts)));
        job.setParam(NBatchConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(NBatchConstants.P_DATA_RANGE_START, mergedSegment.getSegRange().getStart().toString());
        job.setParam(NBatchConstants.P_DATA_RANGE_END, mergedSegment.getSegRange().getEnd().toString());

        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, Sets.newHashSet(mergedSegment));
        JobStepFactory.addStep(job, JobStepType.MERGING, Sets.newHashSet(mergedSegment));
        JobStepFactory.addStep(job, JobStepType.CLEAN_UP_AFTER_MERGE, Sets.newHashSet(mergedSegment));

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
        List<NDataSegment> segments = new ArrayList<>();
        NDataSegment segment = dataflow.getSegment(getSparkMergingStep().getSegmentIds().iterator().next());
        if (segment != null && !segment.getStatus().equals(SegmentStatusEnum.READY)) {
            segments.add(segment);
        }
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
        NDefaultScheduler.stopThread(getId());
    }
}
