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

import java.util.Set;
import java.util.UUID;

import io.kyligence.kap.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import com.google.common.collect.Lists;

public class NSparkMergingJob extends DefaultChainedExecutable {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingJob.class);

    public static NSparkMergingJob merge(CubeSegment mergedSegment, String submitter) {
        return NSparkMergingJob.merge(mergedSegment, submitter, UUID.randomUUID().toString());
    }

    /**
     * Merge the segments that are contained in the given mergedSegment
     *
     * @param mergedSegment, new segment that expect to merge, which should contains a couple of ready segments.
     */
    public static NSparkMergingJob merge(CubeSegment mergedSegment, String submitter, String jobId) {
        Preconditions.checkArgument(mergedSegment != null);
        Preconditions.checkArgument(submitter != null);

        CubeInstance cube = mergedSegment.getCubeInstance();

        NSparkMergingJob job = new NSparkMergingJob();
        job.setName(JobTypeEnum.INDEX_MERGE.toString());
        job.setId(jobId);
        job.setTargetSubject(mergedSegment.getModel().getUuid());
        job.setTargetSegments(Lists.newArrayList(String.valueOf(mergedSegment.getUuid())));
        job.setProject(mergedSegment.getProject());
        job.setSubmitter(submitter);

        job.setParam(MetadataConstants.P_JOB_ID, jobId);
        job.setParam(MetadataConstants.P_PROJECT_NAME, cube.getProject());
        job.setParam(MetadataConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(MetadataConstants.P_CUBE_ID, cube.getId());
        job.setParam(MetadataConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(MetadataConstants.P_DATA_RANGE_START, mergedSegment.getSegRange().start.toString());
        job.setParam(MetadataConstants.P_DATA_RANGE_END, mergedSegment.getSegRange().end.toString());
        job.setParam(MetadataConstants.P_OUTPUT_META_URL, cube.getConfig().getMetadataUrl().toString());
        job.setParam(MetadataConstants.P_JOB_TYPE, String.valueOf(JobTypeEnum.INDEX_MERGE));

        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, cube);
        JobStepFactory.addStep(job, JobStepType.MERGING, cube);
        //JobStepFactory.addStep(job, JobStepType.CLEAN_UP_AFTER_MERGE, cube);

        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCubeByUuid(cubeId);
        return MetaDumpUtil.collectCubeMetadata(cubeInstance);
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

}
