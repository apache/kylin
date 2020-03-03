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

import java.util.Set;
import java.util.UUID;

import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
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
