/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.metadata.cube.model.Cube;
import org.apache.kylin.engine.spark.metadata.cube.model.DataSegment;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

/**
 */
public class NSparkCubingJob extends DefaultChainedExecutable {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);

    private Cube cube;

    // for test use only
    public static NSparkCubingJob create(Set<DataSegment> segments, Set<LayoutEntity> layouts, String submitter) {
        return create(segments, layouts, submitter, JobTypeEnum.INDEX_BUILD, UUID.randomUUID().toString());
    }

    public static NSparkCubingJob create(Set<DataSegment> segments, Set<LayoutEntity> layouts, String submitter,
            JobTypeEnum jobType, String jobId) {
        Preconditions.checkArgument(!segments.isEmpty());
        Preconditions.checkArgument(!layouts.isEmpty());
        Preconditions.checkArgument(submitter != null);
        NSparkCubingJob job = new NSparkCubingJob();
        job.cube = segments.iterator().next().getCube();
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (DataSegment segment : segments) {
            startTime = startTime < Long.parseLong(segment.getSegRange().getStart().toString()) ? startTime
                    : Long.parseLong(segment.getSegRange().getStart().toString());
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        job.setId(jobId);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setTargetSubject(job.cube.getModel().getId());
        job.setTargetSegments(segments.stream().map(x -> String.valueOf(x.getId())).collect(Collectors.toList()));
        job.setProject(job.cube.getProject());
        job.setSubmitter(submitter);

        job.setParam(MetadataConstants.P_JOB_ID, jobId);
        job.setParam(MetadataConstants.P_PROJECT_NAME, job.cube.getProject());
        job.setParam(MetadataConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(MetadataConstants.P_CUBE_ID, job.cube.getId());
        job.setParam(MetadataConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(layouts)));
        job.setParam(MetadataConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(MetadataConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        job.setParam(MetadataConstants.P_DATA_RANGE_END, String.valueOf(endTime));

        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, job.cube);
        JobStepFactory.addStep(job, JobStepType.CUBING, job.cube);
        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
//        return cube.collectPrecalculationResource();
        //TODO: dump metadata first, convert to metadata interface in spark
        return null;
    }

    public NSparkCubingStep getSparkCubingStep() {
        return getTask(NSparkCubingStep.class);
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    public Cube getCube() {
        return cube;
    }

    public void setCube(Cube cube) {
        this.cube = cube;
    }

    /*@Override
    public void cancelJob() {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        Cube dataflow = nDataflowManager.getDataflow(getSparkCubingStep().getDataflowId());
        List<NDataSegment> segments = new ArrayList<>();
        for (String id : getSparkCubingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && !segment.getStatus().equals(SegmentStatusEnum.READY)) {
                segments.add(segment);
            }
        }
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
        NDefaultScheduler.stopThread(getId());
    }*/

}
