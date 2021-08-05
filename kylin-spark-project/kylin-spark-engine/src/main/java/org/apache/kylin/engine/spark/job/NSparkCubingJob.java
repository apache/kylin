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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class NSparkCubingJob extends CubingJob {

    // KEYS of Output.extraInfo map, info passed across job steps
    public static final String SOURCE_RECORD_COUNT = "sourceRecordCount";
    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);
    private static final String DEPLOY_ENV_NAME = "envName";
    private CubeInstance cube;

    // for test use only
    public static NSparkCubingJob create(Set<CubeSegment> segments, String submitter) {
        return create(segments, submitter, CubingJobTypeEnum.BUILD, UUID.randomUUID().toString());
    }

    public static NSparkCubingJob create(Set<CubeSegment> segments, String submitter, CubingJobTypeEnum jobType,
            String jobId) {
        Preconditions.checkArgument(!segments.isEmpty());
        Preconditions.checkArgument(submitter != null);
        NSparkCubingJob job = new NSparkCubingJob();
        job.cube = segments.iterator().next().getCubeInstance();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        format.setTimeZone(TimeZone.getTimeZone(job.cube.getConfig().getTimeZone()));
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        StringBuilder builder = new StringBuilder();
        builder.append(jobType).append(" CUBE - ");
        for (CubeSegment segment : segments) {
            startTime = startTime < (long) (segment.getSegRange().start.v) ? startTime
                    : (long) (segment.getSegRange().start.v);
            endTime = endTime > (long) (segment.getSegRange().end.v) ? endTime : (long) (segment.getSegRange().end.v);
            builder.append(segment.getCubeInstance().getDisplayName()).append(" - ").append(segment.getName())
                    .append(" - ");
        }
        builder.append(format.format(new Date(System.currentTimeMillis())));
        job.setId(jobId);
        job.setName(builder.toString());
        job.setProjectName(job.cube.getProject());
        job.setTargetSubject(job.cube.getModel().getId());
        job.setTargetSegments(segments.stream().map(x -> String.valueOf(x.getUuid())).collect(Collectors.toList()));
        job.setProject(job.cube.getProject());
        job.setSubmitter(submitter);
        job.setParam(CubingExecutableUtil.SEGMENT_ID,
                segments.stream().map(x -> String.valueOf(x.getUuid())).collect(Collectors.joining(" ")));
        job.setParam(MetadataConstants.P_JOB_ID, jobId);
        job.setParam(MetadataConstants.SEGMENT_NAME, segments.iterator().next().getName());
        job.setParam(MetadataConstants.P_PROJECT_NAME, job.cube.getProject());
        job.setParam(MetadataConstants.P_CUBE_NAME, job.cube.getName());
        job.setParam(MetadataConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(MetadataConstants.P_CUBE_ID, job.cube.getId());
        job.setParam(MetadataConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(MetadataConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        job.setParam(MetadataConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        job.setParam(MetadataConstants.P_OUTPUT_META_URL, job.cube.getConfig().getMetadataUrl().toString());
        job.setParam(MetadataConstants.P_CUBOID_NUMBER, String.valueOf(job.cube.getDescriptor().getAllCuboids().size()));

        //set param for job metrics
        job.setParam(MetadataConstants.P_JOB_TYPE, jobType.toString());
        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, job.cube);
        JobStepFactory.addStep(job, JobStepType.CUBING, job.cube);

        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCubeByUuid(cubeId);
        return MetaDumpUtil.collectCubeMetadata(cubeInstance);
    }

    public String getDeployEnvName() {
        return getParam(DEPLOY_ENV_NAME);
    }

    public long findSourceRecordCount() {
        return Long.parseLong(findExtraInfo(SOURCE_RECORD_COUNT, "0"));
    }

    public long getMapReduceWaitTime() {
        return getExtraInfoAsLong(MAP_REDUCE_WAIT_TIME, 0L);
    }

    public NSparkCubingStep getSparkCubingStep() {
        return getTask(NSparkCubingStep.class);
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    public CubeInstance getCube() {
        return cube;
    }

    public void setCube(CubeInstance cube) {
        this.cube = cube;
    }

    public void cleanupAfterJobDiscard(String segmentName, String segmentIdentifier) {
        try {
            PathManager.deleteJobTempPath(getConfig(), getParam(MetadataConstants.P_PROJECT_NAME),
                    getParam(MetadataConstants.P_JOB_ID));

            CubeManager cubeManager = CubeManager.getInstance(getConfig());
            CubeInstance cube = cubeManager.getCube(getParam(MetadataConstants.P_CUBE_NAME));
            PathManager.deleteSegmentParquetStoragePath(cube, segmentName, segmentIdentifier);
        } catch (IOException e) {
            logger.warn("Delete resource file failed after job be discarded, due to", e);
        }
    }
}
