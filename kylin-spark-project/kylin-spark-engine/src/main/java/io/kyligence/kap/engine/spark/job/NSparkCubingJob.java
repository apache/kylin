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

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.kyligence.kap.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.util.MailNotificationUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

/**
 */
public class NSparkCubingJob extends DefaultChainedExecutable {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);

    private CubeInstance cube;

    // for test use only
    public static NSparkCubingJob create(Set<CubeSegment> segments, String submitter) {
        return create(segments, submitter, JobTypeEnum.INDEX_BUILD, UUID.randomUUID().toString());
    }

    public static NSparkCubingJob create(Set<CubeSegment> segments, String submitter,
            JobTypeEnum jobType, String jobId) {
        Preconditions.checkArgument(!segments.isEmpty());
        Preconditions.checkArgument(submitter != null);
        NSparkCubingJob job = new NSparkCubingJob();
        job.cube = segments.iterator().next().getCubeInstance();
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (CubeSegment segment : segments) {
            startTime = startTime < Long.parseLong(segment.getSegRange().start.toString()) ? startTime
                    : Long.parseLong(segment.getSegRange().start.toString());
            endTime = endTime > Long.parseLong(segment.getSegRange().start.toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().end.toString());
        }
        job.setId(jobId);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setTargetSubject(job.cube.getModel().getId());
        job.setTargetSegments(segments.stream().map(x -> String.valueOf(x.getUuid())).collect(Collectors.toList()));
        job.setProject(job.cube.getProject());
        job.setSubmitter(submitter);

        job.setParam(MetadataConstants.P_JOB_ID, jobId);
        job.setParam(MetadataConstants.P_PROJECT_NAME, job.cube.getProject());
        job.setParam(MetadataConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(MetadataConstants.P_CUBE_ID, job.cube.getId());
        job.setParam(MetadataConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(MetadataConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        job.setParam(MetadataConstants.P_DATA_RANGE_END, String.valueOf(endTime));

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

    // KEYS of Output.extraInfo map, info passed across job steps
    public static final String SOURCE_RECORD_COUNT = "sourceRecordCount";
    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";
    private static final String DEPLOY_ENV_NAME = "envName";

    @Override
    protected Pair<String, String> formatNotifications(ExecutableContext context, ExecutableState state) {
        CubeInstance cubeInstance = CubeManager.getInstance(context.getConfig())
                .getCube(this.getParams().get(CUBE_NAME));
        final Output output = getManager().getOutput(getId());
        state = output.getState();
        if (state != ExecutableState.ERROR
                && !cubeInstance.getDescriptor().getStatusNeedNotify().contains(state.toString())) {
            logger.info("state:" + state + " no need to notify users");
            return null;
        }

        if (!MailNotificationUtil.hasMailNotification(state)) {
            logger.info("Cannot find email template for job state: " + state);
            return null;
        }

        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("job_name", getName());
        dataMap.put("env_name", getDeployEnvName());
        dataMap.put("submitter", StringUtil.noBlank(getSubmitter(), "missing submitter"));
        dataMap.put("job_engine", MailNotificationUtil.getLocalHostName());
        dataMap.put("project_name", cubeInstance.getProject());
        dataMap.put("cube_name", cubeInstance.getName());
        dataMap.put("source_records_count", String.valueOf(findSourceRecordCount()));
        dataMap.put("start_time", new Date(getStartTime()).toString());
        dataMap.put("duration", getDuration() / 60000 + "mins");
        dataMap.put("mr_waiting", getMapReduceWaitTime() / 60000 + "mins");
        dataMap.put("last_update_time", new Date(getLastModified()).toString());

        return super.formatNotifications(context, state);
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
