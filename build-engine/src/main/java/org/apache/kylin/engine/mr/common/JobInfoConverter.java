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

package org.apache.kylin.engine.mr.common;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JobSearchResult;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInfoConverter {
    private static final Logger logger = LoggerFactory.getLogger(JobInfoConverter.class);

    public static JobInstance parseToJobInstanceQuietly(AbstractExecutable job, Map<String, Output> outputs) {
        try {
            if (job instanceof CheckpointExecutable) {
                return parseToJobInstance((CheckpointExecutable)job, outputs);
            } else if (job instanceof CubingJob) {
                return parseToJobInstance((CubingJob)job, outputs);
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("Failed to parse job instance: uuid={}", job, e);
            return null;
        }
    }

    public static JobInstance parseToJobInstance(CubingJob job, Map<String, Output> outputs) {
        if (job == null) {
            logger.warn("job is null.");
            return null;
        }

        Output output = outputs.get(job.getId());
        if (output == null) {
            logger.warn("job output is null.");
            return null;
        }

        CubingJob cubeJob = (CubingJob) job;
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));

        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setProjectName(cubeJob.getProjectName());
        result.setRelatedCube(cube != null ? cube.getName() : CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setDisplayCubeName(cube != null ? cube.getDisplayName() : CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setRelatedSegmentName(CubingExecutableUtil.getSegmentName(cubeJob.getParams()));
        result.setLastModified(output.getLastModified());
        result.setSubmitter(job.getSubmitter());
        result.setUuid(job.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(parseToJobStatus(output.getState()));
        result.setBuildInstance(AbstractExecutable.getBuildInstance(output));
        result.setMrWaiting(AbstractExecutable.getExtraInfoAsLong(output, CubingJob.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
        result.setExecStartTime(AbstractExecutable.getStartTime(output));
        result.setExecEndTime(AbstractExecutable.getEndTime(output));
        result.setExecInterruptTime(AbstractExecutable.getInterruptTime(output));
        result.setDuration(AbstractExecutable.getDuration(result.getExecStartTime(), result.getExecEndTime(),
                result.getExecInterruptTime()) / 1000);
        for (int i = 0; i < job.getTasks().size(); ++i) {
            AbstractExecutable task = job.getTasks().get(i);
            result.addStep(parseToJobStep(task, i, outputs.get(task.getId())));
        }
        return result;
    }

    public static JobInstance parseToJobInstance(CheckpointExecutable job, Map<String, Output> outputs) {
        if (job == null) {
            logger.warn("job is null.");
            return null;
        }

        Output output = outputs.get(job.getId());
        if (output == null) {
            logger.warn("job output is null.");
            return null;
        }

        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setProjectName(job.getProjectName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(job.getParams()));
        result.setDisplayCubeName(CubingExecutableUtil.getCubeName(job.getParams()));
        result.setLastModified(output.getLastModified());
        result.setSubmitter(job.getSubmitter());
        result.setUuid(job.getId());
        result.setType(CubeBuildTypeEnum.CHECKPOINT);
        result.setStatus(parseToJobStatus(output.getState()));
        result.setBuildInstance(AbstractExecutable.getBuildInstance(output));
        result.setExecStartTime(AbstractExecutable.getStartTime(output));
        result.setExecEndTime(AbstractExecutable.getEndTime(output));
        result.setExecInterruptTime(AbstractExecutable.getInterruptTime(output));
        result.setDuration(AbstractExecutable.getDuration(result.getExecStartTime(), result.getExecEndTime(),
                result.getExecInterruptTime()) / 1000);
        for (int i = 0; i < job.getTasks().size(); ++i) {
            AbstractExecutable task = job.getTasks().get(i);
            result.addStep(parseToJobStep(task, i, outputs.get(task.getId())));
        }
        return result;
    }

    public static JobInstance.JobStep parseToJobStep(AbstractExecutable task, int i, Output stepOutput) {
        JobInstance.JobStep result = new JobInstance.JobStep();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(i);

        if (stepOutput == null) {
            logger.warn("Cannot found output for task: id={}", task.getId());
            return result;
        }

        result.setStatus(parseToJobStepStatus(stepOutput.getState()));
        for (Map.Entry<String, String> entry : stepOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        if (task instanceof MapReduceExecutable) {
            result.setExecCmd(((MapReduceExecutable) task).getMapReduceParams());
            result.setExecWaitTime(
                    AbstractExecutable.getExtraInfoAsLong(stepOutput, MapReduceExecutable.MAP_REDUCE_WAIT_TIME, 0L)
                            / 1000);
        }
        if (task instanceof HadoopShellExecutable) {
            result.setExecCmd(((HadoopShellExecutable) task).getJobParams());
        }
        return result;
    }

    public static JobStatusEnum parseToJobStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStatusEnum.PENDING;
        case RUNNING:
            return JobStatusEnum.RUNNING;
        case ERROR:
            return JobStatusEnum.ERROR;
        case DISCARDED:
            return JobStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStatusEnum.FINISHED;
        case STOPPED:
            return JobStatusEnum.STOPPED;
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    public static JobStepStatusEnum parseToJobStepStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStepStatusEnum.PENDING;
        case RUNNING:
            return JobStepStatusEnum.RUNNING;
        case ERROR:
            return JobStepStatusEnum.ERROR;
        case DISCARDED:
            return JobStepStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStepStatusEnum.FINISHED;
        case STOPPED:
            return JobStepStatusEnum.STOPPED;
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    public static JobSearchResult parseToJobSearchResult(DefaultChainedExecutable job, Map<String, ExecutableOutputPO> outputs) {
        if (job == null) {
            logger.warn("job is null.");
            return null;
        }

        ExecutableOutputPO output = outputs.get(job.getId());
        if (output == null) {
            logger.warn("job output is null.");
            return null;
        }

        final JobSearchResult result = new JobSearchResult();

        String cubeName = CubingExecutableUtil.getCubeName(job.getParams());

        if (cubeName == null) {
            cubeName = job.getParam("model_name");
        } else {
            CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
            if (cube != null) {
                cubeName = cube.getDisplayName();
            }
        }
        result.setCubeName(cubeName);
        result.setId(job.getId());
        result.setJobName(job.getName());
        result.setLastModified(output.getLastModified());
        result.setJobStatus(JobInfoConverter.parseToJobStatus(job.getStatus()));
        return result;
    }
}
