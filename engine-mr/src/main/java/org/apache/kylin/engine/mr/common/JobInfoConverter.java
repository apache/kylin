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

import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;

import com.google.common.base.Preconditions;

public class JobInfoConverter {
    public static JobInstance parseToJobInstance(AbstractExecutable job, Map<String, Output> outputs) {
        if (job == null) {
            return null;
        }
        Preconditions.checkState(job instanceof CubingJob, "illegal job type, id:" + job.getId());
        CubingJob cubeJob = (CubingJob) job;
        Output output = outputs.get(job.getId());
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setLastModified(output.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(parseToJobStatus(output.getState()));
        result.setMrWaiting(AbstractExecutable.getExtraInfoAsLong(output, CubingJob.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
        result.setExecStartTime(AbstractExecutable.getStartTime(output));
        result.setExecEndTime(AbstractExecutable.getEndTime(output));
        result.setDuration(AbstractExecutable.getDuration(result.getExecStartTime(), result.getExecEndTime()) / 1000);
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(parseToJobStep(task, i, outputs.get(task.getId())));
        }
        return result;
    }

    public static JobInstance.JobStep parseToJobStep(AbstractExecutable task, int i, Output stepOutput) {
        Preconditions.checkNotNull(stepOutput);
        JobInstance.JobStep result = new JobInstance.JobStep();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(i);
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
            result.setExecWaitTime(AbstractExecutable.getExtraInfoAsLong(stepOutput, MapReduceExecutable.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
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
}
