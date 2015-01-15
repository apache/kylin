/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.service;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.model.CubeBuildTypeEnum;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.common.HadoopShellExecutable;
import com.kylinolap.job.common.MapReduceExecutable;
import com.kylinolap.job.common.ShellExecutable;
import com.kylinolap.job.cube.CubingJob;
import com.kylinolap.job.cube.CubingJobBuilder;
import com.kylinolap.job.execution.ExecutableState;
import com.kylinolap.job.execution.Output;
import com.kylinolap.job.impl.threadpool.AbstractExecutable;
import com.kylinolap.metadata.model.SegmentStatusEnum;
import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.exception.InternalErrorException;

/**
 * @author ysong1
 */
@Component("jobService")
public class JobService extends BasicService {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CubeService.class);

    public List<JobInstance> listAllJobs(final String cubeName, final String projectName, final List<JobStatusEnum> statusList, final Integer limitValue, final Integer offsetValue) throws IOException, JobException {
        Integer limit = (null == limitValue) ? 30 : limitValue;
        Integer offset = (null == offsetValue) ? 0 : offsetValue;
        List<JobInstance> jobs = listAllJobs(cubeName, projectName, statusList);
        Collections.sort(jobs);

        if (jobs.size() <= offset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - offset) < limit) {
            return jobs.subList(offset, jobs.size());
        }

        return jobs.subList(offset, offset + limit);
    }

    public List<JobInstance> listAllJobs(final String cubeName, final String projectName, final List<JobStatusEnum> statusList) {
        return listCubeJobInstance(cubeName, projectName, statusList);
    }

    private List<JobInstance> listCubeJobInstance(final String cubeName, final String projectName, List<JobStatusEnum> statusList) {
        Set<ExecutableState> states;
        if (statusList == null || statusList.isEmpty()) {
            states = EnumSet.allOf(ExecutableState.class);
        } else {
            states = Sets.newHashSet();
            for (JobStatusEnum status : statusList) {
                states.add(parseToExecutableState(status));
            }
        }
        return Lists.newArrayList(FluentIterable.from(listAllCubingJobs(cubeName, projectName, states)).transform(new Function<CubingJob, JobInstance>() {
            @Override
            public JobInstance apply(CubingJob cubingJob) {
                return parseToJobInstance(cubingJob);
            }
        }));
    }

    private ExecutableState parseToExecutableState(JobStatusEnum status) {
        switch (status) {
            case DISCARDED:
                return ExecutableState.DISCARDED;
            case ERROR:
                return ExecutableState.ERROR;
            case FINISHED:
                return ExecutableState.SUCCEED;
            case NEW:
                return ExecutableState.READY;
            case PENDING:
                return ExecutableState.READY;
            case RUNNING:
                return ExecutableState.RUNNING;
            default:
                throw new RuntimeException("illegal status:" + status);
        }
    }


    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public JobInstance submitJob(CubeInstance cube, long startDate, long endDate, CubeBuildTypeEnum buildType, String submitter) throws IOException, JobException {

        final List<CubingJob> cubingJobs = listAllCubingJobs(cube.getName(), null, EnumSet.allOf(ExecutableState.class));
        for (CubingJob job : cubingJobs) {
            if (job.getStatus() == ExecutableState.READY || job.getStatus() == ExecutableState.RUNNING) {
                throw new JobException("The cube " + cube.getName() + " has running job(" + job.getId() + ") please discard it and try again.");
            }
        }

        try {
            CubingJob job;
            CubingJobBuilder builder = CubingJobBuilder.newBuilder().setJobEnginConfig(new JobEngineConfig(getConfig())).setSubmitter(submitter);
            if (buildType == CubeBuildTypeEnum.BUILD) {
                builder.setSegment(getCubeManager().appendSegments(cube, startDate, endDate));
                job = builder.buildJob();
            } else if (buildType == CubeBuildTypeEnum.MERGE) {
                builder.setSegment(getCubeManager().mergeSegments(cube, startDate, endDate));
                job = builder.mergeJob();
            } else {
                throw new JobException("invalid build type:" + buildType);
            }
            getExecutableManager().addJob(job);
            return parseToJobInstance(job);
        } catch (CubeIntegrityException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    public JobInstance getJobInstance(String uuid) throws IOException, JobException {
        return parseToJobInstance(getExecutableManager().getJob(uuid));
    }

    private JobInstance parseToJobInstance(AbstractExecutable job) {
        if (job == null) {
            return null;
        }
        Preconditions.checkState(job instanceof CubingJob, "illegal job type, id:" + job.getId());
        CubingJob cubeJob = (CubingJob) job;
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setRelatedCube(cubeJob.getCubeName());
        result.setRelatedSegment(cubeJob.getSegmentId());
        result.setLastModified(cubeJob.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(parseToJobStatus(job.getStatus()));
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(parseToJobStep(task, i));
        }
        return result;
    }

    private JobInstance.JobStep parseToJobStep(AbstractExecutable task, int i) {
        JobInstance.JobStep result = new JobInstance.JobStep();
        result.setName(task.getName());
        result.setSequenceID(i);
        result.setStatus(parseToJobStepStatus(task.getStatus()));
        final Output output = getExecutableManager().getOutput(task.getId());
        for (Map.Entry<String, String> entry: output.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setExecStartTime(task.getStartTime());
        result.setExecEndTime(task.getEndTime());
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        if (task instanceof MapReduceExecutable) {
            result.setExecCmd(((MapReduceExecutable) task).getMapReduceParams());
            result.setExecWaitTime(((MapReduceExecutable) task).getMapReduceWaitTime()/1000);
        }
        if (task instanceof HadoopShellExecutable) {
            result.setExecCmd(((HadoopShellExecutable) task).getJobParams());
        }
        return result;
    }

    private JobStatusEnum parseToJobStatus(ExecutableState state) {
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
            default:
                throw new RuntimeException("invalid state:" + state);
        }
    }

    private JobStepStatusEnum parseToJobStepStatus(ExecutableState state) {
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
            default:
                throw new RuntimeException("invalid state:" + state);
        }
    }


    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public void resumeJob(String jobId) throws IOException, JobException {
        getExecutableManager().resumeJob(jobId);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public JobInstance cancelJob(String jobId) throws IOException, JobException, CubeIntegrityException {
//        CubeInstance cube = this.getCubeManager().getCube(job.getRelatedCube());
//        for (BuildCubeJob cubeJob: listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING))) {
//            getExecutableManager().stopJob(cubeJob.getId());
//        }
        final JobInstance jobInstance = getJobInstance(jobId);
        final String segmentId = jobInstance.getRelatedSegment();
        CubeInstance cubeInstance = getCubeManager().getCube(jobInstance.getRelatedCube());
        final CubeSegment segment = cubeInstance.getSegmentById(segmentId);
        if (segment.getStatus() == SegmentStatusEnum.NEW) {
            cubeInstance.getSegments().remove(segment);
            getCubeManager().updateCube(cubeInstance);
        }
        getExecutableManager().discardJob(jobId);
        return jobInstance;
    }

}
