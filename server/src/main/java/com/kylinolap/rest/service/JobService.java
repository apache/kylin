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
import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import com.google.common.collect.Sets;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.cube.BuildCubeJob;
import com.kylinolap.job2.cube.BuildCubeJobBuilder;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.model.CubeBuildTypeEnum;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.exception.InvalidJobInstanceException;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.exception.InternalErrorException;

/**
 * @author ysong1
 */
@Component("jobService")
public class JobService extends BasicService {

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
        return Lists.newArrayList(FluentIterable.from(listAllCubingJobs(cubeName, projectName, states)).transform(new Function<BuildCubeJob, JobInstance>() {
            @Override
            public JobInstance apply(BuildCubeJob buildCubeJob) {
                return parseToJobInstance(buildCubeJob);
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
    public String submitJob(CubeInstance cube, long startDate, long endDate, CubeBuildTypeEnum buildType, String submitter) throws IOException, JobException, InvalidJobInstanceException {

        final List<BuildCubeJob> buildCubeJobs = listAllCubingJobs(cube.getName(), null, EnumSet.allOf(ExecutableState.class));
        for (BuildCubeJob job : buildCubeJobs) {
            if (job.getStatus() == ExecutableState.READY || job.getStatus() == ExecutableState.RUNNING) {
                throw new JobException("The cube " + cube.getName() + " has running job(" + job.getId() + ") please discard it and try again.");
            }
        }

        String uuid = null;
        try {
            List<CubeSegment> cubeSegments;
            if (buildType == CubeBuildTypeEnum.BUILD) {
                cubeSegments = this.getCubeManager().appendSegments(cube, startDate, endDate);
            } else if (buildType == CubeBuildTypeEnum.MERGE) {
                cubeSegments = this.getCubeManager().mergeSegments(cube, startDate, endDate);
            } else {
                throw new JobException("invalid build type:" + buildType);
            }
            getCubeManager().updateCube(cube);
            for (CubeSegment segment : cubeSegments) {
                uuid = segment.getUuid();
                BuildCubeJobBuilder builder = BuildCubeJobBuilder.newBuilder(new JobEngineConfig(getConfig()), segment);
                getExecutableManager().addJob(builder.build());
                segment.setLastBuildJobID(uuid);
            }
//            for (JobInstance job : jobs) {
//                this.getJobManager().submitJob(job);
//                permissionService.init(job, null);
//                permissionService.inherit(job, cube);
//            }
        } catch (CubeIntegrityException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }

        return uuid;
    }

    public JobInstance getJobInstance(String uuid) throws IOException, JobException {
        return parseToJobInstance(getExecutableManager().getJob(uuid));
    }

    private JobInstance parseToJobInstance(AbstractExecutable job) {
        Preconditions.checkState(job instanceof BuildCubeJob, "illegal job type, id:" + job.getId());
        BuildCubeJob cubeJob = (BuildCubeJob) job;
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setRelatedCube(cubeJob.getCubeName());
        result.setLastModified(cubeJob.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
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
        return result;
    }

    private JobStepStatusEnum parseToJobStepStatus(ExecutableState state) {
        switch (state) {
            case READY:
                return JobStepStatusEnum.PENDING;
            case RUNNING:
                return JobStepStatusEnum.RUNNING;
            case ERROR:
                return JobStepStatusEnum.ERROR;
            case STOPPED:
                return JobStepStatusEnum.PENDING;
            case DISCARDED:
                return JobStepStatusEnum.DISCARDED;
            case SUCCEED:
                return JobStepStatusEnum.FINISHED;
            default:
                throw new RuntimeException("invalid state:" + state);
        }
    }


    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public void resumeJob(JobInstance job) throws IOException, JobException {
        getExecutableManager().updateJobStatus(job.getId(), ExecutableState.READY);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public void cancelJob(JobInstance job) throws IOException, JobException, CubeIntegrityException {
        CubeInstance cube = this.getCubeManager().getCube(job.getRelatedCube());
        for (BuildCubeJob cubeJob: listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING))) {
            getExecutableManager().stopJob(cubeJob.getId());
        }
    }

}
