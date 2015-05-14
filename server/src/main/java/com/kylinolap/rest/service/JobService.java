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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.exception.InvalidJobInstanceException;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.request.MetricsRequest;
import com.kylinolap.rest.response.MetricsResponse;

/**
 * @author ysong1
 */
@Component("jobService")
public class JobService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(CubeService.class);

    @Autowired
    private AccessService permissionService;

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

    public List<JobInstance> listAllJobs(String cubeName, String projectName, List<JobStatusEnum> statusList) throws IOException, JobException {
        List<JobInstance> jobs = new ArrayList<JobInstance>();
        jobs.addAll(this.getJobManager().listJobs(cubeName, projectName));

        if (null == jobs || jobs.size() == 0) {
            return jobs;
        }

        List<JobInstance> results = new ArrayList<JobInstance>();

        for (JobInstance job : jobs) {
            if (null != statusList && statusList.size() > 0) {
                for (JobStatusEnum status : statusList) {
                    if (job.getStatus() == status) {
                        results.add(job);
                    }
                }
            } else {
                results.add(job);
            }
        }

        return results;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public String submitJob(CubeInstance cube, long startDate, long endDate, CubeBuildTypeEnum buildType,String submitter) throws IOException, JobException, InvalidJobInstanceException {

        List<JobInstance> jobInstances = this.getJobManager().listJobs(cube.getName(), null);
        for (JobInstance jobInstance : jobInstances) {
            if (jobInstance.getStatus() == JobStatusEnum.PENDING || jobInstance.getStatus() == JobStatusEnum.RUNNING || jobInstance.getStatus() == JobStatusEnum.ERROR) {
                throw new JobException("The cube " + cube.getName() + " has running job(" + jobInstance.getUuid() + ") please discard it and try again.");
            }
        }

        String uuid = null;
        try {
            List<CubeSegment> cubeSegments = this.getCubeManager().allocateSegments(cube, buildType, startDate, endDate);
            List<JobInstance> jobs = Lists.newArrayListWithExpectedSize(cubeSegments.size());
            for (CubeSegment segment : cubeSegments) {
                uuid = segment.getUuid();
                JobInstance job = this.getJobManager().createJob(cube.getName(), segment.getName(), segment.getUuid(), buildType,submitter);
                segment.setLastBuildJobID(uuid);
                jobs.add(job);
            }
            getCubeManager().updateCube(cube);
            for (JobInstance job: jobs) {
                this.getJobManager().submitJob(job);
                permissionService.init(job, null);
                permissionService.inherit(job, cube);
            }
        } catch (CubeIntegrityException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }

        return uuid;
    }

    public JobInstance getJobInstance(String uuid) throws IOException, JobException {
        return this.getJobManager().getJob(uuid);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public void resumeJob(JobInstance job) throws IOException, JobException {
        this.getJobManager().resumeJob(job.getUuid());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public void cancelJob(JobInstance job) throws IOException, JobException, CubeIntegrityException {
        CubeInstance cube = this.getCubeManager().getCube(job.getRelatedCube());
        List<JobInstance> jobs = this.getJobManager().listJobs(cube.getName(), null);
        for (JobInstance jobInstance : jobs) {
            if (jobInstance.getStatus() != JobStatusEnum.DISCARDED && jobInstance.getStatus() != JobStatusEnum.FINISHED && jobInstance.getUuid().equals(job.getUuid())) {
                this.getJobManager().discardJob(jobInstance.getUuid());
            }
        }
    }

    public MetricsResponse calculateMetrics(MetricsRequest request) {
        List<JobInstance> jobs = new ArrayList<JobInstance>();

        try {
            jobs.addAll(getJobManager().listJobs(null, null));
        } catch (IOException e) {
            logger.error("", e);
        } catch (JobException e) {
            logger.error("", e);
        }

        MetricsResponse metrics = new MetricsResponse();
        int successCount = 0;
        long totalTime = 0;
        Date startTime = (null == request.getStartTime()) ? new Date(-1) : request.getStartTime();
        Date endTime = (null == request.getEndTime()) ? new Date() : request.getEndTime();

        for (JobInstance job : jobs) {
            if (job.getExecStartTime() > startTime.getTime() && job.getExecStartTime() < endTime.getTime()) {
                metrics.increase("total");
                metrics.increase(job.getStatus().name());

                if (job.getStatus() == JobStatusEnum.FINISHED) {
                    successCount++;
                    totalTime += (job.getExecEndTime() - job.getExecStartTime());
                }
            }
        }

        metrics.increase("aveExecTime", ((successCount == 0) ? 0 : totalTime / (float) successCount));

        return metrics;
    }
}
