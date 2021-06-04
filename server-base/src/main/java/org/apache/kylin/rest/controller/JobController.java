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

package org.apache.kylin.rest.controller;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.JobListRequest;
import org.apache.kylin.rest.request.SparkJobUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.PutMapping;

import javax.servlet.http.HttpServletResponse;

@Controller
@RequestMapping(value = "jobs")
public class JobController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    /**
     * get all cube jobs
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<JobInstance> list(JobListRequest jobRequest) {

        List<JobInstance> jobInstanceList;
        List<JobStatusEnum> statusList = new ArrayList<JobStatusEnum>();

        if (null != jobRequest.getStatus()) {
            for (int status : jobRequest.getStatus()) {
                statusList.add(JobStatusEnum.getByCode(status));
            }
        }

        JobTimeFilterEnum timeFilter = null;
        if (null != jobRequest.getTimeFilter()) {
            timeFilter = JobTimeFilterEnum.getByCode(jobRequest.getTimeFilter());
        } else {
            timeFilter = JobTimeFilterEnum.getByCode(KylinConfig.getInstanceFromEnv().getDefaultTimeFilter());
        }

        JobService.JobSearchMode jobSearchMode = JobService.JobSearchMode.CUBING_ONLY;
        if (null != jobRequest.getJobSearchMode()) {
            try {
                jobSearchMode = JobService.JobSearchMode.valueOf(jobRequest.getJobSearchMode());
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid value for JobSearchMode: '" + jobRequest.getJobSearchMode() + "', skip it.");
            }
        }

        try {
            jobInstanceList = jobService.searchJobsV2(jobRequest.getCubeName(), jobRequest.getProjectName(), statusList,
                    jobRequest.getLimit(), jobRequest.getOffset(), timeFilter, jobSearchMode);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
        return jobInstanceList;
    }

    /**
     * get job status overview
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/overview", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public Map<JobStatusEnum, Integer> jobOverview(JobListRequest jobRequest) {
        Map<JobStatusEnum, Integer> jobOverview = new HashMap<>();
        List<JobStatusEnum> statusList = new ArrayList<JobStatusEnum>();
        if (null != jobRequest.getStatus()) {
            for (int status : jobRequest.getStatus()) {
                statusList.add(JobStatusEnum.getByCode(status));
            }
        }

        JobTimeFilterEnum timeFilter = JobTimeFilterEnum.LAST_ONE_WEEK;
        if (null != jobRequest.getTimeFilter()) {
            timeFilter = JobTimeFilterEnum.getByCode(jobRequest.getTimeFilter());
        }

        JobService.JobSearchMode jobSearchMode = JobService.JobSearchMode.CUBING_ONLY;
        if (null != jobRequest.getJobSearchMode()) {
            try {
                jobSearchMode = JobService.JobSearchMode.valueOf(jobRequest.getJobSearchMode());
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid value for JobSearchMode: '" + jobRequest.getJobSearchMode() + "', skip it.");
            }
        }

        try {
            jobOverview = jobService.searchJobsOverview(jobRequest.getCubeName(), jobRequest.getProjectName(), statusList,
                    timeFilter, jobSearchMode);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
        return jobOverview;
    }

    /**
     * Get a cube job
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public JobInstance get(@PathVariable String jobId) {
        JobInstance jobInstance = null;
        try {
            jobInstance = jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

        return jobInstance;
    }

    /**
     * Get a job step output
     */
    @RequestMapping(value = "/{jobId}/steps/{stepId}/output", method = { RequestMethod.GET }, produces = {
            "application/json" })
    @ResponseBody
    public Map<String, String> getStepOutput(@PathVariable String jobId, @PathVariable String stepId) {
        Map<String, String> result = new HashMap<>();
        result.put("jobId", jobId);
        result.put("stepId", String.valueOf(stepId));
        result.put("cmd_output", jobService.getJobStepOutput(jobId, stepId));
        return result;
    }

    /**
     * Download a step output(Spark driver log) from hdfs
     */
    @RequestMapping(value = "/{job_id:.+}/steps/{step_id:.+}/log", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<String> downloadLogFile(@PathVariable("job_id") String jobId,
                                                    @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project,
                                                    HttpServletResponse response) throws IOException {
        checkRequiredArg("job_id", jobId);
        checkRequiredArg("step_id", stepId);
        checkRequiredArg("project", project);
        String validatedPrj =  CliCommandExecutor.checkParameter(project);
        String validatedStepId =  CliCommandExecutor.checkParameter(stepId);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s.log", validatedPrj, validatedStepId);

        String jobOutput = jobService.getAllJobStepOutput(jobId, stepId);
        setDownloadResponse(new ByteArrayInputStream(jobOutput.getBytes(StandardCharsets.UTF_8)), downloadFilename, MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     */
    @PutMapping(value = "/spark")
    @ResponseBody
    public EnvelopeResponse<String> updateSparkJobInfo(@RequestBody SparkJobUpdateRequest sparkJobUpdateRequest) {
        jobService.updateSparkJobInfo(sparkJobUpdateRequest.getProject(),
                sparkJobUpdateRequest.getTaskId(),
                sparkJobUpdateRequest.getYarnAppUrl());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    /**
     * Resume a cube job
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/resume", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance resume(@PathVariable String jobId) {
        try {
            final JobInstance jobInstance = jobService.getJobInstance(jobId);
            if (jobInstance == null) {
                throw new BadRequestException("Cannot find job: " + jobId);
            }
            jobService.resumeJob(jobInstance);
            return jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
    }

//    /**
//     * resubmit streaming segment
//     *
//     * @throws IOException
//     */
//    @RequestMapping(value = "/{jobId}/resubmit", method = { RequestMethod.PUT }, produces = {
//            "application/json" })
//    @ResponseBody
//    public void resubmitJob(@PathVariable String jobId) throws IOException {
//        try {
//            final JobInstance jobInstance = jobService.getJobInstance(jobId);
//            if (jobInstance == null) {
//                throw new BadRequestException("Cannot find job: " + jobId);
//            }
//            jobService.resubmitJob(jobInstance);
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage(), e);
//            throw e;
//        }
//    }

    /**
     * Cancel/discard a job
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/cancel", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance cancel(@PathVariable String jobId) {

        try {
            final JobInstance jobInstance = jobService.getJobInstance(jobId);
            if (jobInstance == null) {
                throw new BadRequestException("Cannot find job: " + jobId);
            }
            jobService.cancelJob(jobInstance);
            return jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
    }

    /**
     * Pause a job
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/pause", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance pause(@PathVariable String jobId) {

        try {
            final JobInstance jobInstance = jobService.getJobInstance(jobId);
            if (jobInstance == null) {
                throw new BadRequestException("Cannot find job: " + jobId);
            }
            jobService.pauseJob(jobInstance);
            return jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

    }

    /**
     * Rollback a job to the given step
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/steps/{stepId}/rollback", method = { RequestMethod.PUT }, produces = {
            "application/json" })
    @ResponseBody
    public JobInstance rollback(@PathVariable String jobId, @PathVariable String stepId) {
        try {
            final JobInstance jobInstance = jobService.getJobInstance(jobId);
            if (jobInstance == null) {
                throw new BadRequestException("Cannot find job: " + jobId);
            }
            jobService.rollbackJob(jobInstance, stepId);
            return jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
    }

    /**
     * Drop a cube job
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/drop", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public JobInstance dropJob(@PathVariable String jobId) {
        JobInstance jobInstance = null;
        try {
            jobInstance = jobService.getJobInstance(jobId);
            if (jobInstance == null) {
                throw new BadRequestException("Cannot find job: " + jobId);
            }
            jobService.dropJob(jobInstance);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

        return jobInstance;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

}
