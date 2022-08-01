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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ID_EMPTY;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.common.persistence.transaction.UpdateJobStatusEventNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.rest.request.JobErrorRequest;
import org.apache.kylin.rest.request.JobFilter;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.request.SparkJobTimeRequest;
import org.apache.kylin.rest.request.SparkJobUpdateRequest;
import org.apache.kylin.rest.request.StageRequest;
import org.apache.kylin.rest.response.EventResponse;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class JobController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger("schedule");
    private static final String JOB_ID_ARG_NAME = "jobId";
    private static final String STEP_ID_ARG_NAME = "stepId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "getJobList", tags = {
            "DW" }, notes = "Update Param: job_names, time_filter, subject_alias, project_name, page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ExecutableResponse>>> getJobList(
            @RequestParam(value = "statuses", required = false, defaultValue = "") List<String> statuses,
            @RequestParam(value = "job_names", required = false) List<String> jobNames,
            @RequestParam(value = "time_filter") Integer timeFilter,
            @RequestParam(value = "subject", required = false) String subject,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse) {
        jobService.checkJobStatus(statuses);
        checkRequiredArg("time_filter", timeFilter);
        JobFilter jobFilter = new JobFilter(statuses, jobNames, timeFilter, subject, key, project, sortBy, reverse);
        DataResult<List<ExecutableResponse>> executables;
        if (!StringUtils.isEmpty(project)) {
            executables = jobService.listJobs(jobFilter, pageOffset, pageSize);
        } else {
            executables = jobService.listGlobalJobs(jobFilter, pageOffset, pageSize);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, executables, "");
    }

    @ApiOperation(value = "dropJob dropGlobalJob", tags = {
            "DW" }, notes = "Update URL: {project}; Update Param: project, job_ids")
    @DeleteMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> dropJob(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "job_ids", required = false) List<String> jobIds,
            @RequestParam(value = "statuses", required = false) List<String> statuses) throws IOException {
        jobService.checkJobStatus(statuses);
        if (StringUtils.isBlank(project) && CollectionUtils.isEmpty(jobIds)) {
            throw new KylinException(JOB_ID_EMPTY, "delete");
        }

        if (null != project) {
            jobService.batchDropJob(project, jobIds, statuses);
        } else {
            jobService.batchDropGlobalJob(jobIds, statuses);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus", tags = { "DW" }, notes = "Update Body: job_ids")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest) throws IOException {
        checkRequiredArg("action", jobUpdateRequest.getAction());
        jobService.checkJobStatusAndAction(jobUpdateRequest);
        if (StringUtils.isBlank(jobUpdateRequest.getProject())
                && CollectionUtils.isEmpty(jobUpdateRequest.getJobIds())) {
            throw new KylinException(JOB_ID_EMPTY, jobUpdateRequest.getAction());
        }

        if (!StringUtils.isEmpty(jobUpdateRequest.getProject())) {
            jobService.batchUpdateJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getProject(),
                    jobUpdateRequest.getAction(), jobUpdateRequest.getStatuses());
        } else {
            jobService.batchUpdateGlobalJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getAction(),
                    jobUpdateRequest.getStatuses());
            EventBusFactory.getInstance().postAsync(new UpdateJobStatusEventNotifier(jobUpdateRequest.getJobIds(),
                    jobUpdateRequest.getAction(), jobUpdateRequest.getStatuses()));
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getJobDetail", tags = { "DW" }, notes = "Update Param: job_id")
    @GetMapping(value = "/{job_id:.+}/detail")
    @ResponseBody
    public EnvelopeResponse<List<ExecutableStepResponse>> getJobDetail(@PathVariable(value = "job_id") String jobId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobService.getJobDetail(project, jobId), "");
    }

    @ApiOperation(value = "updateJobStatus", tags = {
            "DW" }, notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/output")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getJobOutput(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        Map<String, String> result = new HashMap<>();
        result.put(JOB_ID_ARG_NAME, jobId);
        result.put(STEP_ID_ARG_NAME, stepId);
        result.put("cmd_output", jobService.getJobOutput(project, jobId, stepId));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "downloadLogFile", tags = {
            "DW" }, notes = "Update URL: {job_id}, {step_id}; Update Param: job_id, step_id")
    @GetMapping(value = "/{job_id:.+}/steps/{step_id:.+}/log")
    @ResponseBody
    public EnvelopeResponse<String> downloadLogFile(@PathVariable("job_id") String jobId,
            @PathVariable("step_id") String stepId, @RequestParam(value = "project") String project,
            HttpServletResponse response) {
        final String projectName = checkProjectName(project);
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        checkRequiredArg(STEP_ID_ARG_NAME, stepId);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s.log", projectName, stepId);
        InputStream jobOutput = jobService.getAllJobOutput(projectName, jobId, stepId);
        setDownloadResponse(jobOutput, downloadFilename, MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/statistics")
    @ApiOperation(value = "jobStatistics", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<JobStatisticsResponse> getJobStats(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobService.getJobStats(project, startTime, endTime),
                "");
    }

    @GetMapping(value = "/statistics/count")
    @ApiOperation(value = "jobStatisticsCount", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Integer>> getJobCount(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                jobService.getJobCount(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/statistics/duration_per_byte")
    @ApiOperation(value = "jobDurationCount", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Double>> getJobDurationPerByte(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time") long startTime, @RequestParam(value = "end_time") long endTime,
            @RequestParam(value = "dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                jobService.getJobDurationPerByte(project, startTime, endTime, dimension), "");
    }

    /**
     * RPC Call
     *
     * @param request
     * @return
     */
    @ApiOperation(value = "updateJobError", tags = { "DW" }, notes = "Update Body: job error")
    @PutMapping(value = "error")
    @ResponseBody
    public EnvelopeResponse<String> updateJobError(@RequestBody JobErrorRequest request) {
        if (StringUtils.isBlank(request.getProject()) && StringUtils.isBlank(request.getJobId())) {
            throw new KylinException(JOB_ID_EMPTY, "At least one job should be selected to update stage status");
        }
        checkProjectName(request.getProject());
        logger.info("updateJobError errorRequest is : {}", request);

        jobService.updateJobError(request.getProject(), request.getJobId(), request.getFailedStepId(),
                request.getFailedSegmentId(), request.getFailedStack(), request.getFailedReason());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     *
     * @param stageRequest
     * @return
     */
    @ApiOperation(value = "updateStageStatus", tags = { "DW" }, notes = "Update Body: jobIds(stage ids)")
    @PutMapping(value = "/stage/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStageStatus(@RequestBody StageRequest stageRequest) {
        if (StringUtils.isBlank(stageRequest.getProject()) && StringUtils.isBlank(stageRequest.getTaskId())) {
            throw new KylinException(JOB_ID_EMPTY, "At least one job should be selected to update stage status");
        }
        checkProjectName(stageRequest.getProject());
        logger.info("updateStageStatus stageRequest is : {}", stageRequest);
        jobService.updateStageStatus(stageRequest.getProject(), stageRequest.getTaskId(), stageRequest.getSegmentId(),
                stageRequest.getStatus(), stageRequest.getUpdateInfo(), stageRequest.getErrMsg());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     *
     * @param sparkJobUpdateRequest
     * @return
     */
    @PutMapping(value = "/spark")
    @ApiOperation(value = "updateURL", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<String> updateSparkJobInfo(@RequestBody SparkJobUpdateRequest sparkJobUpdateRequest) {
        checkProjectName(sparkJobUpdateRequest.getProject());
        jobService.updateSparkJobInfo(sparkJobUpdateRequest.getProject(), sparkJobUpdateRequest.getJobId(),
                sparkJobUpdateRequest.getTaskId(), sparkJobUpdateRequest.getYarnAppId(),
                sparkJobUpdateRequest.getYarnAppUrl());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     *
     * @param sparkJobTimeRequest
     * @return
     */
    @PutMapping(value = "/wait_and_run_time")
    @ApiOperation(value = "updateWaitTime", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<String> updateSparkJobTime(@RequestBody SparkJobTimeRequest sparkJobTimeRequest) {
        checkProjectName(sparkJobTimeRequest.getProject());
        jobService.updateSparkTimeInfo(sparkJobTimeRequest.getProject(), sparkJobTimeRequest.getJobId(),
                sparkJobTimeRequest.getTaskId(), sparkJobTimeRequest.getYarnJobWaitTime(),
                sparkJobTimeRequest.getYarnJobRunTime());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @Deprecated
    @ApiOperation(value = "getWaitingJobs", tags = { "DW" }, notes = "Update Response: total_size")
    @GetMapping(value = "/waiting_jobs")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<EventResponse>>> getWaitingJobs(
            @RequestParam(value = "project") String project, @RequestParam(value = "model") String modelId,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(null, offset, limit), "");
    }

    @Deprecated
    @ApiOperation(value = "waitingJobsByModel", tags = { "DW" })
    @GetMapping(value = "/waiting_jobs/models")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getWaitingJobsInfoGroupByModel(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobService.getEventsInfoGroupByModel(project), "");
    }
}
