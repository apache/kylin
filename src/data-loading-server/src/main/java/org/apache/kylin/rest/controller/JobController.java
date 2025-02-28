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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ID_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_TYPE_ILLEGAL;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.view.LogicalView;
import org.apache.kylin.metadata.view.LogicalViewManager;
import org.apache.kylin.rest.request.JobErrorRequest;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.request.LoadGlutenCacheRequest;
import org.apache.kylin.rest.request.ReplaceMetaRequest;
import org.apache.kylin.rest.request.SparkJobTimeRequest;
import org.apache.kylin.rest.request.SparkJobUpdateRequest;
import org.apache.kylin.rest.request.StageRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.EventResponse;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.service.JobInfoService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.RouteService;
import org.apache.kylin.util.DumpInfo;
import org.apache.kylin.util.MetadataDumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class JobController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger("schedule");
    private static final String JOB_ID_ARG_NAME = "jobId";
    private static final String STEP_ID_ARG_NAME = "stepId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    private JobInfoService jobInfoService;

    @Autowired
    private RouteService routeService;

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
            @RequestParam(value = "exact", required = false, defaultValue = "false") boolean exactMatch,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse,
            @RequestParam(value = "type", required = false) List<String> types) {
        jobInfoService.checkJobStatus(statuses);
        jobNames = CollectionUtils.isEmpty(jobNames) ? Lists.newArrayList() : jobNames;
        types = CollectionUtils.isEmpty(types) ? Lists.newArrayList() : types;
        jobNames.addAll(types);
        if (jobNames.stream().anyMatch(type -> Objects.isNull(JobTypeEnum.getEnumByName(type)))) {
            throw new KylinException(JOB_TYPE_ILLEGAL);
        }
        checkRequiredArg("time_filter", timeFilter);
        if (jobNames.isEmpty()) {
            jobNames = JobTypeEnum.BUILD_JOB_TYPES;
        }
        JobFilter jobFilter = new JobFilter(jobInfoService.parseJobStatus(statuses), jobNames, timeFilter, subject, key,
                exactMatch, project, sortBy, reverse);
        // pageOffset is 1,2,3.... means pageNo
        Integer pageOffsetNew = pageOffset * pageSize;
        List<ExecutableResponse> result = jobInfoService.listJobs(jobFilter, pageOffsetNew, pageSize);
        long count = jobInfoService.countJobs(jobFilter);
        DataResult<List<ExecutableResponse>> executables = new DataResult<>(result, (int) count, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, executables, "");
    }

    @ApiOperation(value = "dropJob dropGlobalJob", tags = {
            "DW" }, notes = "Update URL: {project}; Update Param: project, job_ids")
    @DeleteMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> dropJob(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "job_ids", required = false) List<String> jobIds,
            @RequestParam(value = "statuses", required = false) List<String> statuses) throws IOException {
        jobInfoService.checkJobStatus(statuses);
        if (StringUtils.isBlank(project) && CollectionUtils.isEmpty(jobIds)) {
            throw new KylinException(JOB_ID_EMPTY, "delete");
        }

        if (null != project) {
            jobInfoService.batchDropJob(project, jobIds, statuses);
        } else {
            jobInfoService.batchDropGlobalJob(jobIds, statuses);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJobStatus", tags = { "DW" }, notes = "Update Body: job_ids")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateJobStatus(@RequestBody JobUpdateRequest jobUpdateRequest,
            @RequestHeader HttpHeaders headers) throws IOException {
        checkRequiredArg("action", jobUpdateRequest.getAction());
        jobInfoService.checkJobStatusAndAction(jobUpdateRequest);
        Map<String, List<String>> nodeWithJobs = JobContextUtil
                .splitJobIdsByScheduleInstance(jobUpdateRequest.getJobIds());
        if (needRouteToOtherInstance(nodeWithJobs, jobUpdateRequest.getAction())) {
            return remoteUpdateJobStatus(jobUpdateRequest, headers, nodeWithJobs);
        }
        if (StringUtils.isBlank(jobUpdateRequest.getProject())
                && CollectionUtils.isEmpty(jobUpdateRequest.getJobIds())) {
            throw new KylinException(JOB_ID_EMPTY, jobUpdateRequest.getAction());
        }

        jobInfoService.batchUpdateJobStatus(jobUpdateRequest.getJobIds(), jobUpdateRequest.getProject(),
                jobUpdateRequest.getAction(), jobUpdateRequest.getStatuses());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private EnvelopeResponse<String> remoteUpdateJobStatus(JobUpdateRequest jobUpdateRequest, HttpHeaders headers,
            Map<String, List<String>> nodeWithJobs) throws IOException {
        for (Map.Entry<String, List<String>> entry : nodeWithJobs.entrySet()) {
            jobUpdateRequest.setJobIds(entry.getValue());
            forwardRequestToTargetNode(JsonUtil.writeValueAsBytes(jobUpdateRequest), headers, entry.getKey(),
                    "/jobs/status");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobInfoService.getJobDetail(project, jobId), "");
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
        result.put("cmd_output", jobInfoService.getJobOutput(project, jobId, stepId));
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
        InputStream jobOutput = jobInfoService.getAllJobOutput(projectName, jobId, stepId);
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

        return updateJobInfoWithCheck(request.getProject(), request.getJobId(), request.getJobLastRunningStartTime(),
                () -> jobInfoService.updateJobError(request.getProject(), request.getJobId(), request.getFailedStepId(),
                        request.getFailedSegmentId(), request.getFailedStack(), request.getFailedReason()));
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
        return updateJobInfoWithCheck(stageRequest.getProject(), stageRequest.getTaskId(),
                stageRequest.getJobLastRunningStartTime(),
                () -> jobInfoService.updateStageStatus(stageRequest.getProject(), stageRequest.getTaskId(),
                        stageRequest.getSegmentId(), stageRequest.getStatus(), stageRequest.getUpdateInfo(),
                        stageRequest.getErrMsg()));
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
        return updateJobInfoWithCheck(sparkJobUpdateRequest.getProject(), sparkJobUpdateRequest.getJobId(),
                sparkJobUpdateRequest.getJobLastRunningStartTime(),
                () -> jobInfoService.updateSparkJobInfo(sparkJobUpdateRequest));
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
        return updateJobInfoWithCheck(sparkJobTimeRequest.getProject(), sparkJobTimeRequest.getJobId(),
                sparkJobTimeRequest.getJobLastRunningStartTime(),
                () -> jobInfoService.updateSparkTimeInfo(sparkJobTimeRequest.getProject(),
                        sparkJobTimeRequest.getJobId(), sparkJobTimeRequest.getTaskId(),
                        sparkJobTimeRequest.getYarnJobWaitTime(), sparkJobTimeRequest.getYarnJobRunTime()));
    }

    private EnvelopeResponse<String> updateJobInfoWithCheck(String project, String taskOrJobId,
            String jobLastRunningStartTime, Runnable runner) {
        try {
            JobContextUtil.withTxAndRetry(() -> {
                checkJobStatus(project, taskOrJobId, jobLastRunningStartTime);
                runner.run();
                return true;
            });
        } catch (Exception e) {
            logger.warn("Update job info failed.", e);
            if (!checkJobStatusWithOutException(project, taskOrJobId, jobLastRunningStartTime)) {
                return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", "Job has stopped.");
            }
            return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", "");
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private boolean checkJobStatusWithOutException(String project, String jobOrTaskId, String jobLastRunningStartTime) {
        try {
            checkJobStatus(project, jobOrTaskId, jobLastRunningStartTime);
            return true;
        } catch (IllegalStateException ignored) {
            return false;
        }
    }

    private void checkJobStatus(String project, String jobOrTaskId, String jobLastRunningStartTime) {
        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        String jobId = ExecutableManager.extractJobId(jobOrTaskId);
        AbstractExecutable job = execMgr.getJob(jobId);
        if (job.getOutput().getState() != ExecutableState.RUNNING) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Job's state is : %s instead of %s !",
                    job.getOutput().getState(), ExecutableState.RUNNING));
        }
        if (!String.valueOf(job.getOutput().getLastRunningStartTime()).equals(jobLastRunningStartTime)) {
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "Job's lastRunningStartTime is : %s instead of %s !",
                            job.getOutput().getLastRunningStartTime(), jobLastRunningStartTime));
        }
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

    @PostMapping(value = "/destroy_job_process")
    @ApiOperation(value = "destroyJobProcess", tags = { "DW" })
    @ResponseBody
    public void destroyJobProcess(@RequestParam("project") String project) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).destroyAllProcess();
    }

    @PostMapping(value = "/replace_metadata")
    @ApiOperation(value = "replace_metadata", tags = { "DW" })
    @ResponseBody
    public EnvelopeResponse<String> updateDumpedMetadata(@RequestBody ReplaceMetaRequest request) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Set<String> dumpList = new LinkedHashSet<>();
        String project = request.getProject();
        String modelId = request.getModelId();
        String viewTable = request.getViewTable();
        String distMetaUrl = request.getDistMetaUrl();

        NDataflow df = NDataflowManager.getInstance(config, project).getDataflow(modelId);
        dumpList.addAll(df.collectPrecalculationResource());
        dumpList.addAll(getLogicalViewMetaDumpList(config, project, viewTable, modelId));

        DumpInfo dumpInfo = new DumpInfo(project, distMetaUrl, dumpList, DumpInfo.DumpType.DATA_LOADING);
        MetadataDumpUtil.dumpMetadata(dumpInfo);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private Set<String> getLogicalViewMetaDumpList(KylinConfig config, String project, String viewTable,
                                                   String modelId) {
        Set<String> dumpList = new LinkedHashSet<>();
        if (!config.isDDLLogicalViewEnabled()) {
            return dumpList;
        }
        LogicalViewManager viewManager = LogicalViewManager.getInstance(config);
        if (StringUtils.isNotBlank(modelId)) {
            Set<String> viewsMeta = viewManager.findLogicalViewsInModel(project, modelId).stream()
                    .map(LogicalView::getResourcePath).collect(Collectors.toSet());
            dumpList.addAll(viewsMeta);
        }
        if (StringUtils.isNotBlank(viewTable)) {
            LogicalView logicalView = viewManager.findLogicalViewInProject(project, viewTable);
            if (logicalView != null) {
                dumpList.add(logicalView.getResourcePath());
            }
        }
        return dumpList;
    }

    @ApiOperation(value = "startProfile", tags = { "DW" }, notes = "")
    @GetMapping(value = "/profile/start_project")
    @ResponseBody
    public EnvelopeResponse<String> profile(@RequestParam(value = "project") String project,
            @RequestParam(value = "step_id") String jobStepId,
            @RequestParam(value = "params", defaultValue = "start,event=cpu", required = false) String params,
            HttpServletRequest request) {
        jobService.setResponseLanguage(request);
        checkProjectName(project);
        jobService.startProfileByProject(project, jobStepId, params);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "dumpProfile", tags = { "DW" }, notes = "")
    @GetMapping(value = "/profile/dump_project")
    @ResponseBody
    public EnvelopeResponse<String> stopProfile(@RequestParam(value = "project") String project,
            @RequestParam(value = "step_id") String jobStepId,
            @RequestParam(value = "params", defaultValue = "flamegraph", required = false) String params,
            HttpServletRequest request, HttpServletResponse response) {
        jobService.setResponseLanguage(request);
        checkProjectName(project);
        Pair<InputStream, String> jobOutputAndDownloadFile = new Pair<>();
        jobService.dumpProfileByProject(project, jobStepId, params, jobOutputAndDownloadFile);
        if (jobOutputAndDownloadFile.getFirst() == null || jobOutputAndDownloadFile.getSecond() == null) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "dump profile for ut");
        }
        setDownloadResponse(jobOutputAndDownloadFile.getFirst(), jobOutputAndDownloadFile.getSecond(),
                MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "startProfileByYarnAppId", tags = { "DW" }, notes = "")
    @GetMapping(value = "/profile/start_appid")
    @ResponseBody
    public EnvelopeResponse<String> profileByYarnAppId(@RequestParam(value = "app_id") String yarnAppId,
            @RequestParam(value = "params", defaultValue = "start,event=cpu", required = false) String params,
            HttpServletRequest request) {
        jobService.setResponseLanguage(request);
        jobService.startProfileByYarnAppId(yarnAppId, params);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "dumpProfile", tags = { "DW" }, notes = "")
    @GetMapping(value = "/profile/dump_appid")
    @ResponseBody
    public EnvelopeResponse<String> stopProfileByYarnAppId(@RequestParam(value = "app_id") String yarnAppId,
            @RequestParam(value = "params", defaultValue = "flamegraph", required = false) String params,
            HttpServletRequest request, HttpServletResponse response) {
        jobService.setResponseLanguage(request);
        Pair<InputStream, String> jobOutputAndDownloadFile = new Pair<>();
        jobService.dumpProfileByYarnAppId(yarnAppId, params, jobOutputAndDownloadFile);
        if (jobOutputAndDownloadFile.getFirst() == null || jobOutputAndDownloadFile.getSecond() == null) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "dump profile for ut");
        }
        setDownloadResponse(jobOutputAndDownloadFile.getFirst(), jobOutputAndDownloadFile.getSecond(),
                MediaType.APPLICATION_OCTET_STREAM_VALUE, response);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * RPC Call
     */
    @PostMapping(value = "/gluten_cache")
    @ApiOperation(value = "routeGlutenCache", tags = { "Gluten" })
    @ResponseBody
    public EnvelopeResponse<Boolean> routeGlutenCache(@RequestBody LoadGlutenCacheRequest request,
            HttpServletRequest servletRequest) throws Exception {
        logger.info("routeGlutenCache request is [{}]", request);
        checkProjectName(request.getProject());
        checkCollectionRequiredArg("cache_commands", request.getCacheCommands());
        val result = routeService.routeGlutenCache(request.getCacheCommands(), servletRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }
}
