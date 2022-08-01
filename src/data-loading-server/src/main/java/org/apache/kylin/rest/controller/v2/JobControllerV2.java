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

package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.controller.BaseController;
import org.apache.kylin.rest.request.JobFilter;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.service.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/jobs", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class JobControllerV2 extends BaseController {

    private static final String JOB_ID_ARG_NAME = "jobId";

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    public AclEvaluate aclEvaluate;

    @ApiOperation(value = "resume", tags = { "DW" })
    @PutMapping(value = "/{jobId}/resume")
    @ResponseBody
    public EnvelopeResponse<ExecutableResponse> resume(@PathVariable(value = "jobId") String jobId) throws IOException {
        checkRequiredArg(JOB_ID_ARG_NAME, jobId);
        final ExecutableResponse jobInstance = jobService.getJobInstance(jobId);
        aclEvaluate.checkProjectOperationPermission(jobInstance.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                jobService.manageJob(jobInstance.getProject(), jobInstance, JobActionEnum.RESUME.toString()), "");
    }

    @ApiOperation(value = "getJobList", tags = { "DW" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse getJobList(
            @RequestParam(value = "status", required = false, defaultValue = "") Integer[] status,
            @RequestParam(value = "timeFilter") Integer timeFilter,
            @RequestParam(value = "jobName", required = false) String jobName,
            @RequestParam(value = "projectName") String project,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        List<String> statuses = Lists.newArrayList();
        for (Integer code : status) {
            JobStatusEnum jobStatus = JobStatusEnum.getByCode(code);
            if (Objects.isNull(jobStatus)) {
                jobService.checkJobStatus(String.valueOf(code));
                continue;
            }
            statuses.add(jobStatus.toString());
        }

        JobFilter jobFilter = new JobFilter(statuses,
                Objects.isNull(jobName) ? Lists.newArrayList() : Lists.newArrayList(jobName), timeFilter, null, key,
                project, sortBy, reverse);
        List<ExecutableResponse> executables = jobService.listJobs(jobFilter);
        executables = jobService.addOldParams(executables);
        Map<String, Object> result = getDataResponse("jobs", executables, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }
}
