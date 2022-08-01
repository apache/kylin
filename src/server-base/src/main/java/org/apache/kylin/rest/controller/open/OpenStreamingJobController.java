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
package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.metadata.streaming.StreamingJobRecord;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.request.StreamingJobExecuteRequest;
import org.apache.kylin.rest.request.StreamingJobFilter;
import org.apache.kylin.rest.response.StreamingJobDataStatsResponse;
import org.apache.kylin.rest.response.StreamingJobResponse;
import org.apache.kylin.rest.service.StreamingJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/streaming_jobs", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenStreamingJobController extends NBasicController {

    @Autowired
    @Qualifier("streamingJobService")
    private StreamingJobService streamingJobService;

    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<StreamingJobResponse>>> getStreamingJobList(
            @RequestParam(value = "model_name", required = false, defaultValue = "") String modelName,
            @RequestParam(value = "model_names", required = false) List<String> modelNames,
            @RequestParam(value = "job_types", required = false, defaultValue = "") List<String> jobTypes,
            @RequestParam(value = "statuses", required = false, defaultValue = "") List<String> statuses,
            @RequestParam(value = "project", required = false, defaultValue = "") String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse,
            @RequestParam(value = "job_ids", required = false, defaultValue = "") List<String> jobIds) {
        checkStreamingEnabled();
        StreamingJobFilter jobFilter = new StreamingJobFilter(modelName, modelNames, jobTypes, statuses, project,
                sortBy, reverse, jobIds);
        val data = streamingJobService.getStreamingJobList(jobFilter, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    @ApiOperation(value = "updateStreamingJobStatus", notes = "Update Body: jobId")
    @PutMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<String> updateStreamingJobStatus(
            @RequestBody StreamingJobExecuteRequest streamingJobExecuteRequest) {
        checkStreamingEnabled();
        checkRequiredArg("action", streamingJobExecuteRequest.getAction());
        if (CollectionUtils.isEmpty(streamingJobExecuteRequest.getJobIds())) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, "job_ids");
        }
        streamingJobService.updateStreamingJobStatus(streamingJobExecuteRequest.getProject(),
                streamingJobExecuteRequest.getJobIds(), streamingJobExecuteRequest.getAction());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/stats/{jobId:.+}")
    @ResponseBody
    public EnvelopeResponse<StreamingJobDataStatsResponse> getStreamingJobDataStats(
            @PathVariable(value = "jobId") String jobId,
            @RequestParam(value = "time_filter", required = false, defaultValue = "30") Integer timeFilter) {
        checkStreamingEnabled();
        val response = streamingJobService.getStreamingJobDataStats(jobId, timeFilter);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @GetMapping(value = "/records")
    @ResponseBody
    public EnvelopeResponse<List<StreamingJobRecord>> getStreamingJobRecordList(
            @RequestParam(value = "job_id") String jobId) {
        checkStreamingEnabled();
        if (StringUtils.isEmpty(jobId)) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, "job_id");
        }
        val data = streamingJobService.getStreamingJobRecordList(jobId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }
}
