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

package org.apache.kylin.rest.controller2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "jobs")
public class JobControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(JobControllerV2.class);

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    /**
     * get all cube jobs
     * 
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listV2(@RequestHeader("Accept-Language") String lang, @RequestParam(value = "status", required = false) Integer[] status, @RequestParam(value = "timeFilter", required = true) Integer timeFilter, @RequestParam(value = "cubeName", required = false) String cubeName, @RequestParam(value = "projectName", required = false) String projectName, @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {
        MsgPicker.setMsg(lang);

        HashMap<String, Object> data = new HashMap<String, Object>();
        List<JobStatusEnum> statusList = new ArrayList<JobStatusEnum>();

        if (null != status) {
            for (int sta : status) {
                statusList.add(JobStatusEnum.getByCode(sta));
            }
        }

        List<JobInstance> jobInstanceList = jobService.searchJobs(cubeName, projectName, statusList, JobTimeFilterEnum.getByCode(timeFilter));

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (jobInstanceList.size() <= offset) {
            offset = jobInstanceList.size();
            limit = 0;
        }

        if ((jobInstanceList.size() - offset) < limit) {
            limit = jobInstanceList.size() - offset;
        }

        data.put("jobs", jobInstanceList.subList(offset, offset + limit));
        data.put("size", jobInstanceList.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Get a cube job
     * 
     * @return
     * @throws JobException 
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId) {
        MsgPicker.setMsg(lang);

        JobInstance jobInstance = jobService.getJobInstance(jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobInstance, "");
    }

    /**
     * Get a job step output
     * 
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}/steps/{stepId}/output", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getStepOutputV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId, @PathVariable String stepId) {
        MsgPicker.setMsg(lang);

        Map<String, String> result = new HashMap<String, String>();
        result.put("jobId", jobId);
        result.put("stepId", String.valueOf(stepId));
        result.put("cmd_output", jobService.getExecutableManager().getOutput(stepId).getVerboseMsg());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    /**
     * Resume a cube job
     * 
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}/resume", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse resumeV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId) {
        MsgPicker.setMsg(lang);

        final JobInstance jobInstance = jobService.getJobInstance(jobId);
        jobService.resumeJob(jobInstance);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobInstance(jobId), "");
    }

    /**
     * Cancel/discard a job
     * 
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}/cancel", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cancelV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId) throws IOException {
        MsgPicker.setMsg(lang);

        final JobInstance jobInstance = jobService.getJobInstance(jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.cancelJob(jobInstance), "");
    }

    /**
     * Pause a job
     *
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}/pause", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse pauseV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId) {
        MsgPicker.setMsg(lang);

        final JobInstance jobInstance = jobService.getJobInstance(jobId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.pauseJob(jobInstance), "");
    }

    /**
     * Rollback a job to the given step
     *
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}/steps/{stepId}/rollback", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rollbackV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId, @PathVariable String stepId) {
        MsgPicker.setMsg(lang);

        final JobInstance jobInstance = jobService.getJobInstance(jobId);
        jobService.rollbackJob(jobInstance, stepId);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobInstance(jobId), "");
    }

    /**
     * Drop a cube job
     *
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{jobId}/drop", method = { RequestMethod.DELETE }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse dropJobV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId) throws IOException {
        MsgPicker.setMsg(lang);

        JobInstance jobInstance = jobService.getJobInstance(jobId);
        jobService.dropJob(jobInstance);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobInstance, "");
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

}
