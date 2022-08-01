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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.request.JobErrorRequest;
import org.apache.kylin.rest.request.JobFilter;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.request.SparkJobTimeRequest;
import org.apache.kylin.rest.request.SparkJobUpdateRequest;
import org.apache.kylin.rest.request.StageRequest;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.service.JobService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import lombok.val;

public class JobControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private JobService jobService;

    @InjectMocks
    private final JobController jobController = Mockito.spy(new JobController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(jobController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetJobs() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        List<String> statuses = Lists.newArrayList("NEW", "RUNNING");
        JobFilter jobFilter = new JobFilter(statuses, jobNames, 4, "", "", "default", "job_name", false);
        Mockito.when(jobService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("page_offset", "0").param("page_size", "10")
                .param("time_filter", "1").param("subject", "").param("key", "").param("job_names", "")
                .param("statuses", "NEW,RUNNING").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobList(statuses, jobNames, 1, "", "", "default", 0, 10, "last_modified",
                true);
    }

    @Test
    public void testGetWaitingJobs() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/waiting_jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("model", "test_model")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getWaitingJobs("default", "test_model", 0, 10);
    }

    @Test
    public void testGetWaitingJobsModels() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/jobs/waiting_jobs/models").contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getWaitingJobsInfoGroupByModel("default");
    }

    @Test
    public void testDropJob() throws Exception {
        Mockito.doNothing().when(jobService).batchDropJob("default",
                Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"), Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs").param("project", "default")
                .param("job_ids", "e1ad7bb0-522e-456a-859d-2eab1df448de").param("statuses", "")
                .param("project_all_jobs", "false").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).dropJob("default", Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"),
                Lists.newArrayList());
    }

    @Test
    public void testDropGlobalJob() throws Exception {
        Mockito.doNothing().when(jobService)
                .batchDropGlobalJob(Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"), Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs")
                .param("job_ids", "e1ad7bb0-522e-456a-859d-2eab1df448de").param("statuses", "")
                .param("project_all_jobs", "false").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).dropJob(null, Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"),
                Lists.newArrayList());
    }

    @Test
    public void testDropJob_selectNoneJob_exception() throws Exception {
        Mockito.doNothing().when(jobService).batchDropJob("default", Lists.newArrayList(), Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs").param("project", "default").param("job_ids", "")
                .param("statuses", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).dropJob("default", Lists.newArrayList(), Lists.newArrayList());
    }

    @Test
    public void testUpdateJobStatus_PASS() throws Exception {
        val request = mockJobUpdateRequest();
        Mockito.doNothing().when(jobService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), "default",
                "RESUME", mockJobUpdateRequest().getStatuses());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testUpdateGlobalJobStatus_PASS() throws Exception {
        val request = mockJobUpdateRequest();
        request.setProject(null);
        Mockito.doNothing().when(jobService).batchUpdateGlobalJobStatus(mockJobUpdateRequest().getJobIds(), "RESUME",
                mockJobUpdateRequest().getStatuses());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testUpdateJobStatus_selectNoneJob_Exception() throws Exception {
        val request = mockJobUpdateRequest();
        request.setJobIds(Lists.newArrayList());
        Mockito.doNothing().when(jobService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), "default",
                "RESUME", mockJobUpdateRequest().getStatuses());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testGetJobDetail() throws Exception {
        Mockito.when(jobService.getJobDetail("default", "e1ad7bb0-522e-456a-859d-2eab1df448de"))
                .thenReturn(mockStepsResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/{job}/detail", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .param("job_id", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobDetail("e1ad7bb0-522e-456a-859d-2eab1df448de", "default");
    }

    private List<ExecutableStepResponse> mockStepsResponse() {
        List<ExecutableStepResponse> result = new ArrayList<>();
        result.add(new ExecutableStepResponse());
        result.add(new ExecutableStepResponse());
        return result;
    }

    private JobUpdateRequest mockJobUpdateRequest() {
        JobUpdateRequest jobUpdateRequest = new JobUpdateRequest();
        jobUpdateRequest.setProject("default");
        jobUpdateRequest.setAction("RESUME");
        jobUpdateRequest.setJobIds(Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"));
        return jobUpdateRequest;
    }

    @Test
    public void testGetJobOverallStats() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/statistics").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start_time", String.valueOf(Long.MIN_VALUE))
                .param("end_time", String.valueOf(Long.MAX_VALUE))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void testGetJobCount() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/statistics/count").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start_time", String.valueOf(Long.MIN_VALUE))
                .param("end_time", String.valueOf(Long.MAX_VALUE)).param("dimension", "model")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobCount("default", Long.MIN_VALUE, Long.MAX_VALUE, "model");
    }

    @Test
    public void testGetJobDurationPerMb() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/statistics/duration_per_byte")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .param("start_time", String.valueOf(Long.MIN_VALUE)).param("end_time", String.valueOf(Long.MAX_VALUE))
                .param("dimension", "model").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobDurationPerByte("default", Long.MIN_VALUE, Long.MAX_VALUE, "model");
    }

    @Test
    public void testGetJobOutput() throws Exception {
        mockJobUpdateRequest();
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/jobs/{jobId}/steps/{stepId}/output", "e1ad7bb0-522e-456a-859d-2eab1df448de",
                        "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobOutput("e1ad7bb0-522e-456a-859d-2eab1df448de",
                "e1ad7bb0-522e-456a-859d-2eab1df448de", "default");
    }

    @Test
    public void testUpdateSparkJobInfo() throws Exception {
        SparkJobUpdateRequest request = new SparkJobUpdateRequest();
        request.setProject("default");
        request.setJobId("b");
        request.setTaskId("c");
        request.setYarnAppUrl("url");
        request.setYarnAppId("app_id");
        Mockito.doNothing().when(jobService).updateSparkJobInfo(request.getProject(), request.getJobId(),
                request.getTaskId(), request.getYarnAppId(), request.getYarnAppUrl());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/spark").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).updateSparkJobInfo(request);
    }

    @Test
    public void testUpdateJobError() throws Exception {
        JobErrorRequest request = new JobErrorRequest();
        request.setProject("default");
        request.setJobId("b");
        request.setFailedStepId("c");
        request.setFailedSegmentId("d");
        request.setFailedStack("error");
        request.setFailedReason("reason");

        Mockito.doNothing().when(jobService).updateJobError(request.getProject(), request.getJobId(),
                request.getFailedStepId(), request.getFailedSegmentId(), request.getFailedStack(),
                request.getFailedReason());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/error") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).updateJobError(request);
    }

    @Test
    public void testUpdateStageStatus() throws Exception {
        StageRequest request = new StageRequest();
        request.setProject("default");
        request.setSegmentId("b");
        request.setTaskId("c");
        request.setStatus("RUNNING");
        Mockito.doNothing().when(jobService).updateStageStatus(request.getProject(), request.getTaskId(),
                request.getSegmentId(), request.getStatus(), request.getUpdateInfo(), request.getErrMsg());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).updateStageStatus(request);

        request = new StageRequest();
        request.setProject("");
        request.setSegmentId("b");
        request.setTaskId("");
        request.setStatus("RUNNING");
        Mockito.doNothing().when(jobService).updateStageStatus(request.getProject(), request.getTaskId(),
                request.getSegmentId(), request.getStatus(), request.getUpdateInfo(), request.getErrMsg());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(jobController).updateStageStatus(request);
    }

    @Test
    public void testUpdateSparkJobTime() throws Exception {
        SparkJobTimeRequest request = new SparkJobTimeRequest();
        request.setProject("default");
        request.setJobId("b");
        request.setTaskId("c");
        request.setYarnJobWaitTime("2");
        request.setYarnJobRunTime("1");
        Mockito.doNothing().when(jobService).updateSparkTimeInfo(request.getProject(), request.getJobId(),
                request.getTaskId(), request.getYarnJobWaitTime(), request.getYarnJobRunTime());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/wait_and_run_time")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).updateSparkJobTime(request);
    }
}
