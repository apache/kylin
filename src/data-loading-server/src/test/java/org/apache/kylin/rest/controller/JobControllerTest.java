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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.JobErrorRequest;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.request.LoadGlutenCacheRequest;
import org.apache.kylin.rest.request.SparkJobTimeRequest;
import org.apache.kylin.rest.request.SparkJobUpdateRequest;
import org.apache.kylin.rest.request.StageRequest;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.service.JobInfoService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.RouteService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.sparkproject.guava.collect.Sets;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;
import lombok.var;

public class JobControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    private JobInfoDao jobInfoDao;

    @Mock
    private JobService jobService;
    @Mock
    private RouteService routeService;

    @Mock
    private JobInfoService jobInfoService;

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

        JobContextUtil.cleanUp();
        JobContext jobContext = JobContextUtil.getJobContext(getTestConfig());
        ReflectionTestUtils.setField(jobController, "jobContext", jobContext);

        jobInfoDao = JobContextUtil.getJobInfoDao(getTestConfig());

        ReflectionTestUtils.setField(jobController, "routeService", routeService);
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testGetJobs() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        List<String> types = Lists.newArrayList();
        List<JobStatusEnum> statuses = Lists.newArrayList(JobStatusEnum.NEW, JobStatusEnum.RUNNING);
        JobFilter jobFilter = new JobFilter(statuses, jobNames, 4, "", "", false, "default", "job_name", false);
        Mockito.when(jobInfoService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("page_offset", "0").param("page_size", "10")
                .param("time_filter", "1").param("subject", "").param("key", "").param("job_names", "")
                .param("statuses", "NEW,RUNNING").param("type", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).getJobList(statuses.stream().map(Enum::name).collect(Collectors.toList()),
                jobNames, 1, "", "", false, "default", 0, 10, "last_modified", true, types);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("time_filter", "1").param("job_names", "")
                .param("type", "test1,INDEX_BUILD").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
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
        Mockito.doNothing().when(jobInfoService).batchDropJob("default",
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
        Mockito.doNothing().when(jobInfoService)
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
        Mockito.doNothing().when(jobInfoService).batchDropJob("default", Lists.newArrayList(), Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs").param("project", "default").param("job_ids", "")
                .param("statuses", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).dropJob("default", Lists.newArrayList(), Lists.newArrayList());
    }

    @Test
    public void testUpdateJobStatus_PASS() throws Exception {
        val request = mockJobUpdateRequest();
        Mockito.doNothing().when(jobInfoService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), "default",
                "RESUME", mockJobUpdateRequest().getStatuses());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).updateJobStatus(Mockito.any(JobUpdateRequest.class),
                Mockito.any(HttpHeaders.class));
    }

    @Test
    public void testUpdateGlobalJobStatus_PASS() throws Exception {
        val request = mockJobUpdateRequest();
        request.setProject(null);
        Mockito.doNothing().when(jobInfoService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), null,
                "RESUME", mockJobUpdateRequest().getStatuses());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).updateJobStatus(Mockito.any(JobUpdateRequest.class),
                Mockito.any(HttpHeaders.class));
    }

    @Test
    public void testUpdateJobStatus_selectNoneJob_Exception() throws Exception {
        val request = mockJobUpdateRequest();
        request.setJobIds(Lists.newArrayList());
        Mockito.doNothing().when(jobInfoService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), "default",
                "RESUME", mockJobUpdateRequest().getStatuses());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(jobController).updateJobStatus(Mockito.any(JobUpdateRequest.class),
                Mockito.any(HttpHeaders.class));
    }

    @Test
    public void testGetJobDetail() throws Exception {
        Mockito.when(jobInfoService.getJobDetail("default", "e1ad7bb0-522e-456a-859d-2eab1df448de"))
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
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        SparkJobUpdateRequest request = new SparkJobUpdateRequest();
        request.setJobLastRunningStartTime(String.valueOf(job.getOutput().getLastRunningStartTime()));
        request.setProject(job.getProject());
        request.setJobId(job.getId());
        request.setTaskId(job.getId() + "_00_00");
        request.setYarnAppUrl("url");
        request.setYarnAppId("app_id");
        request.setCores("1");
        request.setMemory("1024");
        request.setQueueName("queue");
        Mockito.doNothing().when(jobInfoService).updateSparkJobInfo(request);
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/jobs/spark").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
        Assert.assertEquals(response.get("code"), KylinException.CODE_SUCCESS);

        Mockito.verify(jobController).updateSparkJobInfo(request);
    }

    @Test
    public void testUpdateJobError() throws Exception {
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        JobErrorRequest request = new JobErrorRequest();
        request.setJobLastRunningStartTime(String.valueOf(job.getOutput().getLastRunningStartTime()));
        request.setProject(job.getProject());
        request.setJobId(job.getId());
        request.setFailedStepId("c");
        request.setFailedSegmentId("d");
        request.setFailedStack("error");
        request.setFailedReason("reason");

        Mockito.doNothing().when(jobInfoService).updateJobError(request.getProject(), request.getJobId(),
                request.getFailedStepId(), request.getFailedSegmentId(), request.getFailedStack(),
                request.getFailedReason());

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/error") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
        Assert.assertEquals(response.get("code"), KylinException.CODE_SUCCESS);

        Mockito.verify(jobController).updateJobError(request);
    }

    @Test
    public void testUpdateStageStatus() throws Exception {
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        StageRequest request = new StageRequest();
        request.setProject(job.getProject());
        request.setSegmentId(job.getTargetSegments().get(0));
        request.setTaskId(job.getId() + "_00_00");
        request.setStatus("RUNNING");
        request.setJobLastRunningStartTime(String.valueOf(job.getOutput().getLastRunningStartTime()));
        Mockito.doNothing().when(jobInfoService).updateStageStatus(request.getProject(), request.getTaskId(),
                request.getSegmentId(), request.getStatus(), request.getUpdateInfo(), request.getErrMsg());
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
        Assert.assertEquals(response.get("code"), KylinException.CODE_SUCCESS);

        Mockito.verify(jobController).updateStageStatus(request);

        request = new StageRequest();
        request.setProject("");
        request.setSegmentId(job.getTargetSegments().get(0));
        request.setTaskId("");
        request.setStatus("RUNNING");
        request.setJobLastRunningStartTime(String.valueOf(job.getOutput().getLastRunningStartTime()));
        Mockito.doNothing().when(jobInfoService).updateStageStatus(request.getProject(), request.getTaskId(),
                request.getSegmentId(), request.getStatus(), request.getUpdateInfo(), request.getErrMsg());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(jobController).updateStageStatus(request);
    }

    @Test
    public void testUpdateStageStatusOnRestartJob() throws Exception {
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        StageRequest request = new StageRequest();
        request.setProject(job.getProject());
        request.setSegmentId(job.getTargetSegments().get(0));
        request.setTaskId(job.getId() + "_00_00");
        request.setStatus("RUNNING");
        request.setJobLastRunningStartTime(String.valueOf(System.currentTimeMillis()));
        Mockito.doNothing().when(jobInfoService).updateStageStatus(request.getProject(), request.getTaskId(),
                request.getSegmentId(), request.getStatus(), request.getUpdateInfo(), request.getErrMsg());
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
        Assert.assertEquals(response.get("code"), KylinException.CODE_UNDEFINED);
        Assert.assertEquals(response.get("msg"), "Job has stopped.");

        Mockito.verify(jobController).updateStageStatus(request);
    }

    @Test
    public void testUpdateStageStatusOnStoppedJob() throws Exception {
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        StageRequest request = new StageRequest();
        request.setProject(job.getProject());
        request.setSegmentId(job.getTargetSegments().get(0));
        request.setTaskId(job.getId() + "_00_00");
        request.setStatus("PENDING");
        request.setJobLastRunningStartTime(String.valueOf(System.currentTimeMillis()));
        Mockito.doNothing().when(jobInfoService).updateStageStatus(request.getProject(), request.getTaskId(),
                request.getSegmentId(), request.getStatus(), request.getUpdateInfo(), request.getErrMsg());
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
        Assert.assertEquals(response.get("code"), KylinException.CODE_UNDEFINED);
        Assert.assertEquals(response.get("msg"), "Job has stopped.");

        Mockito.verify(jobController).updateStageStatus(request);
    }

    @Test
    public void testUpdateStageStatusConcurrently() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.job.max-transaction-retry", "10");
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        StageRequest request = new StageRequest();
        request.setProject(job.getProject());
        request.setSegmentId(job.getTargetSegments().get(0));
        request.setTaskId(job.getId() + "_01_01");
        request.setStatus("RUNNING");
        request.setJobLastRunningStartTime(String.valueOf(job.getOutput().getLastRunningStartTime()));

        // call real methods of joInfoService
        ReflectionTestUtils.setField(jobController, "jobInfoService", Mockito.spy(JobInfoService.class));

        AtomicInteger failedCount = new AtomicInteger();
        Runnable runnable = () -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/stage/status") //
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
                Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
                if (!response.get("code").equals(KylinException.CODE_SUCCESS)) {
                    failedCount.incrementAndGet();
                }
            } catch (Exception exception) {
                failedCount.incrementAndGet();
            }
        };

        int repeatTime = 10;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < repeatTime; i++) {
            threads.add(new Thread(runnable));
        }
        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                failedCount.incrementAndGet();
            }
        });

        Mockito.verify(jobController, Mockito.times(repeatTime)).updateStageStatus(request);
        Assert.assertEquals(0, failedCount.get());
    }

    @Test
    public void testUpdateSparkJobTime() throws Exception {
        ExecutablePO job = mockJob(ExecutableState.RUNNING);
        SparkJobTimeRequest request = new SparkJobTimeRequest();
        request.setJobLastRunningStartTime(String.valueOf(job.getOutput().getLastRunningStartTime()));
        request.setProject(job.getProject());
        request.setJobId(job.getId());
        request.setTaskId(job.getId() + "_00_00");
        request.setYarnJobWaitTime("2");
        request.setYarnJobRunTime("1");
        Mockito.doNothing().when(jobInfoService).updateSparkTimeInfo(request.getProject(), request.getJobId(),
                request.getTaskId(), request.getYarnJobWaitTime(), request.getYarnJobRunTime());
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/jobs/wait_and_run_time")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Map<String, String> response = JsonUtil.readValueAsMap(result.getResponse().getContentAsString());
        Assert.assertEquals(response.get("code"), KylinException.CODE_SUCCESS);

        Mockito.verify(jobController).updateSparkJobTime(request);
    }

    @Test
    public void testProfileStartByProject() throws Exception {
        String project = "default";
        String jobStepId = "0cb5ea2e-adfe-be86-a04a-e2d385fd27ad-c11baf56-a593-4c5f-d546-1fa86c2d54ad_01";
        String params = "start,event=cpu";

        Mockito.doNothing().when(jobService).startProfileByProject(project, jobStepId, params);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/jobs/profile/start_project")
                        .contentType(MediaType.APPLICATION_JSON).param("project", project).param("step_id", jobStepId)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).profile(project, jobStepId, params, mvcResult.getRequest());
    }

    @Test
    public void testProfileStopByProject() throws Exception {
        String project = "default";
        String jobStepId = "0cb5ea2e-adfe-be86-a04a-e2d385fd27ad-c11baf56-a593-4c5f-d546-1fa86c2d54ad_01";
        String params = "flamegraph";

        Mockito.doNothing().when(jobService).dumpProfileByProject(project, jobStepId, params, new Pair<>());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/jobs/profile/dump_project")
                        .contentType(MediaType.APPLICATION_JSON).param("project", project).param("step_id", jobStepId)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).stopProfile(project, jobStepId, params, mvcResult.getRequest(),
                mvcResult.getResponse());
    }

    @Test
    public void testProfileStartByYarnAppId() throws Exception {
        String yarnAppId = "application_1554187389076_9296";
        String params = "start,event=cpu";

        Mockito.doNothing().when(jobService).startProfileByYarnAppId(yarnAppId, params);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/jobs/profile/start_appid")
                        .contentType(MediaType.APPLICATION_JSON).param("app_id", yarnAppId)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).profileByYarnAppId(yarnAppId, params, mvcResult.getRequest());
    }

    @Test
    public void testProfileStopByYarnAppId() throws Exception {
        String yarnAppId = "application_1554187389076_9296";
        String params = "flamegraph";

        Mockito.doNothing().when(jobService).dumpProfileByYarnAppId(yarnAppId, params, new Pair<>());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/jobs/profile/dump_appid")
                        .contentType(MediaType.APPLICATION_JSON).param("app_id", yarnAppId)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).stopProfileByYarnAppId(yarnAppId, params, mvcResult.getRequest(),
                mvcResult.getResponse());
    }

    private ExecutablePO mockJob(ExecutableState state) {
        ExecutablePO po = mockJob(RandomUtil.randomUUIDStr(), 0, Long.MAX_VALUE);
        jobInfoDao.updateJob(po.getId(), job -> {
            job.getOutput().setStatus(state.name());
            if (state.equals(ExecutableState.RUNNING)) {
                job.getOutput().setLastRunningStartTime(System.currentTimeMillis());
            }
            return true;
        });
        return ExecutableManager.getInstance(getTestConfig(), "default").getExecutablePO(po.getId());
    }

    private ExecutablePO mockJob(String jobId, long start, long end) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        val oneSeg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(start, end));
        NSparkCubingJob job = NSparkCubingJob.create(new JobFactory.JobBuildParams(Sets.newHashSet(oneSeg),
                Sets.newLinkedHashSet(layouts), "ADMIN", JobTypeEnum.INDEX_BUILD, jobId, null, null, null, null, null));
        ExecutableManager.getInstance(getTestConfig(), "default").addJob(job);
        return ExecutableManager.getInstance(getTestConfig(), "default").getExecutablePO(jobId);
    }

    @Test
    public void routeGlutenCache() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new LoadGlutenCacheRequest("default", Lists.newArrayList("test command"));
        Mockito.when(routeService.routeGlutenCache(request.getCacheCommands(), servletRequest)).thenReturn(true);
        val mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/jobs/gluten_cache") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobController).routeGlutenCache(request, mvcResult.getRequest());
    }
}
