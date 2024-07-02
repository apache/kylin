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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.service.JobInfoService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class JobControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private JobService jobService;

    @Mock
    private JobInfoService jobInfoService;

    @InjectMocks
    private final JobControllerV2 jobControllerV2 = Mockito.spy(new JobControllerV2());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(jobControllerV2).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void tesResume() throws Exception {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        ExecutableResponse response = new ExecutableResponse();
        Mockito.when(jobInfoService.getJobInstance(jobId)).thenReturn(response);
        Mockito.when(jobInfoService.manageJob(jobId, response, JobActionEnum.RESUME.toString()))
                .thenReturn(new ExecutableResponse());

        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/jobs/{jobId}/resume", jobId).contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).resume(jobId);
    }

    @Test
    public void testGetJobs() throws Exception {
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(JobStatusEnum.NEW), jobNames, 4, "", "", false,
                "default", "job_name", false);
        Mockito.when(jobInfoService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "0").param("pageSize", "10")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).getJobList(new Integer[] { 0 }, 1, "", "default", null, 0, 10, "last_modified",
                null, true);
    }

    @Test
    public void testGetJobsWithoutProjectAndSortby() throws Exception {
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, null, null, false, null, "job_name",
                true);
        Mockito.when(jobInfoService.listJobs(jobFilter, 0, Integer.MAX_VALUE)).thenReturn(jobs);
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON).param("timeFilter", "4")
                        .param("sortby", "job_name").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).getJobList(new Integer[0], 4, null, null, null, 0, 10, "last_modified",
                "job_name", true);
    }

    @Test
    public void testGetJob() throws Exception {
        mockJobUpdateRequest();
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/{jobId}", jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).getJob(jobId);
    }

    @Test
    public void testGetJobOutput() throws Exception {
        mockJobUpdateRequest();
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        Mockito.when(jobInfoService.getProjectByJobId(jobId)).thenReturn("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/{job_id:.+}/steps/{step_id:.+}/output", jobId, jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).getJobOutput(jobId, jobId);
    }

    private JobUpdateRequest mockJobUpdateRequest() {
        JobUpdateRequest jobUpdateRequest = new JobUpdateRequest();
        jobUpdateRequest.setProject("default");
        jobUpdateRequest.setAction("RESUME");
        jobUpdateRequest.setJobIds(Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"));
        return jobUpdateRequest;
    }

    @Test
    public void testGetJobsException_pageOffset_pageSize() throws Exception {
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(JobStatusEnum.NEW), jobNames, 4, "", "", false,
                "default", "job_name", false);
        Mockito.when(jobInfoService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "a").param("pageSize", "10")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "-1").param("pageSize", "10")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "1").param("pageSize", "-1")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "1").param("pageSize", "a")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "1").param("pageSize", "10")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

    }

}
