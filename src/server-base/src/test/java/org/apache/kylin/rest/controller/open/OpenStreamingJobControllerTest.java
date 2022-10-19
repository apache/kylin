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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.Collections;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.StreamingJobExecuteRequest;
import org.apache.kylin.rest.service.StreamingJobService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;

public class OpenStreamingJobControllerTest extends NLocalFileMetadataTestCase {

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private MockMvc mockMvc;
    @Mock
    private StreamingJobService streamingJobService = Mockito.spy(StreamingJobService.class);
    @InjectMocks
    private OpenStreamingJobController streamingJobController = Mockito.spy(new OpenStreamingJobController());

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(streamingJobController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(streamingJobController, "streamingJobService", streamingJobService);
    }

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetStreamingJobList() throws Exception {
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs")
                .contentType(MediaType.APPLICATION_JSON).param("model_name", StringUtils.EMPTY)
                .param("model_names", StringUtils.EMPTY).param("job_types", StringUtils.EMPTY)
                .param("statuses", StringUtils.EMPTY).param("project", PROJECT).param("page_offset", "0")
                .param("page_size", "10").param("sort_by", "last_modified").param("reverse", "true")
                .accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        MvcResult mvcResult = mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        Mockito.verify(streamingJobController).getStreamingJobList(StringUtils.EMPTY, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, PROJECT, 0, 10, "last_modified", true,
                Collections.EMPTY_LIST);
    }

    @Test
    public void testUpdateStreamingJobStatus() throws Exception {
        val request = new StreamingJobExecuteRequest();
        request.setProject(PROJECT);
        request.setAction("START");
        request.setJobIds(
                Collections.singletonList(StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name())));
        val mockRequestBuilder = MockMvcRequestBuilders.put("/api/streaming_jobs/status")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(streamingJobController).updateStreamingJobStatus(Mockito.any(StreamingJobExecuteRequest.class));
    }

    /**
     * test for empty job_ids parameters
     * @throws Exception
     */
    @Test
    public void testUpdateStreamingJobStatus_EmptyJobIds() throws Exception {
        val request = new StreamingJobExecuteRequest();
        request.setProject(PROJECT);
        request.setAction("START");
        request.setJobIds(Collections.emptyList());
        val mockRequestBuilder = MockMvcRequestBuilders.put("/api/streaming_jobs/status")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        val errMsg = mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andReturn().getResolvedException().getMessage();
        Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("job_ids"), errMsg);
    }

    @Test
    public void testGetStreamingJobDataStats() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs/stats/" + jobId)
                .contentType(MediaType.APPLICATION_JSON).accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobDataStats(jobId, 30);
    }

    @Test
    public void testGetStreamingJobRecordList() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs/records")
                .contentType(MediaType.APPLICATION_JSON).param("job_id", jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON));
        mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobRecordList(jobId);
    }

    @Test
    public void testGetStreamingJobRecordList_EmptyJobId() throws Exception {
        val jobId = "";
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs/records")
                .contentType(MediaType.APPLICATION_JSON).param("job_id", jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON));
        val errMsg = mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andReturn().getResolvedException().getMessage();
        Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("job_id"), errMsg);
    }
}
