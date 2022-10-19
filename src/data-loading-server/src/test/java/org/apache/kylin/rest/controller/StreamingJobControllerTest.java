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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.rest.request.StreamingJobExecuteRequest;
import org.apache.kylin.rest.request.StreamingJobParamsRequest;
import org.apache.kylin.rest.service.StreamingJobService;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.apache.kylin.streaming.request.LayoutUpdateRequest;
import org.apache.kylin.streaming.request.StreamingJobStatsRequest;
import org.apache.kylin.streaming.request.StreamingJobUpdateRequest;
import org.apache.kylin.streaming.request.StreamingSegmentRequest;
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

public class StreamingJobControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private StreamingJobService streamingJobService = Mockito.spy(StreamingJobService.class);

    @InjectMocks
    private StreamingJobController streamingJobController = Mockito.spy(new StreamingJobController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    private static String DATAFLOW_ID = MODEL_ID;

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
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/streaming_jobs").contentType(MediaType.APPLICATION_JSON)
                        .param("model_name", StringUtils.EMPTY).param("model_names", StringUtils.EMPTY)
                        .param("job_types", StringUtils.EMPTY).param("statuses", StringUtils.EMPTY)
                        .param("project", PROJECT).param("page_offset", "0").param("page_size", "10")
                        .param("sort_by", "last_modified").param("reverse", "true")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobList(StringUtils.EMPTY, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, PROJECT, 0, 10, "last_modified", true);
    }

    @Test
    public void testUpdateStreamingJobStatus() throws Exception {
        val request = new StreamingJobExecuteRequest();
        request.setProject(PROJECT);
        request.setAction("START");
        request.setJobIds(Arrays.asList(StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name())));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateStreamingJobStatus(Mockito.any(StreamingJobExecuteRequest.class));
    }

    @Test
    public void testUpdateStreamingJobParams() throws Exception {
        val request = new StreamingJobParamsRequest();
        request.setProject(PROJECT);
        request.setJobId(StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name()));
        request.setParams(new HashMap<>());
        request.getParams().put(StreamingConstants.SPARK_MASTER, StreamingConstants.SPARK_MASTER_DEFAULT);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/params").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateStreamingJobParams(Mockito.any(StreamingJobParamsRequest.class));
    }

    @Test
    public void testGetStreamingJobDataStats() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/streaming_jobs/stats/" + jobId)
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT).param("time_filter", "1")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobDataStats(jobId, PROJECT, 1);
    }

    @Test
    public void testCollectStreamingJobStats() throws Exception {
        val request = new StreamingJobStatsRequest();
        request.setJobExecutionId(0);
        request.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        request.setProject(PROJECT);
        String job_id = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d92_build";
        request.setJobId(job_id);
        Long batch_row_num = 1234532L;
        request.setBatchRowNum(batch_row_num);
        Double rows_per_second = 123.32;
        request.setRowsPerSecond(rows_per_second);
        Long duration_ms = 12222L;
        request.setProcessingTime(duration_ms);
        Long trigger_start_time = 999L;
        request.setTriggerStartTime(trigger_start_time);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/stats").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).collectStreamingJobStats(Mockito.any(StreamingJobStatsRequest.class));
    }

    @Test
    public void testUpdateStreamingJobInfo() throws Exception {
        val request = new StreamingJobUpdateRequest();
        request.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        request.setProject(PROJECT);
        request.setModelId(MODEL_ID);
        request.setProcessId("9921");
        Mockito.when(streamingJobService.updateStreamingJobInfo(Mockito.any())).thenReturn(new StreamingJobMeta());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/spark").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateStreamingJobInfo(Mockito.any(StreamingJobUpdateRequest.class));
    }

    @Test
    public void testGetStreamingJobRecordList() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/streaming_jobs/records")
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT).param("job_id", jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobRecordList(PROJECT, jobId);
    }

    @Test
    public void testGetStreamingModelNameList() throws Exception {
        val modelName = "stream_merge";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/streaming_jobs/model_name")
                .contentType(MediaType.APPLICATION_JSON).param("model_name", modelName).param("project", PROJECT)
                .param("page_size", "20").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingModelNameList(modelName, PROJECT, 20);
    }

    @Test
    public void testGetStreamingModelNameListNonProject() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/streaming_jobs/model_name").contentType(MediaType.APPLICATION_JSON)
                        .param("page_size", "20").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingModelNameList("", "", 20);
    }

    @Test
    public void testAddSegment() throws Exception {
        val request = new StreamingSegmentRequest();
        request.setJobExecutionId(0);
        request.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);
        request.setSegmentRange(new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 1L,
                createKafkaPartitionOffset(0, 100L), createKafkaPartitionOffset(0, 200L)));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/streaming_jobs/dataflow/segment")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).addSegment(Mockito.any(StreamingSegmentRequest.class));
    }

    @Test
    public void testUpdateSegment() throws Exception {
        val request = new StreamingSegmentRequest();
        request.setJobExecutionId(0);
        request.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);
        request.setNewSegId("c380dd2a-43b8-4268-b73d-2a5f76236638");
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        request.setRemoveSegment(df.getSegments());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/dataflow/segment")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateSegment(Mockito.any(StreamingSegmentRequest.class));
    }

    @Test
    public void testDeleteSegment() throws Exception {
        val request = new StreamingSegmentRequest();
        request.setJobExecutionId(0);
        request.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        request.setRemoveSegment(df.getSegments());

        Mockito.doNothing().when(streamingJobService).deleteSegment(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyList());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/streaming_jobs/dataflow/segment/deletion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).deleteSegment(Mockito.any(StreamingSegmentRequest.class));
    }

    @Test
    public void testUpdateLayout() throws Exception {
        val request = new LayoutUpdateRequest();
        request.setJobExecutionId(0);
        request.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);

        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);

        Assert.assertTrue(df.getSegments().size() > 0);
        val segDetails = df.getSegments().getFirstSegment().getSegDetails();
        request.setSegDetails(Arrays.asList(segDetails));
        val layouts = segDetails.getLayouts();
        request.setLayouts(layouts);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/dataflow/layout")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateLayout(Mockito.any(LayoutUpdateRequest.class));

    }

    @Test
    public void testGetStreamingJobDriverLogSimple() throws Exception {
        String job_id = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d92_build";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/streaming_jobs/{job_id}/simple_log", job_id)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(streamingJobController).getStreamingJobDriverLogSimple(job_id, "default");
    }
}
