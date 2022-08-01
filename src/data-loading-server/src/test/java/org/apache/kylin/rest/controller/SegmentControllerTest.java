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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.request.BuildIndexRequest;
import org.apache.kylin.rest.request.BuildSegmentsRequest;
import org.apache.kylin.rest.request.IncrementBuildSegmentsRequest;
import org.apache.kylin.rest.request.PartitionsBuildRequest;
import org.apache.kylin.rest.request.PartitionsRefreshRequest;
import org.apache.kylin.rest.request.SegmentFixRequest;
import org.apache.kylin.rest.request.SegmentTimeRequest;
import org.apache.kylin.rest.request.SegmentsRequest;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.ModelSaveCheckResponse;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.response.SegmentPartitionResponse;
import org.apache.kylin.rest.service.FusionModelService;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.apache.kylin.rest.service.params.MergeSegmentParams;
import org.apache.kylin.rest.service.params.RefreshSegmentParams;
import org.junit.After;
import org.junit.Assert;
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

public class SegmentControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @Mock
    private ModelBuildService modelBuildService;

    @Mock
    private FusionModelService fusionModelService;

    @InjectMocks
    private final SegmentController segmentController = Mockito.spy(new SegmentController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(segmentController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetSegments() throws Exception {
        when(modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "432", "2234", "",
                "end_time", true)).thenReturn(mockSegments());
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).param("offset", "0").param("project", "default")
                        .param("limit", "10").param("start", "432").param("end", "2234").param("sort_by", "end_time")
                        .param("reverse", "true").param("status", "")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentController).getSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "", 0, 10,
                "432", "2234", null, null, false, "end_time", true);
    }

    @Test
    public void testDeleteSegmentsAll() throws Exception {
        Mockito.doNothing().when(modelService).purgeModelManually("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .param("project", "default").param("purge", "true")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).deleteSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", true, false,
                null, null);
    }

    @Test
    public void testDeleteSegmentsByIds() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        Mockito.doNothing().when(modelService).deleteSegmentById("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                request.getIds(), false);
        Mockito.doReturn(request.getIds()).when(modelService).convertSegmentIdWithName(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getProject(), request.getIds(), null);
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .param("project", "default").param("purge", "false")
                        .param("ids", "ef5e0663-feba-4ed2-b71c-21958122bbff")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).deleteSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", false,
                false, request.getIds(), null);
    }

    @Test
    public void testRefreshSegmentsById() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo("78847556-2cdb-4b07-b39e-4c29856309aa",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        SegmentsRequest request = mockSegmentRequest();
        Mockito.doAnswer(x -> jobInfos).when(modelBuildService).refreshSegmentById(Mockito.any());
        Mockito.doReturn(request.getIds()).when(modelService).convertSegmentIdWithName(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getProject(), request.getIds(), null);
        String mvcResult = mockMvc
                .perform(MockMvcRequestBuilders
                        .put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn().getResponse().getContentAsString();
        Assert.assertTrue(mvcResult.contains("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        Mockito.verify(segmentController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testMergeSegments() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setType(SegmentsRequest.SegmentsRequestType.MERGE);
        request.setIds(new String[] { "0", "1" });
        Mockito.doAnswer(x -> new JobInfoResponse.JobInfo("0312bcc1-092e-42b1-ab0e-27807cf54f16",
                "79c27a68-343c-4b73-b406-dd5af0add951")).when(modelBuildService).mergeSegmentsManually(Mockito.any());
        Mockito.doReturn(request.getIds()).when(modelService).convertSegmentIdWithName(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getProject(), request.getIds(), null);
        val mvcResult = mockMvc
                .perform(MockMvcRequestBuilders
                        .put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn().getResponse().getContentAsString();
        Assert.assertTrue(mvcResult.contains("79c27a68-343c-4b73-b406-dd5af0add951"));
        Mockito.verify(segmentController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testMergeSegmentsException() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setType(SegmentsRequest.SegmentsRequestType.MERGE);
        Mockito.doReturn(new JobInfoResponse.JobInfo()).when(modelBuildService).mergeSegmentsManually(
                new MergeSegmentParams("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getIds()));
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(segmentController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testRefreshSegmentsByIdException() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setIds(null);
        Mockito.doAnswer(x -> null).when(modelBuildService).refreshSegmentById(
                new RefreshSegmentParams("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getIds()));
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(segmentController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    private SegmentsRequest mockSegmentRequest() {
        SegmentsRequest segmentsRequest = new SegmentsRequest();
        segmentsRequest.setIds(new String[] { "ef5e0663-feba-4ed2-b71c-21958122bbff" });
        segmentsRequest.setProject("default");
        return segmentsRequest;
    }

    @Test
    public void testBuildSegments() throws Exception {
        BuildSegmentsRequest request1 = new BuildSegmentsRequest();
        request1.setProject("default");
        Mockito.doAnswer(x -> null).when(modelBuildService).buildSegmentsManually("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request1))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).buildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildSegmentsRequest.class));

        IncrementBuildSegmentsRequest request2 = new IncrementBuildSegmentsRequest();
        request2.setProject("default");
        request2.setStart("100");
        request2.setEnd("200");
        request2.setPartitionDesc(new PartitionDesc());
        IncrementBuildSegmentParams incrParams = new IncrementBuildSegmentParams("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request2.getStart(), request2.getEnd(),
                request2.getPartitionDesc(), null, request2.getSegmentHoles(), true, null);
        Mockito.doAnswer(x -> null).when(fusionModelService).incrementBuildSegmentsManually(incrParams);
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{model}/model_segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request2))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).incrementBuildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(IncrementBuildSegmentsRequest.class));
    }

    @Test
    public void testBuildSegments_DataRangeEndLessThanStart() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("100");
        request.setEnd("1");
        Mockito.doAnswer(x -> null).when(modelBuildService).buildSegmentsManually("default", "nmodel_basci", "100",
                "1");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(segmentController).buildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildSegmentsRequest.class));
    }

    @Test
    public void testBuildSegments_DataRangeLessThan0() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("-1");
        request.setEnd("1");
        Mockito.doAnswer(x -> null).when(modelBuildService).buildSegmentsManually("default", "nmodel_basci", "-1", "1");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(segmentController).buildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildSegmentsRequest.class));
    }

    @Test
    public void testBuildIndex() throws Exception {
        BuildIndexRequest request = new BuildIndexRequest();
        request.setProject("default");
        Mockito.doAnswer(x -> null).when(modelBuildService).buildSegmentsManually("default", "nmodel_basci", "0",
                "100");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/indices", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).buildIndicesManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildIndexRequest.class));
    }

    @Test
    public void testFixSegmentHole() throws Exception {
        SegmentFixRequest request = new SegmentFixRequest();
        request.setProject("default");
        SegmentTimeRequest timeRequest = new SegmentTimeRequest();
        timeRequest.setEnd("2");
        timeRequest.setStart("1");
        request.setSegmentHoles(Lists.newArrayList(timeRequest));
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segment_holes", "e0e90065-e7c3-49a0-a801-20465ca64799")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).fixSegHoles(eq("e0e90065-e7c3-49a0-a801-20465ca64799"), eq(request));
    }

    @Test
    public void testCheckSegmentHoles() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("0");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/segment/validation", "e0e90065-e7c3-49a0-a801-20465ca64799")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(segmentController).checkSegment(eq("e0e90065-e7c3-49a0-a801-20465ca64799"), eq(request));

    }

    private Segments<NDataSegmentResponse> mockSegments() {
        final Segments<NDataSegmentResponse> nDataSegments = new Segments<>();
        NDataSegmentResponse segment = new NDataSegmentResponse();
        segment.setId(RandomUtil.randomUUIDStr());
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    @Test
    public void testBuildMultiPartition() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo(JobTypeEnum.SUB_PARTITION_BUILD.toString(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        PartitionsBuildRequest param = new PartitionsBuildRequest();
        param.setProject("default");
        param.setSegmentId("73570f31-05a5-448f-973c-44209830dd01");
        param.setSubPartitionValues(Lists.newArrayList());
        param.setBuildAllSubPartitions(false);
        Mockito.doReturn(new ModelSaveCheckResponse()).when(modelService).checkBeforeModelSave(Mockito.any());
        Mockito.doReturn(new JobInfoResponse()).when(modelBuildService).buildSegmentPartitionByValue(param.getProject(),
                "", param.getSegmentId(), param.getSubPartitionValues(), param.isParallelBuildBySegment(),
                param.isBuildAllSubPartitions(), param.getPriority(), param.getYarnQueue(), param.getTag());
        Mockito.doNothing().when(modelService).validateCCType(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/model_segments/multi_partition", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(param))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testRefreshMultiPartition() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo(JobTypeEnum.SUB_PARTITION_REFRESH.toString(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);

        PartitionsRefreshRequest param = new PartitionsRefreshRequest();
        param.setProject("default");
        param.setSegmentId("73570f31-05a5-448f-973c-44209830dd01");

        Mockito.doReturn(response).when(modelBuildService).refreshSegmentPartition(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/model_segments/multi_partition", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(param))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testDeleteMultiPartition() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo(JobTypeEnum.SUB_PARTITION_REFRESH.toString(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        Mockito.doReturn(response).when(modelBuildService).refreshSegmentPartition(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/model_segments/multi_partition")
                .param("model", "89af4ee2-2cdb-4b07-b39e-4c29856309aa").param("project", "default")
                .param("segment", "73570f31-05a5-448f-973c-44209830dd01").param("ids", new String[] { "1", "2" })
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetMultiPartition() throws Exception {
        List<SegmentPartitionResponse> responses = Lists.newArrayList();
        Mockito.doReturn(responses).when(modelService).getSegmentPartitions("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "73570f31-05a5-448f-973c-44209830dd01", Lists.newArrayList(),
                "last_modify_time", true);
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/models/{model}/model_segments/multi_partition", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default").param("model", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("segment_id", "73570f31-05a5-448f-973c-44209830dd01")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
