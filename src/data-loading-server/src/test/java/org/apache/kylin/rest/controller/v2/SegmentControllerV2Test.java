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
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.request.CubeRebuildRequest;
import org.apache.kylin.rest.request.SegmentMgmtRequest;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.ModelService;
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

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class SegmentControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @Mock
    private ModelBuildService modelBuildService;

    @InjectMocks
    private final SegmentControllerV2 segmentControllerV2 = Mockito.spy(new SegmentControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(segmentControllerV2).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() throws Exception {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private Segments<NDataSegmentResponse> mockSegments() {
        final Segments<NDataSegmentResponse> nDataSegments = new Segments<>();
        NDataSegmentResponse segment = new NDataSegmentResponse();
        segment.setId(RandomUtil.randomUUIDStr());
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private List<NDataModelResponse> mockModels() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final List<NDataModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        model.setProject("default");
        NDataModelResponse modelResponse = new NDataModelResponse(model);
        Assert.assertEquals(model.getCreateTime(), modelResponse.getCreateTime());
        NDataSegmentResponse segmentResponse1 = new NDataSegmentResponse();
        segmentResponse1.setId("seg1");
        segmentResponse1.setName("test_seg1");
        NDataSegmentResponse segmentResponse2 = new NDataSegmentResponse();
        segmentResponse2.setId("seg2");
        segmentResponse2.setName("test_seg2");
        modelResponse.setSegments(Lists.newArrayList(segmentResponse1, segmentResponse2));
        modelResponse.setProject("default");
        modelResponse.setConfig(kylinConfig);
        models.add(modelResponse);
        NDataModel model1 = new NDataModel();
        model1.setUuid("model2");
        NDataModelResponse model2Response = new NDataModelResponse(model1);
        model2Response.setSegments(Lists.newArrayList(segmentResponse1));
        model2Response.setProject("default");
        model2Response.setConfig(kylinConfig);
        models.add(model2Response);
        NDataModel model2 = new NDataModel();
        model2.setUuid("model3");
        NDataModelResponse model3Response = new NDataModelResponse(model2);
        model3Response.setProject("default");
        model3Response.setConfig(kylinConfig);
        models.add(model3Response);
        NDataModel model3 = new NDataModel();
        model3.setUuid("model4");
        NDataModelResponse model4Response = new NDataModelResponse(model3);
        model4Response.setProject("default");
        model4Response.setConfig(kylinConfig);
        models.add(model4Response);

        return models;
    }

    @Test
    public void testRebuild() throws Exception {
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(0));
        String startTime = String.valueOf(0L);
        String endTime = String.valueOf(Long.MAX_VALUE - 1);
        Mockito.doReturn(new JobInfoResponse()).when(modelBuildService).buildSegmentsManually("default", "model1",
                startTime, endTime);

        CubeRebuildRequest rebuildRequest = new CubeRebuildRequest();
        rebuildRequest.setBuildType("BUILD");
        rebuildRequest.setStartTime(0L);
        rebuildRequest.setEndTime(Long.MAX_VALUE - 1);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/rebuild", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(rebuildRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentControllerV2).rebuild(eq("model1"), eq(null), Mockito.any(CubeRebuildRequest.class));
    }

    @Test
    public void testRebuildRefresh() throws Exception {
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(1));
        Mockito.doReturn(Lists.newArrayList(new JobInfoResponse.JobInfo())).when(modelBuildService)
                .refreshSegmentById(new RefreshSegmentParams("default", "model1", new String[] { "seg1", "seg2" }));

        CubeRebuildRequest rebuildRequest = new CubeRebuildRequest();
        rebuildRequest.setBuildType("REFRESH");
        rebuildRequest.setStartTime(0L);
        rebuildRequest.setEndTime(Long.MAX_VALUE - 1);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/rebuild", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(rebuildRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentControllerV2).rebuild(eq("model1"), eq(null), Mockito.any(CubeRebuildRequest.class));
    }

    @Test
    public void testManageSegmentsMerge() throws Exception {
        JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_MERGE.toString(), "");
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(0));
        Mockito.doReturn(jobInfo).when(modelBuildService)
                .mergeSegmentsManually(new MergeSegmentParams("default", "model1", new String[] { "seg1", "seg2" }));

        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("MERGE");
        request.setSegments(Lists.newArrayList("test_seg1", "test_seg2"));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/segments", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentControllerV2).manageSegments(eq("model1"), eq(null),
                Mockito.any(SegmentMgmtRequest.class));
    }

    @Test
    public void testManageSegmentsFresh() throws Exception {
        JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_MERGE.toString(), "");
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(0));
        Mockito.doReturn(Lists.newArrayList(jobInfo)).when(modelBuildService)
                .refreshSegmentById(new RefreshSegmentParams("default", "model1", new String[] { "seg1", "seg2" }));

        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("REFRESH");
        request.setSegments(Lists.newArrayList("test_seg1", "test_seg2"));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/segments", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentControllerV2).manageSegments(eq("model1"), eq(null),
                Mockito.any(SegmentMgmtRequest.class));
    }

    @Test
    public void testManageSegmentsDelete() throws Exception {
        JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_MERGE.toString(), "");
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(0));
        Mockito.doNothing().when(modelService).deleteSegmentById("model1", "default", new String[] { "seg1", "seg2" },
                true);

        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("DROP");
        request.setSegments(Lists.newArrayList("test_seg1", "test_seg2"));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/segments", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentControllerV2).manageSegments(eq("model1"), eq(null),
                Mockito.any(SegmentMgmtRequest.class));
    }

    @Test
    public void testGetHoles() throws Exception {
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(1));

        mockMvc.perform(MockMvcRequestBuilders.get("/api/cubes/{cubeName}/holes", "model1")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(segmentControllerV2).getHoles("model1", null);
    }
}
