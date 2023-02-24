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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.request.ModelCheckRequest;
import org.apache.kylin.rest.request.ModelCloneRequest;
import org.apache.kylin.rest.request.ModelConfigRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.ModelUpdateRequest;
import org.apache.kylin.rest.request.ModelValidationRequest;
import org.apache.kylin.rest.request.MultiPartitionMappingRequest;
import org.apache.kylin.rest.request.OwnerChangeRequest;
import org.apache.kylin.rest.request.UnlinkModelRequest;
import org.apache.kylin.rest.request.UpdateMultiPartitionValueRequest;
import org.apache.kylin.rest.response.IndicesResponse;
import org.apache.kylin.rest.response.ModelConfigResponse;
import org.apache.kylin.rest.response.ModelSaveCheckResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.response.RelatedModelResponse;
import org.apache.kylin.rest.service.FusionModelService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelTdsService;
import org.apache.kylin.tool.bisync.SyncContext;
import org.apache.kylin.tool.bisync.model.SyncModel;
import org.junit.After;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;

public class NModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @Mock
    private ModelTdsService tdsService;

    @Mock
    private FusionModelService fusionModelService;

    @InjectMocks
    private NModelController nModelController = Mockito.spy(new NModelController());

    @InjectMocks
    private NBasicController nBasicController = Mockito.spy(new NBasicController());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
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
    public void testValidateNewModelAlias() throws Exception {
        when(fusionModelService.modelExists("model1", "default")).thenReturn(true);
        val request = new ModelValidationRequest("model1", "default");
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/name/validation", "model1")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).validateNewModelAlias(request);
    }

    @Test
    public void testGetModelSql() throws Exception {
        String sql = "SELECT * FROM TABLE1";
        when(modelService.getModelSql("model1", "default")).thenReturn(sql);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{model}/sql", "model1")
                        .contentType(MediaType.APPLICATION_JSON).param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelSql("model1", "default");
    }

    @Test
    public void testGetModelJson() throws Exception {
        String json = "testjson";
        when(modelService.getModelJson("model1", "default")).thenReturn(json);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{model}/json", "model1")
                        .contentType(MediaType.APPLICATION_JSON).param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelJson("model1", "default");
    }

    @Test
    public void testTableIndices() throws Exception {
        when(modelService.getTableIndices("model1", "default")).thenReturn(mockIndicesResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/table_indices", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getTableIndices("model1", "default");
    }

    @Test
    public void testAggIndices() throws Exception {
        when(modelService.getAggIndices("model1", "default", null, null, false, 0, 10, null, true))
                .thenReturn(mockIndicesResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/agg_indices", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", "model1")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getAggIndices("model1", "default", null, null, false, 0, 10,
                "last_modify_time", true);
    }

    @Test
    public void testGetIndicesById() throws Exception {
        IndexEntity index = new IndexEntity();
        index.setId(432323);
        index.setIndexPlan(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        IndicesResponse indices = new IndicesResponse(index.getIndexPlan());
        when(modelService.getIndicesById("default", "model1", 432323L)).thenReturn(indices);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/agg_indices", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("index", "432323").param("project", "default")
                .param("model", "model1").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getAggIndices("model1", "default", 432323L, null, false, 0, 10,
                "last_modify_time", true);
    }

    @Test
    public void testGetModels() throws Exception {
        List<NDataModelResponse> mockedModels = mockModels();
        // Adding spy models for condition coverage in tests; Mocking cannot be nested so I declare them here.
        NDataModel modelSpy1 = Mockito.spy(new NDataModel());
        when(modelSpy1.getModelType()).thenReturn(NDataModel.ModelType.BATCH);
        mockedModels.add(new NDataModelResponse(modelSpy1));
        NDataModel modelSpy2 = Mockito.spy(new NDataModel());
        when(modelSpy2.getModelType()).thenReturn(NDataModel.ModelType.HYBRID);
        mockedModels.add(new NDataModelResponse(modelSpy2));
        NDataModel modelSpy3 = Mockito.spy(new NDataModel());
        when(modelSpy3.getModelType()).thenReturn(NDataModel.ModelType.STREAMING);
        mockedModels.add(new NDataModelResponse(modelSpy3));
        NDataModelResponse modelSpy4 = Mockito.spy(new NDataModelResponse(new NDataModel()));
        when(modelSpy4.isSecondStorageEnabled()).thenReturn(true);
        mockedModels.add(modelSpy4);

        when(modelService.getModels("model1", "default", true, "ADMIN", Arrays.asList("ONLINE"), "last_modify", true,
                null, null, null)).thenReturn(mockedModels);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model_name", "model1").param("limit", "10")
                .param("exact", "true").param("table", "").param("owner", "ADMIN").param("status", "ONLINE")
                .param("sortBy", "last_modify").param("reverse", "true")
                .param("model_attributes", "BATCH,STREAMING,HYBRID,SECOND_STORAGE")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels(null, "model1", true, "default", "ADMIN", Arrays.asList("ONLINE"),
                "", 0, 10, "last_modify", true, null, Arrays.asList(ModelAttributeEnum.BATCH,
                        ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID, ModelAttributeEnum.SECOND_STORAGE),
                null, null, true, false);
    }

    @Test
    public void testGetRelatedModels() throws Exception {

        when(modelService.getRelateModels("default", "TEST_KYLIN_FACT", "model1")).thenReturn(mockRelatedModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model_name", "model1").param("limit", "10")
                .param("exact", "true").param("owner", "ADMIN").param("status", "ONLINE").param("sortBy", "last_modify")
                .param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels(null, "model1", true, "default", "ADMIN", Arrays.asList("ONLINE"),
                "TEST_KYLIN_FACT", 0, 10, "last_modify", true, null, null, null, null, true, false);
    }

    @Test
    public void testGetModelsWithOutModelName() throws Exception {
        when(modelService.getModels("", "default", true, "ADMIN", Arrays.asList("ONLINE"), "last_modify", true))
                .thenReturn(mockModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model_name", "").param("limit", "10")
                .param("exact", "true").param("owner", "ADMIN").param("status", "ONLINE").param("sortBy", "last_modify")
                .param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels(null, "", true, "default", "ADMIN", Arrays.asList("ONLINE"),
                "TEST_KYLIN_FACT", 0, 10, "last_modify", true, null, null, null, null, true, false);
    }

    @Test
    public void testRenameModel() throws Exception {
        Mockito.doNothing().when(modelService).renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "newAlias", "");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/name", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelName(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testRenameModelException() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setNewModelName("newAlias)))&&&");
        Mockito.doNothing().when(modelService).renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "newAlias)))&&&", "");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/name", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nModelController).updateModelName(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testUpdateModelStatus() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setStatus("DISABLED");
        Mockito.doNothing().when(modelService).updateDataModelStatus("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "OFFLINE");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/status", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelStatus(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testUpdateModelStatusInsensitiveProject() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setProject("DEFAULT");
        modelUpdateRequest.setStatus("ONLINE");
        Mockito.doNothing().when(modelService).updateDataModelStatus("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "OFFLINE");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/status", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelStatus(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    private ModelUpdateRequest mockModelUpdateRequest() {
        ModelUpdateRequest updateRequest = new ModelUpdateRequest();
        updateRequest.setProject("default");
        updateRequest.setNewModelName("newAlias");
        updateRequest.setStatus("DISABLED");
        return updateRequest;
    }

    @Test
    public void testDeleteModel() throws Exception {
        Mockito.doNothing().when(fusionModelService).dropModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model}", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
    }

    @Test
    public void testCreateModel() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        NDataModel mockModel = new NDataModel();
        mockModel.setUuid("mock");
        mockModel.setProject("default");
        Mockito.doReturn(mockModel).when(modelService).createModel(request.getProject(), request);
        Mockito.doReturn(new IndexPlan()).when(modelService).getIndexPlan(mockModel.getId(), mockModel.getProject());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModelWithBaseIndexTypeAndWithBaseIndex() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");

        request.setBaseIndexType(
                Sets.newHashSet(IndexEntity.Source.BASE_AGG_INDEX, IndexEntity.Source.BASE_TABLE_INDEX));
        request.setWithBaseIndex(true);
        NDataModel mockModel = new NDataModel();
        mockModel.setUuid("mock");
        mockModel.setProject("default");
        Mockito.doReturn(mockModel).when(modelService).createModel(request.getProject(), request);
        Mockito.doReturn(new IndexPlan()).when(modelService).getIndexPlan(mockModel.getId(), mockModel.getProject());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModelWithBaseIndex() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");

        request.setWithBaseIndex(true);
        NDataModel mockModel = new NDataModel();
        mockModel.setUuid("mock");
        mockModel.setProject("default");
        Mockito.doReturn(mockModel).when(modelService).createModel(request.getProject(), request);
        Mockito.doReturn(new IndexPlan()).when(modelService).getIndexPlan(mockModel.getId(), mockModel.getProject());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModelWithBaseIndexType() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");

        request.setBaseIndexType(
                Sets.newHashSet(IndexEntity.Source.BASE_AGG_INDEX, IndexEntity.Source.BASE_TABLE_INDEX));
        NDataModel mockModel = new NDataModel();
        mockModel.setUuid("mock");
        mockModel.setProject("default");
        Mockito.doReturn(mockModel).when(modelService).createModel(request.getProject(), request);
        Mockito.doReturn(new IndexPlan()).when(modelService).getIndexPlan(mockModel.getId(), mockModel.getProject());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void checkPartitionDesc() throws Exception {
        PartitionDesc partitionDesc = new PartitionDesc();
        partitionDesc.setPartitionDateColumn("col");
        partitionDesc.setPartitionDateFormat(PartitionDesc.TimestampType.SECOND.name);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/check_partition_desc") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(partitionDesc))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        partitionDesc.setPartitionDateFormat("yyyy'@:1008'MM''dd");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/check_partition_desc") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(partitionDesc))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        partitionDesc.setPartitionDateFormat("error format");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/check_partition_desc") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(partitionDesc))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        partitionDesc.setPartitionDateFormat("");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/check_partition_desc") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(partitionDesc))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        partitionDesc.setPartitionDateFormat("YYYY-dd-hh");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/check_partition_desc") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(partitionDesc))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController, times(5)).checkPartitionDesc(Mockito.any(PartitionDesc.class));
    }

    @Test
    public void testCreateModel_PartitionColumnNotExistException() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setPartitionDesc(new PartitionDesc());
        request.setProject("default");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModel_DataRangeEndLessThanStart() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("1325347200000");
        request.setEnd("1293811200000");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModel_DataRangeLessThan0() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("-1");
        request.setEnd("1293811200000");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setNewModelName("new_model");
        request.setProject("default");
        Mockito.doNothing().when(modelService).cloneModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "new_model",
                "default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model}/clone", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).cloneModel(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelCloneRequest.class));
        request.setNewModelName("dsf gfdg fds");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model}/clone", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
    }

    @Test
    public void testUpdateModelDataCheckDesc() throws Exception {
        final ModelCheckRequest request = new ModelCheckRequest();
        request.setProject("default");
        request.setCheckOptions(7);
        request.setFaultThreshold(10);
        request.setFaultActions(2);
        Mockito.doNothing().when(modelService).updateModelDataCheckDesc("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 7, 10, 2);
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{name}/data_check", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelDataCheckDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);

    }

    @Test
    public void testUpdateModelSemantics_DataRangeEndLessThanStart() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("100");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nModelController).updateSemantic(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testUpdateModelSemantics_DataRangeLessThan0() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("-1");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nModelController).updateSemantic(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testUnlinkModel() throws Exception {
        UnlinkModelRequest request = new UnlinkModelRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService).unlinkModel("default", "nmodel_basci");
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/management_type", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).unlinkModel(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(UnlinkModelRequest.class));
    }

    @Test
    public void testOfflineAllModelsInProject() throws Exception {
        Mockito.doNothing().when(modelService).offlineAllModelsInProject("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/disable_all_models").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).offlineAllModelsInProject("default");
    }

    @Test
    public void testOnlineAllModelsInProject() throws Exception {
        Mockito.doNothing().when(modelService).onlineAllModelsInProject("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/enable_all_models").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).onlineAllModelsInProject("default");
    }

    @Test
    public void testGetModelConfig() throws Exception {
        Mockito.doReturn(new ArrayList<ModelConfigResponse>()).when(modelService).getModelConfig("default", null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/config").param("project", "default")
                .param("model_name", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).getModelConfig("", "default", 0, 10);
    }

    @Test
    public void testUpdateModelConfig() throws Exception {
        val request = new ModelConfigRequest();
        request.setAutoMergeEnabled(false);
        request.setProject("default");
        Mockito.doNothing().when(modelService).updateModelConfig("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelConfig("89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
    }

    @Test
    public void testBatchSaveModels() throws Exception {
        ModelRequest request = new ModelRequest();
        Mockito.doNothing().when(modelService).batchCreateModel("gc_test", Mockito.spy(Lists.newArrayList(request)),
                Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/batch_save_models").param("project", "gc_test")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(Lists.newArrayList(request)))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).batchSaveModels(eq("gc_test"), Mockito.anyList());
    }

    private IndicesResponse mockIndicesResponse() {
        IndexEntity index = new IndexEntity();
        index.setId(1234);
        index.setIndexPlan(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        IndicesResponse indices = new IndicesResponse(index.getIndexPlan());
        indices.addIndexEntity(index);
        return indices;
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
        final List<NDataModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new NDataModelResponse(model));
        NDataModel model1 = new NDataModel();
        model1.setUuid("model2");
        models.add(new NDataModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model2.setUuid("model3");
        models.add(new NDataModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model3.setUuid("model4");
        models.add(new NDataModelResponse(model3));

        return models;
    }

    private List<RelatedModelResponse> mockRelatedModels() {
        final List<RelatedModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new RelatedModelResponse(model));
        NDataModel model1 = new NDataModel();
        model.setUuid("model2");
        models.add(new RelatedModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model.setUuid("model3");
        models.add(new RelatedModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setUuid("model4");
        models.add(new RelatedModelResponse(model3));

        return models;
    }

    @Test
    public void testCheckBeforeModelSave() {
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.setProject("default");
        Mockito.doReturn(new ModelSaveCheckResponse()).when(modelService).checkBeforeModelSave(Mockito.any());
        nModelController.checkBeforeModelSave(modelRequest);
        Mockito.verify(nModelController).checkBeforeModelSave(modelRequest);
    }

    @Test
    public void testUpdateModelOwner() {
        String project = "default";
        String owner = "test";
        String modelId = RandomUtil.randomUUIDStr();

        OwnerChangeRequest ownerChangeRequest = new OwnerChangeRequest();
        ownerChangeRequest.setProject(project);
        ownerChangeRequest.setOwner(owner);

        Mockito.doNothing().when(modelService).updateModelOwner(project, modelId, ownerChangeRequest);
        nModelController.updateModelOwner(modelId, ownerChangeRequest);
        Mockito.verify(nModelController).updateModelOwner(modelId, ownerChangeRequest);
    }

    @Test
    public void testUpdateMultiPartitionMapping() throws Exception {
        MultiPartitionMappingRequest request = new MultiPartitionMappingRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService).updateMultiPartitionMapping(request.getProject(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/multi_partition/mapping", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetMultiPartitionValues() throws Exception {
        MultiPartitionMappingRequest request = new MultiPartitionMappingRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService).updateMultiPartitionMapping(request.getProject(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/multi_partition/mapping", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testAddMultiPartitionValues() throws Exception {
        UpdateMultiPartitionValueRequest request = new UpdateMultiPartitionValueRequest();
        request.setProject("default");
        List<String[]> partition_values = Lists.newArrayList();
        String[] value = new String[] { "5" };
        partition_values.add(value);
        request.setSubPartitionValues(partition_values);
        Mockito.doNothing().when(modelService).addMultiPartitionValues(request.getProject(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getSubPartitionValues());
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/multi_partition/sub_partition_values",
                        "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testAddMultiPartitionValuesException() throws Exception {
        UpdateMultiPartitionValueRequest request = new UpdateMultiPartitionValueRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService).addMultiPartitionValues(request.getProject(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getSubPartitionValues());
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/multi_partition/sub_partition_values",
                        "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
    }

    @Test
    public void testDeleteMultiPartitionValues() throws Exception {
        Mockito.doNothing().when(modelService).deletePartitions("default", null, "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                Sets.newHashSet(1L, 2L));
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/models/{model}/multi_partition/sub_partition_values",
                        "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default").param("ids", new String[] { "1", "2" })
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testValidateExport() throws Exception {
        String project = "default";
        String modelName = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(project);
        syncContext.setModelId(modelName);
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        SyncModel syncModel = Mockito.mock(SyncModel.class);
        Mockito.doReturn(syncContext).when(tdsService).prepareSyncContext(project, modelName,
                SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080);
        Mockito.doReturn(syncModel).when(tdsService).exportTDSDimensionsAndMeasuresByAdmin(syncContext,
                ImmutableList.of(), ImmutableList.of());
        Mockito.doReturn(Boolean.TRUE).when(tdsService).preCheckNameConflict(syncModel);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/validate_export").param("model", modelName)
                .param("project", project).contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testExport() throws Exception {
        String project = "default";
        String modelName = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(project);
        syncContext.setModelId(modelName);
        syncContext.setTargetBI(SyncContext.BI.TABLEAU_CONNECTOR_TDS);
        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        syncContext.setHost("localhost");
        syncContext.setPort(8080);
        syncContext.setDataflow(NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelName));
        syncContext.setKylinConfig(getTestConfig());
        syncContext.setAdmin(true);
        SyncModel syncModel = Mockito.mock(SyncModel.class);
        Mockito.doReturn(syncModel).when(tdsService).exportTDSDimensionsAndMeasuresByAdmin(syncContext,
                ImmutableList.of(), ImmutableList.of());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/export", modelName).param("project", project)
                .param("export_as", "TABLEAU_CONNECTOR_TDS").param("element", "AGG_INDEX_AND_TABLE_INDEX_COL")
                .param("server_host", "localhost").param("server_port", "8080").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testFormatStatus() {
        List<String> status = Lists.newArrayList("OFFLINE", null, "broken");
        assertEquals(nBasicController.formatStatus(status, ModelStatusToDisplayEnum.class),
                Lists.newArrayList("OFFLINE", "BROKEN"));

        thrown.expect(KylinException.class);
        thrown.expectMessage(PARAMETER_INVALID_SUPPORT_LIST.getMsg("status", "ONLINE, OFFLINE, WARNING, BROKEN"));
        status = Lists.newArrayList("OFF", null, "broken");
        nBasicController.formatStatus(status, ModelStatusToDisplayEnum.class);
    }
}
