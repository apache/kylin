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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INDEX_PARAMETER_INVALID;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.NModelController;
import org.apache.kylin.rest.request.ModelCloneRequest;
import org.apache.kylin.rest.request.ModelConfigRequest;
import org.apache.kylin.rest.request.ModelParatitionDescRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.ModelUpdateRequest;
import org.apache.kylin.rest.request.MultiPartitionMappingRequest;
import org.apache.kylin.rest.request.OpenModelConfigRequest;
import org.apache.kylin.rest.request.OpenModelRequest;
import org.apache.kylin.rest.request.PartitionColumnRequest;
import org.apache.kylin.rest.request.UpdateMultiPartitionValueRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.ComputedColumnConflictResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.response.ModelConfigResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.response.OpenGetIndexResponse;
import org.apache.kylin.rest.service.FusionIndexService;
import org.apache.kylin.rest.service.FusionModelService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelTdsService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.tool.bisync.SyncContext;
import org.apache.kylin.tool.bisync.model.SyncModel;
import org.hamcrest.Matchers;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;

public class OpenModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private NModelController nModelController;

    @Mock
    private FusionIndexService fusionIndexService;

    @Mock
    private ModelService modelService;

    @Mock
    private FusionModelService fusionModelService;

    @Mock
    private ModelTdsService tdsService;

    @Mock
    private AclEvaluate aclEvaluate;

    @InjectMocks
    private OpenModelController openModelController = Mockito.spy(new OpenModelController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static final String LAST_MODIFY = "last_modified";
    private static final String USAGE = "usage";
    private static final String DATA_SIZE = "data_size";
    private static final Set<String> INDEX_SORT_BY_SET = ImmutableSet.of(USAGE, LAST_MODIFY, DATA_SIZE);
    private static final Set<String> INDEX_SOURCE_SET = Arrays.stream(IndexEntity.Source.values()).map(Enum::name)
            .collect(Collectors.toSet());
    private static final Set<String> INDEX_STATUS_SET = Arrays.stream(IndexEntity.Status.values()).map(Enum::name)
            .collect(Collectors.toSet());

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());
        Mockito.doReturn(true).when(aclEvaluate).hasProjectOperationPermission(Mockito.any());
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private List<NDataModel> mockModels() {
        final List<NDataModel> models = new ArrayList<>();
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

    private Segments<NDataSegmentResponse> mockSegments() {
        final Segments<NDataSegmentResponse> nDataSegments = new Segments<>();
        NDataSegmentResponse segment = new NDataSegmentResponse();
        segment.setId(RandomUtil.randomUUIDStr());
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private NDataModelResponse mockGetModelName(String modelName, String project, String modelId) {
        NDataModelResponse model = new NDataModelResponse();
        model.setUuid(modelId);
        Mockito.doReturn(model).when(modelService).getModel(modelName, project);
        return model;
    }

    @Test
    public void testGetModels() throws Exception {
        Mockito.when(nModelController.getModels("model1", "model1", true, "default", "ADMIN", Arrays.asList("NEW"), "",
                1, 5, "last_modify", false, null, null, null, null, true, false)).thenReturn(
                        new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(mockModels(), 0, 10), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("page_offset", "1").param("project", "default").param("model_id", "model1")
                .param("model_name", "model1").param("page_size", "5").param("exact", "true").param("table", "")
                .param("owner", "ADMIN").param("status", "NEW").param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openModelController).getModels("default", "model1", "model1", true, "ADMIN",
                Arrays.asList("NEW"), "", 1, 5, "last_modify", true, null, null, null, true, false);
    }

    @Test
    public void testGetIndexes() throws Exception {
        List<Long> ids = Lists.newArrayList(1L, 20000020001L);
        String project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String modelName = "default_model_name";
        NDataModel model = new NDataModel();
        model.setUuid(modelId);
        model.setAlias(modelName);
        val modelManager = Mockito.mock(NDataModelManager.class);
        Mockito.doCallRealMethod().when(modelService).getModel(Mockito.anyString(), Mockito.anyString());
        Mockito.when(modelService.getManager(NDataModelManager.class, project)).thenReturn(modelManager);
        Mockito.when(modelManager.listAllModels()).thenReturn(Lists.newArrayList(model));

        Mockito.when(fusionIndexService.getIndexesWithRelatedTables(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(getIndexResponses());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/indexes", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("project", project) //
                .param("batch_index_ids", "1,20000020001")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        OpenGetIndexResponse response = openModelController.getIndexes(project, modelName, null, 0, 10, //
                null, "last_modified", "", true, ids).getData();
        Assert.assertArrayEquals(response.getAbsentBatchIndexIds().toArray(),
                Lists.newArrayList(20000020001L).toArray());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/indexes", modelId)
                .contentType(MediaType.APPLICATION_JSON).param("project", project) //
                .param("batch_index_ids", "1,20000020001")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/indexes", modelId + modelName)
                .contentType(MediaType.APPLICATION_JSON).param("project", project) //
                .param("batch_index_ids", "1,20000020001")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

    }

    private List<IndexResponse> getIndexResponses() throws Exception {
        IndexResponse index = new IndexResponse();
        index.setId(1L);
        index.setRelatedTables(Lists.newArrayList("table1", "table2"));
        return Lists.newArrayList(index);
    }

    @Test
    public void testGetModelDesc() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
    }

    @Test
    public void testGetModelDescWithTableRefsAndCCInfo() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
        Assert.assertNotNull(result.getResponse().getContentAsString().contains("all_table_refs"));
        Assert.assertNotNull(result.getResponse().getContentAsString().contains("computed_columns"));
    }

    @Test
    public void testGetModelDescForStreaming() throws Exception {
        String project = "streaming_test";
        String modelAlias = "model_streaming";
        val model = mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        model.setModelType(NDataModel.ModelType.STREAMING);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
    }

    @Test
    public void testUpdateParatitionDesc() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, "modelId");
        ModelParatitionDescRequest modelParatitionDescRequest = new ModelParatitionDescRequest();
        modelParatitionDescRequest.setPartitionDesc(null);
        Mockito.doNothing().when(modelService).updateModelPartitionColumn(project, modelAlias,
                modelParatitionDescRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{project}/{model}/partition_desc", project, modelAlias)
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelParatitionDescRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updatePartitionDesc(project, modelAlias, modelParatitionDescRequest);
    }

    @Test
    public void testUpdateMultiPartitionMapping() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        List<MultiPartitionMappingRequest.MappingRequest<List<String>, List<String>>> valueMapping = new ArrayList<>();
        List<String> origin = new ArrayList<>();
        origin.add("beijing");
        List<String> target = new ArrayList<>();
        target.add("shanghai");
        MultiPartitionMappingRequest.MappingRequest mappingRequest = new MultiPartitionMappingRequest.MappingRequest(
                origin, target);
        valueMapping.add(mappingRequest);
        mockGetModelName(modelName, project, modelId);
        MultiPartitionMappingRequest request = new MultiPartitionMappingRequest();
        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("SSB.CUSTOMER");
        List<String> aliasCols = new ArrayList<>();
        aliasCols.add("SSB");
        // test error
        request.setProject("multi_level_partition");
        Mockito.doNothing().when(modelService).updateMultiPartitionMapping(request.getProject(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/multi_partition/mapping", modelName)
                .param("project", project).contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andExpect(MockMvcResultMatchers.content().string(
                        Matchers.containsString(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode())));
        request.setAliasCols(aliasCols);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/multi_partition/mapping", modelName)
                .param("project", project).contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andExpect(MockMvcResultMatchers.content().string(
                        Matchers.containsString(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode())));
        request.setPartitionCols(partitionCols);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/multi_partition/mapping", modelName)
                .param("project", project).contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andExpect(MockMvcResultMatchers.content().string(
                        Matchers.containsString(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode())));
        request.setValueMapping(valueMapping);
        // test ok
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/multi_partition/mapping", modelName)
                .param("project", project).contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testAddMultiValues() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        UpdateMultiPartitionValueRequest request = new UpdateMultiPartitionValueRequest();
        request.setProject("multi_level_partition");

        Mockito.doReturn(null).when(nModelController).addMultiPartitionValues(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model_name}/segments/multi_partition/sub_partition_values", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        // test exception
        request.setProject("default");
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model_name}/segments/multi_partition/sub_partition_values", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
    }

    @Test
    public void testCheckMLPNotEmpty() {
        String fieldName = "sub_partition_values";
        List<String[]> subPartitionValues = new ArrayList<>();
        try {
            OpenModelController.checkMLP(fieldName, subPartitionValues);
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(String.format(Locale.ROOT, "'%s' cannot be empty.", fieldName), e.getMessage());
        }

    }

    @Test
    public void testUpdatePartition() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        PartitionColumnRequest request = new PartitionColumnRequest();
        request.setMultiPartitionDesc(new MultiPartitionDesc());
        request.setProject("multi_level_partition");
        Mockito.doReturn(null).when(nModelController).updatePartitionSemantic(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/partition", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testCheckIndex() {
        List<String> statusStringList = Lists.newArrayList("recommended_table_index", "RECOMMENDED_AGG_INDEX",
                "CUSTOM_AGG_INDEX", "custom_table_index");
        List<IndexEntity.Source> statuses = OpenModelController.checkSources(statusStringList);
        Assert.assertEquals(IndexEntity.Source.RECOMMENDED_TABLE_INDEX, statuses.get(0));
        Assert.assertEquals(IndexEntity.Source.RECOMMENDED_AGG_INDEX, statuses.get(1));
        Assert.assertEquals(IndexEntity.Source.CUSTOM_AGG_INDEX, statuses.get(2));
        Assert.assertEquals(IndexEntity.Source.CUSTOM_TABLE_INDEX, statuses.get(3));

        try {
            OpenModelController.checkSources(Lists.newArrayList("ab"));
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(INDEX_PARAMETER_INVALID.getCodeMsg("sources", String.join(", ", INDEX_SOURCE_SET)),
                    e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckIndexStatus() {
        List<String> statusStringList = Lists.newArrayList("NO_BUILD", "BUILDING", "LOCKED", "ONLINE");
        List<IndexEntity.Status> statuses = OpenModelController.checkIndexStatus(statusStringList);
        Assert.assertEquals(IndexEntity.Status.NO_BUILD, statuses.get(0));
        Assert.assertEquals(IndexEntity.Status.BUILDING, statuses.get(1));
        Assert.assertEquals(IndexEntity.Status.LOCKED, statuses.get(2));
        Assert.assertEquals(IndexEntity.Status.ONLINE, statuses.get(3));

        try {
            OpenModelController.checkIndexStatus(Lists.newArrayList("ab"));
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(INDEX_PARAMETER_INVALID.getCodeMsg("status", String.join(", ", INDEX_STATUS_SET)),
                    e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckIndexSortBy() {
        List<String> orderBy = Lists.newArrayList("last_modified", "usage", "data_size");
        orderBy.forEach(element -> {
            String actual = OpenModelController.checkIndexSortBy(element);
            Assert.assertEquals(element.toLowerCase(Locale.ROOT), actual);
        });

        try {
            OpenModelController.checkIndexSortBy("ab");
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(INDEX_PARAMETER_INVALID.getCodeMsg("sort_by", String.join(", ", INDEX_SORT_BY_SET)),
                    e.getLocalizedMessage());
        }
    }

    @Test
    public void testOpenapiUpdateModelName() throws Exception {
        String project = "default";
        String new_model_name = "newName";
        String model = "model1";
        mockGetModelName(model, project, RandomUtil.randomUUIDStr());
        ModelUpdateRequest modelUpdateRequest = new ModelUpdateRequest();
        modelUpdateRequest.setProject(project);
        modelUpdateRequest.setNewModelName(new_model_name);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/name", model)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateModelName(model, modelUpdateRequest);
    }

    @Test
    public void testOpenapiUpdateModelName2() throws Exception {
        String project = "default";
        String model = "model1";
        mockGetModelName(model, project, RandomUtil.randomUUIDStr());
        ModelUpdateRequest modelUpdateRequest = new ModelUpdateRequest();
        modelUpdateRequest.setProject(project);
        modelUpdateRequest.setNewModelName("");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/name", model)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        Mockito.verify(openModelController).updateModelName(model, modelUpdateRequest);
    }

    @Test
    public void testOpenAPIBIExport() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/bi_export").param("model_name", modelName)
                .param("project", project).param("export_as", "TABLEAU_ODBC_TDS").param("element", "CUSTOM_COLS")
                .param("dimensions", "").param("measures", "").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testBIExportByADMIN() throws Exception {
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
        SyncModel syncModel = Mockito.mock(SyncModel.class);
        NDataModel model = new NDataModel();
        model.setUuid("aaa");
        Mockito.doReturn(model).when(modelService).getModel(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(syncContext).when(tdsService).prepareSyncContext(project, modelName,
                SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080);
        Mockito.doReturn(syncModel).when(tdsService).exportTDSDimensionsAndMeasuresByAdmin(syncContext,
                Lists.newArrayList(), Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/bi_export").param("model_name", modelName)
                .param("project", project).param("export_as", "TABLEAU_CONNECTOR_TDS").param("element", "CUSTOM_COLS")
                .param("server_host", "localhost").param("server_port", "8080").param("dimensions", "")
                .param("measures", "").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testBIExportByNormalUser() throws Exception {
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
        SyncModel syncModel = Mockito.mock(SyncModel.class);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        NDataModel model = new NDataModel();
        model.setUuid("aaa");
        Mockito.doReturn(model).when(modelService).getModel(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(syncContext).when(tdsService).prepareSyncContext(project, modelName,
                SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080);
        Mockito.doReturn(syncModel).when(tdsService).exportTDSDimensionsAndMeasuresByNormalUser(syncContext,
                Lists.newArrayList(), Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/bi_export").param("model_name", modelName)
                .param("project", project).param("export_as", "TABLEAU_CONNECTOR_TDS").param("element", "CUSTOM_COLS")
                .param("server_host", "localhost").param("server_port", "8080").param("dimensions", "")
                .param("measures", "").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateModelStatus() throws Exception {
        String project = "default";
        String modelName = "model1";
        mockGetModelName(modelName, project, RandomUtil.randomUUIDStr());
        ModelUpdateRequest modelUpdateRequest = new ModelUpdateRequest();
        modelUpdateRequest.setProject(project);
        modelUpdateRequest.setStatus("OFFLINE");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/status", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateModelStatus(modelName, modelUpdateRequest);
    }

    @Test
    public void testCreateModel() throws Exception {
        String project = "default";
        String modelName = "model1";
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.setProject(project);
        modelRequest.setAlias(modelName);
        modelRequest.setRootFactTableName("test_table");

        Mockito.when(modelService.checkCCConflict(modelRequest))
                .thenReturn(new Pair<>(modelRequest, new ComputedColumnConflictResponse()));
        Mockito.when(nModelController.createModel(modelRequest))
                .thenReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, new BuildBaseIndexResponse(), ""));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).createModel(modelRequest);
    }

    @Test
    public void testCreateModelFailed() throws Exception {
        ModelRequest modelRequest = new ModelRequest();

        // Test throwing EMPTY_PROJECT_NAME
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andExpect(MockMvcResultMatchers.content().string(
                        Matchers.containsString(ServerErrorCode.EMPTY_PROJECT_NAME.toErrorCode().getCodeString())));

        // Test throwing INVALID_PARAMETER
        modelRequest.setProject("default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andExpect(MockMvcResultMatchers.content().string(
                        Matchers.containsString(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode())));
    }

    @Test
    public void testUpdateModelSemantics() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        OpenModelRequest request = new OpenModelRequest();
        request.setProject(project);
        request.setModelName(modelAlias);
        Mockito.doReturn(Mockito.mock(BuildBaseIndexResponse.class)).when(fusionModelService)
                .updateDataModelSemantic(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/modification").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateSemantic(Mockito.any(OpenModelRequest.class));
    }

    @Test
    public void testCommentsSynchronization() throws Exception {
        String project = "default";
        String modelName = "model1";
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.setProject(project);
        modelRequest.setAlias(modelName);
        modelRequest.setRootFactTableName("test_fact_table");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/comments_synchronization")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).commentsSynchronization(modelRequest);
    }

    @Test
    public void testDeleteModel() throws Exception {
        String project = "default";
        String modelName = "model1";
        NDataModel model = Mockito.mock(NDataModel.class);

        Mockito.when(nModelController.deleteModel(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(new EnvelopeResponse<String>(KylinException.CODE_SUCCESS, "", ""));
        Mockito.when(modelService.getModelWithoutBrokenCheck(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(model);
        Mockito.when(model.getId()).thenReturn("123");
        mockMvc.perform(MockMvcRequestBuilders.delete(String.format("/api/models/%s?project=%s", modelName, project))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteModel(modelName, project);
    }

    @Test
    public void testCloneModel() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        String cloneModelAlias = "model1_clone";
        mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());

        ModelCloneRequest request = new ModelCloneRequest();
        request.setNewModelName(cloneModelAlias);
        request.setProject(project);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name:.+}/clone", modelAlias)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).cloneModel(modelAlias, request);
    }

    @Test
    public void testModelConfig() throws Exception {
        OpenModelConfigRequest request = new OpenModelConfigRequest();
        request.setProject("default");
        request.setModel("model1");

        NDataModelResponse model = new NDataModelResponse();
        model.setAlias(request.getModel());
        Mockito.doReturn(model).when(modelService).getModel(request.getModel(), request.getProject());

        ArrayList<ModelConfigResponse> modelConfigs = new ArrayList<>();
        ModelConfigResponse configResponse = new ModelConfigResponse();
        configResponse.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        configResponse.setAlias("model1");
        modelConfigs.add(configResponse);
        Mockito.doReturn(modelConfigs).when(modelService).getModelConfig("default", "model1");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/config").param("model", request.getModel())
                .param("project", request.getProject())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).getModelConfig(request.getModel(), request.getProject());

        //add
        request.getCustomSettings().put("test.add.key", "test.add.value");
        request.getCustomSettings().put("test.add.key2", "10");
        ModelConfigRequest modelConfigRequest = new ModelConfigRequest();
        modelConfigRequest.setProject(request.getProject());
        LinkedHashMap<String, String> props = new LinkedHashMap<>(request.getCustomSettings());
        modelConfigRequest.setOverrideProps(props);

        Mockito.doNothing().when(modelService).updateModelConfig("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                modelConfigRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).addModelConfig(request);

        //add error check
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        //modify
        request.getCustomSettings().put("test.add.key", "test.change.value");
        modelConfigRequest.getOverrideProps().put("test.add.key", "test.change.value");
        Mockito.doNothing().when(modelService).updateModelConfig("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                modelConfigRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateModelConfig(request);

        //test case ignore
        request.setProject("dEfault");
        request.setModel("mOdel1");
        Mockito.doReturn(model).when(modelService).getModel("mOdel1", "default");
        Mockito.doReturn(modelConfigs).when(modelService).getModelConfig("dEfault", "model1");
        Mockito.doNothing().when(modelService).updateModelConfig("dEfault", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                modelConfigRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateModelConfig(request);

        request.setProject("default");
        request.setModel("model1");
        //delete
        Set<String> set = new HashSet<>();
        set.add("test.add.key");
        request.setDeleteCustomSettings(set);
        modelConfigRequest.getOverrideProps().remove("test.add.key");
        Mockito.doNothing().when(modelService).updateModelConfig("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                modelConfigRequest);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteModelConfig(request);

        //DeleteCustomSettings empty check
        request.setDeleteCustomSettings(new HashSet<>());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        //CustomSettings empty check
        request.setCustomSettings(new HashMap<>());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        //modify error check
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        //delete error check
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        //project error check
        request.setProject("11");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/config").param("model", request.getModel())
                .param("project", request.getProject())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        //project error check
        request.setModel("default");
        request.setModel("test");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/config").param("model", request.getModel())
                .param("project", request.getProject())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
    }
}
