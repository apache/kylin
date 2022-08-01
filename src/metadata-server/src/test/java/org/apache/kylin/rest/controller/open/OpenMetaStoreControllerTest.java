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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.controller.NMetaStoreController;
import org.apache.kylin.rest.request.ModelImportRequest;
import org.apache.kylin.rest.request.ModelPreviewRequest;
import org.apache.kylin.rest.request.OpenModelPreviewRequest;
import org.apache.kylin.rest.service.MetaStoreService;
import org.apache.kylin.rest.service.ModelService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

public class OpenMetaStoreControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    private static String defaultProjectName = "default";

    @Mock
    private MetaStoreService metaStoreService;

    @Mock
    private ModelService modelService = Mockito.spy(ModelService.class);

    @InjectMocks
    private OpenMetaStoreController openMetaStoreController = Mockito.spy(new OpenMetaStoreController());

    @Mock
    private NMetaStoreController metaStoreController;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openMetaStoreController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
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

    @Test
    public void testPreviewModels() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/metastore/previews/models")
                .contentType(MediaType.APPLICATION_JSON).param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openMetaStoreController).previewModels(defaultProjectName, Collections.emptyList());
    }

    @Test
    public void testExportModelMetadata() throws Exception {
        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        Mockito.doReturn(Mockito.mock(NDataModel.class)).when(dataModelManager)
                .getDataModelDescByAlias("warningmodel1");
        Mockito.doReturn(dataModelManager).when(modelService).getManager(NDataModelManager.class, defaultProjectName);

        final OpenModelPreviewRequest request = mockOpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testExportModelMetadataEmptyModelNameException() throws Exception {
        final OpenModelPreviewRequest request = new OpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testExportModelMetadataModelNotExistException() throws Exception {
        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        Mockito.doReturn(Mockito.mock(NDataModel.class)).when(dataModelManager).getDataModelDescByAlias(null);
        Mockito.doReturn(dataModelManager).when(modelService).getManager(NDataModelManager.class, defaultProjectName);

        final OpenModelPreviewRequest request = mockOpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testExportModelMetadataModelBrokenException() throws Exception {
        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.doReturn(true).when(dataModel).isBroken();
        Mockito.doReturn(dataModel).when(dataModelManager).getDataModelDescByAlias("warningmodel1");
        Mockito.doReturn(dataModelManager).when(modelService).getManager(NDataModelManager.class, defaultProjectName);

        final OpenModelPreviewRequest request = mockOpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testValidateModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                new FileInputStream(file));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/validation/models").file(multipartFile)
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openMetaStoreController).uploadAndCheckModelMetadata("default", multipartFile, null);
    }

    @Test
    public void testImportModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                new FileInputStream(file));

        final ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        request.setModels(models);
        models.add(new ModelImportRequest.ModelImport("ssb_model", null, ModelImportRequest.ImportType.OVERWRITE));

        MockMultipartFile requestFile = new MockMultipartFile("request", "request", "application/json",
                JsonUtil.writeValueAsString(request).getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/import/models").file(multipartFile)
                .file(requestFile).contentType(MediaType.MULTIPART_FORM_DATA_VALUE).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openMetaStoreController).importModelMetadata("default", multipartFile, request);
    }

    private OpenModelPreviewRequest mockOpenModelPreviewRequest() {
        OpenModelPreviewRequest modelPreviewRequest = new OpenModelPreviewRequest();
        List<String> modelNameList = Lists.newArrayList("warningmodel1");
        modelPreviewRequest.setNames(modelNameList);
        modelPreviewRequest.setExportRecommendations(true);
        modelPreviewRequest.setExportOverProps(true);
        return modelPreviewRequest;
    }

    @Test
    public void testConvertToModelPreviewRequest() throws Exception {
        OpenModelPreviewRequest openModelPreviewRequest = new OpenModelPreviewRequest();
        openModelPreviewRequest.setNames(Collections.singletonList("model1"));
        openModelPreviewRequest.setExportOverProps(true);
        openModelPreviewRequest.setExportRecommendations(true);
        openModelPreviewRequest.setExportMultiplePartitionValues(true);

        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.doReturn("1").when(dataModel).getUuid();
        Mockito.doReturn(dataModel).when(dataModelManager).getDataModelDescByAlias(anyString());

        Mockito.doReturn(dataModelManager).when(modelService).getManager(NDataModelManager.class, defaultProjectName);

        ModelPreviewRequest request = Whitebox.invokeMethod(openMetaStoreController, "convertToModelPreviewRequest",
                defaultProjectName, openModelPreviewRequest);

        Assert.assertEquals(Collections.singletonList("1"), request.getIds());
        Assert.assertTrue(request.isExportOverProps());
        Assert.assertTrue(request.isExportRecommendations());
        Assert.assertTrue(request.isExportMultiplePartitionValues());
    }

}
