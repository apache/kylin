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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelImportRequest;
import org.apache.kylin.rest.request.ModelPreviewRequest;
import org.apache.kylin.rest.service.MetaStoreService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

public class NMetaStoreControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Mock
    private MetaStoreService metaStoreService;

    @InjectMocks
    private NMetaStoreController nModelController = Mockito.spy(new NMetaStoreController());

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
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testPreviewModels() throws Exception {
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/metastore/previews/models")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).previewModels("default", Collections.emptyList());
    }

    @Test
    public void testExportModelMetadata() throws Exception {
        Mockito.doNothing().when(nModelController).exportModelMetadata(Mockito.anyString(),
                Mockito.any(ModelPreviewRequest.class), Mockito.any(HttpServletResponse.class));

        final ModelPreviewRequest request = mockModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nModelController).exportModelMetadata(Mockito.anyString(),
                Mockito.any(ModelPreviewRequest.class), Mockito.any(HttpServletResponse.class));
    }

    @Test
    public void testUploadAndCheckModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                Files.newInputStream(file.toPath()));

        SchemaChangeCheckResult schemaChangeCheckResult = new SchemaChangeCheckResult();
        Mockito.when(metaStoreService.checkModelMetadata("default", multipartFile, null))
                .thenReturn(schemaChangeCheckResult);

        final ModelPreviewRequest request = mockModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/validation/models").file(multipartFile)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nModelController).uploadAndCheckModelMetadata("default", multipartFile, null);
    }

    @Test
    public void testImportModelMetadata() throws Throwable {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                Files.newInputStream(file.toPath()));

        final ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        request.setModels(models);
        models.add(new ModelImportRequest.ModelImport("ssb_model", null, ModelImportRequest.ImportType.OVERWRITE));

        MockMultipartFile requestFile = new MockMultipartFile("request", "request", "application/json",
                JsonUtil.writeValueAsString(request).getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/models").file(multipartFile).file(requestFile)
                .contentType(MediaType.MULTIPART_FORM_DATA_VALUE).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nModelController).importModelMetadata("default", multipartFile, request);
    }

    private ModelPreviewRequest mockModelPreviewRequest() {
        ModelPreviewRequest modelPreviewRequest = new ModelPreviewRequest();
        List<String> modelIdList = Lists.newArrayList("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelPreviewRequest.setIds(modelIdList);
        modelPreviewRequest.setExportRecommendations(true);
        modelPreviewRequest.setExportOverProps(true);
        return modelPreviewRequest;
    }
}
