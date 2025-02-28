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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelSelectContext;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.response.OpenValidationResponse;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
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

import lombok.var;

public class OpenSmartModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelSmartService modelSmartService;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private OpenModelSmartController openSmartModelController = Mockito.spy(new OpenModelSmartController());

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private RawRecService rawRecService;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openSmartModelController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());
        Mockito.doReturn(true).when(aclEvaluate).hasProjectOperationPermission(Mockito.any());
        Mockito.doNothing().when(rawRecService).transferAndSaveRecommendations(Mockito.any());
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
    public void testCouldAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        AbstractContext proposeContext = new ModelSelectContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        Mockito.doReturn(proposeContext).when(modelSmartService).probeRecommendation(favoriteRequest.getProject(),
                favoriteRequest.getSqls());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validation").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).couldAnsweredByExistedModel(Mockito.any());
    }

    @Test
    public void testAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        Mockito.doReturn(new OpenValidationResponse()).when(openSmartModelController).batchSqlValidate("default", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_validation")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).answeredByExistedModel(Mockito.any());
    }

    @Test
    public void testCheckSqlListNotEmpty() {
        List<String> sqls = null;
        try {
            OpenModelSmartController.checkNotEmpty(sqls);
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(MsgPicker.getMsg().getNullEmptySql(), e.getMessage());
        }
    }

    @Test
    public void testGetModel() {
        NDataModelManager nDataModelManager = Mockito.mock(NDataModelManager.class);
        when(modelService.getManager(any(), anyString())).thenReturn(nDataModelManager);
        when(modelService.getModel(anyString(), anyString())).thenCallRealMethod();
        when(nDataModelManager.listAllModels()).thenReturn(Collections.emptyList());
        try {
            modelService.getModel("SOME_ALIAS", "SOME_PROJECT");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(MODEL_NAME_NOT_EXIST.getCodeMsg("SOME_ALIAS"), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCouldAnsweredByExistedModelWithNullSqls() throws Exception {
        List<String> sqls = new ArrayList<>();
        try {
            FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validation")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the array parameter \"sqls\".", e.getMessage());
        }

    }

    @Test
    public void testPostModelSuggestionCouldAnsweredByExistedModelWithNullSqls() throws Exception {
        List<String> sqls = new ArrayList<>();
        try {
            FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the array parameter \"sqls\".", e.getMessage());
        }

    }

    @Test
    public void testPostModelOptimizationCouldAnsweredByExistedModelWithNullSqls() throws Exception {
        List<String> sqls = new ArrayList<>();
        try {
            FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the array parameter \"sqls\".", e.getMessage());
        }

    }

    private void changeProjectToSemiAutoMode(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(project, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }
}
