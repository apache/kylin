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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelCreateContext;
import org.apache.kylin.rec.ModelReuseContext;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.service.ModelRecService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;
import lombok.var;

public class OpenModelRecControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Mock
    private ModelSmartService modelSmartService;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private OpenModelRecController openModelRecController = Mockito.spy(new OpenModelRecController());

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private RawRecService rawRecService;

    @Mock
    private ModelRecService modelRecService;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openModelRecController)
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
    public void testSuggestModels() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelCreateContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, false,
                false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        HashMap<String, Object> mapRequest = (HashMap<String, Object>) mockModelSuggestionRequestMap();
        mapRequest.put("with_segment", "abc");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        HashMap<String, Object> mapRequest2 = (HashMap<String, Object>) mockModelSuggestionRequestMap();
        mapRequest2.put("with_model_online", "123");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest2))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        HashMap<String, Object> mapRequest3 = (HashMap<String, Object>) mockModelSuggestionRequestMap();
        mapRequest3.put("with_model_online", true);
        mapRequest3.put("with_segment", false);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest3))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
    }

    @Test
    public void testOptimizeModels() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        {
            HashMap<String, Object> mapRequest = (HashMap<String, Object>) mockModelOptimizationRequestMap();
            mapRequest.put("accept_recommendation", "abc");
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        }

        {
            HashMap<String, Object> mapRequest = (HashMap<String, Object>) mockModelOptimizationRequestMap();
            mapRequest.put("accept_recommendation", "true");
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
        }

        {
            HashMap<String, Object> mapRequest = (HashMap<String, Object>) mockModelOptimizationRequestMap();
            mapRequest.put("accept_recommendation", "false");
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
        }

        {
            HashMap<String, Object> mapRequest = (HashMap<String, Object>) mockModelOptimizationRequestMap();
            mapRequest.put("accept_recommendation", "null");
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mapRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
        }
    }

    private Map<String, Object> mockModelSuggestionRequestMap() {
        HashMap<String, Object> map = Maps.newHashMap();
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        map = JsonUtil.convert(favoriteRequest, HashMap.class);
        return map;
    }

    private Map<String, Object> mockModelOptimizationRequestMap() {
        HashMap<String, Object> map = Maps.newHashMap();
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        map = JsonUtil.convert(favoriteRequest, HashMap.class);
        return map;
    }

    @Test
    public void testAccSqls() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/accelerate_sqls")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelRecController).accelerateSqls(Mockito.any());
    }

    @Test
    public void testSqlAcceleration() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/sql_acceleration")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelRecController).accelerateSqls(Mockito.any());
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

    @Test
    public void testPutBatchApproveRecommendations() throws Exception {
        HashMap<String, Object> request = Maps.newHashMap();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/recommendations/batch")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect((MockMvcResultMatchers.status().is5xxServerError()));

        request.put("filter_by_models", "test");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/recommendations/batch")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect((MockMvcResultMatchers.status().is5xxServerError()));

        request.put("project", "default");
        request.put("filter_by_models", Collections.singletonList("model"));
        changeProjectToSemiAutoMode("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/recommendations/batch")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect((MockMvcResultMatchers.status().is5xxServerError()));

    }
}
