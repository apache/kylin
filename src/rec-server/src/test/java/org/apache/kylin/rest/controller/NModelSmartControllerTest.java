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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AutoIndexPlanRuleUpdateRequest;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.apache.kylin.rest.request.WhiteListIndexRequest;
import org.apache.kylin.rest.service.ModelSmartService;
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
import lombok.var;

public class NModelSmartControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;
    @Mock
    private ModelSmartService modelSmartService;
    @InjectMocks
    private ModelRecController modelRecController = Mockito.spy(new ModelRecController());
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    public static void transferProjectToSemiAutoMode(KylinConfig kylinConfig, String project) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(project, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(modelRecController).defaultRequest(MockMvcRequestBuilders.get("/"))
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
    public void testSuggestModelWithReuseExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        SqlAccelerateRequest favoriteRequest = new SqlAccelerateRequest("gc_test", sqls, true);
        // reuse existed model
        Mockito.doReturn(null).when(modelSmartService).suggestModel(favoriteRequest.getProject(), Mockito.spy(sqls),
                true, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).suggestModel(Mockito.any());
    }

    @Test
    public void testSuggestModelWithoutReuseExistedModel() throws Exception {
        // don't reuse existed model
        String sql = "SELECT lstg_format_name, test_cal_dt.week_beg_dt, sum(price)\n" + "FROM test_kylin_fact\n"
                + "INNER JOIN edw.test_cal_dt AS test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "GROUP BY lstg_format_name, test_cal_dt.week_beg_dt";
        List<String> sqls = Lists.newArrayList(sql);
        SqlAccelerateRequest accerelateRequest = new SqlAccelerateRequest("gc_test", sqls, false);
        Mockito.doReturn(null).when(modelSmartService).suggestModel(accerelateRequest.getProject(), Mockito.spy(sqls),
                false, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(accerelateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).suggestModel(Mockito.any());
    }

    @Test
    public void testApiCanAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("gc_test", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/can_answered_by_existed_model")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).couldAnsweredByExistedModel(Mockito.any());
    }

    @Test
    public void testApiGetWhitelistIndexes() throws Exception {
        List<Long> whiteList = Lists.newArrayList(20000000001L, 20000000002L, 20000000003L);
        String modelId = "8c08822f-296a-b097-c910-e38d8934b6f9";
        String project = "default";
        Mockito.doReturn(whiteList).when(modelSmartService).getAutoIndexPlanWhiteList(modelId, project);
        mockMvc.perform(MockMvcRequestBuilders
                .get(String.format("/api/models/%s/index_white_list?project=%s", modelId, project))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).getWhiteList(Mockito.any(), Mockito.any());
    }

    @Test
    public void testApiAddWhitelistIndexes() throws Exception {
        List<Long> whiteList = Lists.newArrayList(20000000001L, 20000000002L, 20000000003L);
        String modelId = "8c08822f-296a-b097-c910-e38d8934b6f9";
        String project = "default";
        WhiteListIndexRequest request = new WhiteListIndexRequest(project, whiteList);
        mockMvc.perform(MockMvcRequestBuilders.post(String.format("/api/models/%s/index_white_list", modelId))
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).addWhiteList(Mockito.any(), Mockito.any());
    }

    @Test
    public void testApiRemoveWhitelistIndexes() throws Exception {
        List<Long> whiteList = Lists.newArrayList(20000000001L, 20000000002L, 20000000003L);
        String modelId = "8c08822f-296a-b097-c910-e38d8934b6f9";
        String project = "default";
        WhiteListIndexRequest request = new WhiteListIndexRequest(project, whiteList);
        mockMvc.perform(MockMvcRequestBuilders.delete(String.format("/api/models/%s/index_white_list", modelId))
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).deleteWhiteList(Mockito.any(), Mockito.any());
    }

    @Test
    public void testApiGetAutoIndexPlanRule() throws Exception {
        String modelId = "8c08822f-296a-b097-c910-e38d8934b6f9";
        String project = "default";
        Mockito.doReturn(Collections.emptyMap()).when(modelSmartService).getAutoIndexPlanRule(modelId, project);
        mockMvc.perform(MockMvcRequestBuilders
                .get(String.format("/api/models/%s/auto_index_plan_rule?project=%s", modelId, project))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).getAutoIndexPlanRules(Mockito.any(), Mockito.any());
    }

    @Test
    public void testApiUpdateAutoIndexPlanRule() throws Exception {
        String modelId = "8c08822f-296a-b097-c910-e38d8934b6f9";
        String project = "default";
        AutoIndexPlanRuleUpdateRequest request = new AutoIndexPlanRuleUpdateRequest();
        request.setProject(project);
        request.setIndexPlannerEnable(true);
        request.setIndexPlannerMaxIndexCount(100);
        request.setIndexPlannerMaxChangeCount(10);
        request.setAutoIndexPlanAutoCompleteMode("RELATIVE");
        request.setAutoIndexPlanRelativeTimeUnit("MONTH");
        request.setAutoIndexPlanRelativeTimeInterval(12);
        request.setAutoIndexPlanSegmentJobEnable(true);
        Mockito.doNothing().when(modelSmartService).updateAutoIndexPlanRule(modelId, request);
        transferProjectToSemiAutoMode(getTestConfig(), project);
        mockMvc.perform(MockMvcRequestBuilders
                .post(String.format("/api/models/%s/auto_index_plan_rule?project=%s", modelId, project))
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).updateAutoIndexPlanRules(Mockito.any(), Mockito.any());
    }

    @Test
    public void testApiUpdateAutoIndexPlanRuleError() throws Exception {
        String modelId = "8c08822f-296a-b097-c910-e38d8934b6f9";
        String project = "default";
        AutoIndexPlanRuleUpdateRequest request = new AutoIndexPlanRuleUpdateRequest();
        request.setProject(project);
        request.setIndexPlannerEnable(true);
        request.setIndexPlannerMaxIndexCount(100);
        request.setIndexPlannerMaxChangeCount(10);
        request.setAutoIndexPlanAutoCompleteMode("RELATIVE");
        request.setAutoIndexPlanRelativeTimeUnit("MONTH");
        request.setAutoIndexPlanRelativeTimeInterval(12);
        request.setAutoIndexPlanSegmentJobEnable(true);

        testErrorCase(request, modelId, ServerErrorCode.SEMI_AUTO_NOT_ENABLED);
        transferProjectToSemiAutoMode(getTestConfig(), project);

        request.setIndexPlannerMaxChangeCount(101);
        testErrorCase(request, modelId, ServerErrorCode.INVALID_RANGE);

        request.setIndexPlannerMaxIndexCount(100001);
        testErrorCase(request, modelId, ServerErrorCode.INVALID_RANGE);

        request.setIndexPlannerMaxIndexCount(1000);
        request.setIndexPlannerMaxChangeCount(100);
        request.setAutoIndexPlanRelativeTimeUnit("months");
        testErrorCase(request, modelId, ServerErrorCode.INVALID_DATE_UNIT);

        request.setAutoIndexPlanRelativeTimeUnit("MONTH");
        request.setAutoIndexPlanAutoCompleteMode("RELATIVELY");
        testErrorCase(request, modelId, ServerErrorCode.INVALID_AUTO_COMPLETE_MODE);

        request.setAutoIndexPlanAbsoluteBeginDate("20230101");
        request.setAutoIndexPlanAutoCompleteMode("ABSOLUTE");
        testErrorCase(request, modelId, ServerErrorCode.INVALID_DATE_FORMAT);
    }

    private void testErrorCase(AutoIndexPlanRuleUpdateRequest request, String modelId, ServerErrorCode errorCode)
            throws Exception {
        //        Mockito.doNothing().when(modelSmartService).updateAutoIndexPlanRule(modelId, request);
        MvcResult res = mockMvc.perform(MockMvcRequestBuilders
                .post(String.format("/api/models/%s/auto_index_plan_rule?project=%s", modelId, request.getProject()))
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))).andReturn();
        assertTrue(res.getResolvedException() instanceof KylinException);
        val kylinException = (KylinException) res.getResolvedException();
        Assert.assertEquals(errorCode.toErrorCode(), kylinException.getErrorCode());
    }
}
