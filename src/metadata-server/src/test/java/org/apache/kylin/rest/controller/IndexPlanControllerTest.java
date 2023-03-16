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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.CreateBaseIndexRequest;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.service.FusionIndexService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.ModelService;
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

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public class IndexPlanControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private IndexPlanService indexPlanService;

    @Mock
    private FusionIndexService fusionIndexService;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private NIndexPlanController indexPlanController = Mockito.spy(new NIndexPlanController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(indexPlanController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateRule() throws Exception {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").aggregationGroups(Lists.newArrayList()).build();
        Mockito.when(fusionIndexService.updateRuleBasedCuboid(Mockito.anyString(),
                Mockito.any(UpdateRuleBasedCuboidRequest.class)))
                .thenReturn(new Pair<>(new IndexPlan(), new BuildIndexResponse()));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(indexPlanController).updateRule(Mockito.any(UpdateRuleBasedCuboidRequest.class));
    }

    @Test
    public void testGetRule() throws Exception {
        Mockito.doReturn(null).when(fusionIndexService).getIndexes("default", "abc", "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/index_plans/index").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("model", "abc")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testCalculateDiffRuleBasedIndex() throws Exception {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").aggregationGroups(Lists.newArrayList()).build();
        Mockito.when(indexPlanService.calculateDiffRuleBasedIndex(Mockito.any(UpdateRuleBasedCuboidRequest.class)))
                .thenReturn(new DiffRuleBasedIndexResponse("", 1, 1, 1));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule_based_index_diff")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(indexPlanController)
                .calculateDiffRuleBasedIndex(Mockito.any(UpdateRuleBasedCuboidRequest.class));
    }

    @Test
    public void testCreateBaseIndex() throws Exception {
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setProject("default");
        request.setModelId("abc");
        Mockito.doReturn(null).when(indexPlanService).createBaseIndex("default", request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/index_plans/base_index")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateBaseIndex() throws Exception {
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setProject("default");
        request.setModelId("abc");
        Mockito.doReturn(null).when(indexPlanService).updateBaseIndex("default", request, false);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/base_index")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetIndexStat() throws Exception {
        Mockito.doReturn(null).when(indexPlanService).getStat("default", "abc");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/index_plans/index_stat")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model_id", "abc")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateTableIndex() throws Exception {
        CreateTableIndexRequest tableIndexRequest = CreateTableIndexRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").id(20000010001L).colOrder(Lists
                        .newArrayList("TEST_KYLIN_FACT.TRANS_ID", "TEST_SITES.SITE_NAME", "TEST_KYLIN_FACT.CAL_DT"))
                .sortByColumns(Lists.newArrayList()).build();
        Mockito.when(indexPlanService.updateTableIndex(Mockito.anyString(), Mockito.any(CreateTableIndexRequest.class)))
                .thenReturn(new BuildIndexResponse());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/table_index")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(tableIndexRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
