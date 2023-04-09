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

import java.nio.charset.StandardCharsets;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.NIndexPlanController;
import org.apache.kylin.rest.request.OpenUpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.service.FusionIndexService;
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

public class OpenIndexPlanControllerTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final OpenIndexPlanController openIndexPlanController = Mockito.spy(new OpenIndexPlanController());

    @Mock
    private FusionIndexService fusionIndexService;

    @Mock
    private NIndexPlanController nIndexPlanController;

    private MockMvc mockMvc;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openIndexPlanController)
                .defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCreateAggGroups() throws Exception {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").aggregationGroups(Lists.newArrayList()).build();

        OpenUpdateRuleBasedCuboidRequest openRequest = new OpenUpdateRuleBasedCuboidRequest();
        openRequest.setModelAlias("nmodel_basic");
        openRequest.setProject("default");
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        NDataModel model = modelManager.getDataModelDescByAlias(openRequest.getModelAlias());

        Mockito.when(fusionIndexService.convertOpenToInternal(openRequest, model)).thenReturn(request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/agg_groups")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(openRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openIndexPlanController).updateRule(openRequest);
    }

    @Test
    public void testCreateAggGroupsNotExistModel() throws Exception {
        OpenUpdateRuleBasedCuboidRequest openRequest = new OpenUpdateRuleBasedCuboidRequest();
        openRequest.setModelAlias("model_not_exist");
        openRequest.setProject("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/agg_groups")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(openRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        Mockito.verify(openIndexPlanController).updateRule(openRequest);
    }
}
