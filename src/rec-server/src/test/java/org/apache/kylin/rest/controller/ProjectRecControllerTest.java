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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.service.ProjectSmartService;
import org.apache.kylin.rest.util.AclEvaluate;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class ProjectRecControllerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private MockMvc mockMvc;

    @Mock
    private ProjectSmartService projectSmartService;

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private ProjectRecController projectRecController = Mockito.spy(new ProjectRecController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(projectRecController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateFrequencyRule() throws Exception {
        String project = "default";
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(project);
        request.setFreqEnable(false);
        request.setMinHitCount("1");
        request.setUpdateFrequency("1");
        request.setEffectiveDays("1");
        request.setRecommendationsValue("1");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(projectRecController).updateFavoriteRules(Mockito.any(request.getClass()));
    }

    @Test
    public void testCheckUpdateFavoriteRuleArgsWithEmtpyFrequency() {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setMinHitCount("1");
        request.setEffectiveDays("1");
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getUpdateFrequencyNotEmpty());
        ProjectRecController.checkUpdateFavoriteRuleArgs(request);
    }

    @Test
    public void testCheckUpdateFavoriteRuleArgsWithEmptyHitCount() {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getMinHitCountNotEmpty());
        ProjectRecController.checkUpdateFavoriteRuleArgs(request);
    }

    @Test
    public void testCheckUpdateFavoriteRuleArgsWithEmpty() {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setMinHitCount("1");
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getEffectiveDaysNotEmpty());
        ProjectRecController.checkUpdateFavoriteRuleArgs(request);
    }

    @Test
    public void testUpdateFrequencyRuleWithWrongArgs() throws Exception {
        String project = "default";
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(project);
        request.setFreqEnable(true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        request.setFreqEnable(false);
        request.setDurationEnable(true);
        request.setMinDuration("0");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
    }

    @Test
    public void testGetFrequencyRule() throws Exception {
        String project = "default";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/{project}/favorite_rules", project)
                .contentType(MediaType.APPLICATION_JSON).param("project", project)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(projectRecController).getFavoriteRules(project);
    }
}
