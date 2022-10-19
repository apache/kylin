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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.ComputedColumnConfigRequest;
import org.apache.kylin.rest.request.JdbcRequest;
import org.apache.kylin.rest.request.ProjectRequest;
import org.apache.kylin.server.AbstractMVCIntegrationTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

public class NProjectControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testSaveProject() throws Exception {
        ProjectRequest request = new ProjectRequest();
        request.setName("test_PROJECT");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        request.setName("test_project");
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        Assert.assertTrue(StringUtils.contains(result.getResponse().getContentAsString(),
                "The project name \\\"test_PROJECT\\\" already exists. Please rename it."));
    }

    @Test
    public void testUpdateProjectConfig() throws Exception {
        String projectName = "test_update_PROJECT";
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName(projectName);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ComputedColumnConfigRequest request = new ComputedColumnConfigRequest();
        request.setExposeComputedColumn(false);
        mockMvc.perform(MockMvcRequestBuilders
                .put(String.format(Locale.ROOT, "/api/projects/%s/computed_column_config",
                        projectName.toUpperCase(Locale.ROOT)))
                .content(JsonUtil.writeValueAsString(request)).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance project = projectManager.getProject(projectName);
        Assert.assertEquals("false", project.getOverrideKylinProps().get(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF));
    }

    @Test
    public void testUpdateJdbcConfig() throws Exception {
        String projectName = "test_update_jdbc_config";
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName(projectName);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        JdbcRequest jdbcRequest = new JdbcRequest();
        mockMvc.perform(MockMvcRequestBuilders
                .put(String.format(Locale.ROOT, "/api/projects/%s/jdbc_config", projectName.toUpperCase(Locale.ROOT)))
                .content(JsonUtil.writeValueAsString(jdbcRequest)).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
