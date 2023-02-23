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
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelCloneRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.ModelUpdateRequest;
import org.apache.kylin.server.AbstractMVCIntegrationTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.fasterxml.jackson.databind.JsonNode;

public class NModelControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testCreateModel() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("DEfault");
        request.setAlias("NMODEL_BASIC");
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andExpect(jsonPath("$.code").value("999"))
                .andReturn();
        String msg = String.format(Locale.ROOT, Message.getInstance().getModelAliasDuplicated(), "nmodel_basic");
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
    }

    @Test
    public void testBatchSaveModels() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("GC_test");
        request.setAlias("new_MOdel");

        ModelRequest request2 = new ModelRequest();
        request2.setProject("gc_TEst");
        request2.setAlias("new_model");

        List<ModelRequest> modelRequests = Arrays.asList(request, request2);

        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/batch_save_models")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "GC_TEST")
                        .content(JsonUtil.writeValueAsString(modelRequests))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andExpect(jsonPath("$.code").value("999"))
                .andReturn();
        String msg = String.format(Locale.ROOT, Message.getInstance().getModelAliasDuplicated(),
                "new_MOdel, new_model");
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
    }

    @Test
    public void testValidateModelAlias() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("DEfault");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAlias("NMODEL_BASIc");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validate_model")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andExpect(jsonPath("$.code").value("000"))
                .andExpect(jsonPath("$.data").value("true"));
    }

    @Test
    public void testUpdateModelName() throws Exception {
        ModelUpdateRequest request = new ModelUpdateRequest();
        request.setNewModelName("UT_inner_JOIN_CUBE_PARTIAL");
        request.setProject("deFaUlt");

        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/name")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        String msg = String.format(Locale.ROOT, Message.getInstance().getModelAliasDuplicated(),
                "ut_inner_join_cube_partial");
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setProject("deFaUlt");
        request.setNewModelName("nmodel_BASIC");

        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/clone")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        String msg = String.format(Locale.ROOT, Message.getInstance().getModelAliasDuplicated(), "nmodel_basic");
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
    }

    @Test
    public void testOpenAPIBIExport() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/bi_export").param("model_name", modelName)
                .param("project", project).param("export_as", "TABLEAU_ODBC_TDS").param("element", "AGG_INDEX_COL")
                .param("dimensions", "").param("measures", "").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/bi_export").param("model_name", modelName)
                .param("project", project).param("export_as", "OTHER").param("element", "AGG_INDEX_COL")
                .param("dimensions", "").param("measures", "").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError());
    }

    @Test
    public void testBIExportByAdminUser() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/bi_export").param("model_name", modelName)
                .param("project", project).param("export_as", "TABLEAU_ODBC_TDS").param("element", "AGG_INDEX_COL")
                .param("dimensions", "").param("measures", "").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testExport() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/models/multi_level_partition/export")
                .param("model_name", modelName).param("project", project)
                .param("export_as", "TABLEAU_ODBC_TDS").param("element", "AGG_INDEX_COL")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
