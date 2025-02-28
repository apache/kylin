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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.SparkDDLController;
import org.apache.kylin.rest.request.OpenLogicalViewRequest;
import org.apache.kylin.rest.request.ViewRequest;
import org.apache.spark.ddl.DDLConstant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

@MetadataInfo
class OpenLogicalViewControllerTest {
    private MockMvc mockMvc;
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Mock
    private SparkDDLController sparkDDLController = Mockito.spy(SparkDDLController.class);

    @InjectMocks
    private OpenLogicalViewController openLogicalViewController = Mockito.spy(new OpenLogicalViewController());

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openLogicalViewController)
                .defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    void executeSqlCreate() throws Exception {
        OpenLogicalViewRequest request = new OpenLogicalViewRequest();
        request.setSql("CREATE LOGICAL VIEW your_logical_view AS select * from your_loaded_table");
        request.setProject("default");
        request.setRestrict(DDLConstant.LOGICAL_VIEW);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/logical_view/ddl").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        val viewRequest = new ViewRequest(request.getProject(), request.getSql(), request.getRestrict());
        Mockito.verify(sparkDDLController).executeSQL(viewRequest);
    }

    @Test
    void executeSqlReplace() throws Exception {
        OpenLogicalViewRequest request = new OpenLogicalViewRequest();
        request.setSql("REPLACE LOGICAL VIEW your_logical_view AS select * from your_loaded_table");
        request.setProject("default");
        request.setRestrict(DDLConstant.REPLACE_LOGICAL_VIEW);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/logical_view/ddl").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        val viewRequest = new ViewRequest(request.getProject(), request.getSql(), request.getRestrict());
        Mockito.verify(sparkDDLController).executeSQL(viewRequest);
    }

    @Test
    void executeSqlError() throws Exception {
        OpenLogicalViewRequest request = new OpenLogicalViewRequest();
        request.setSql("CREATE LOGICAL VIEW your_logical_view AS select * from your_loaded_table");
        request.setProject("default");
        request.setRestrict(DDLConstant.HIVE_VIEW);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/logical_view/ddl").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
    }
}
