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
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.DDLRequest;
import org.apache.kylin.rest.request.ExportTableRequest;
import org.apache.kylin.rest.service.SparkSourceService;
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

public class SparkSourceControllerTest {
    private MockMvc mockMvc;

    @Mock
    private SparkSourceService sparkSourceService;

    @InjectMocks
    private SparkSourceController sparkSourceController = Mockito.spy(new SparkSourceController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(sparkSourceController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testExecuteSQL() throws Exception {
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("show databases");
        ddlRequest.setDatabase("default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/spark_source/execute").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(ddlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).executeSQL(ddlRequest);
    }

    @Test
    public void testExportTableStructuree() throws Exception {
        ExportTableRequest request = new ExportTableRequest();
        request.setDatabases("SSB");
        request.setTables(new String[] { "LINEORDER", "DATES" });
        mockMvc.perform(MockMvcRequestBuilders.post("/api/spark_source/export_table_structure")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sparkSourceController).exportTableStructure(request);
    }

    @Test
    public void testDropTable() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/spark_source/{database}/tables/{table}", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).dropTable("default", "COUNTRY");
    }

    @Test
    public void testListDatabase() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/databases")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).listDatabase();
    }

    @Test
    public void testListTables() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/tables", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).listTables("default", "test");
    }

    @Test
    public void testListColumns() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/columns", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).listColumns("default", "COUNTRY");
    }

    @Test
    public void testGetTableDesc() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/desc", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).getTableDesc("default", "COUNTRY");
    }

    @Test
    public void testHasPartition() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/spark_source/{database}/{table}/has_partition", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).hasPartition("default", "COUNTRY");
    }

    @Test
    public void testDatabaseExists() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/exists", "default")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).databaseExists("default");
    }

    @Test
    public void testTableExists() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/exists", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).tableExists("default", "COUNTRY");
    }

    @Test
    public void testLoadSamples() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/load_samples")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).loadSamples();
    }

    @Test
    public void testMsck() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/spark_source/{database}/{table}/msck", "default", "COUNTRY")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(sparkSourceController).msck("default", "COUNTRY");
    }
}
