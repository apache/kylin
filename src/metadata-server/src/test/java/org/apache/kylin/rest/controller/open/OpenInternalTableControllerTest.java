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
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.controller.InternalTableController;
import org.apache.kylin.rest.request.InternalTableRequest;
import org.apache.kylin.rest.service.InternalTableService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PrepareForTest({ KylinConfig.class, NProjectManager.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
public class OpenInternalTableControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private InternalTableController internalTableController;

    @Mock
    private InternalTableService internalTableService;

    @InjectMocks
    private OpenInternalTableController openInternalTableController = Mockito.spy(new OpenInternalTableController());

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openInternalTableController)
                .defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

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
    public void testCreateInternalTable() throws Exception {
        testRequest(MockMvcRequestBuilders.post("/api/internal_tables/{database}/{table}?project=default", "SSB",
                "LINEORDER"), true, matchOK());

        testRequest(MockMvcRequestBuilders.post("/api/internal_tables/{database}/{table}?project=default", " ",
                "LINEORDER"), true, match5xxServerError());

        testRequest(MockMvcRequestBuilders.post("/api/internal_tables/{database}/{table}?project=default", "SSB", " "),
                true, match5xxServerError());

        testRequest(MockMvcRequestBuilders.post("/api/internal_tables/{database}/{table}?project=default", " ", " "),
                true, match5xxServerError());
    }

    @Test
    public void testGetTableList() throws Exception {
        testRequest(MockMvcRequestBuilders.get("/api/internal_tables?project=default"), false, matchOK());

        testRequest(MockMvcRequestBuilders.get("/api/internal_tables?project=default&page_offset=-1&page_size=-1"),
                false, match5xxServerError());
    }

    @Test
    public void testGetTableDetail() throws Exception {
        testRequest(MockMvcRequestBuilders.get("/api/internal_tables/{database}/{table}?project=default", "SSB",
                "LINEORDER"), false, matchOK());

        testRequest(
                MockMvcRequestBuilders.get("/api/internal_tables/{database}/{table}?project=default", " ", "LINEORDER"),
                false, match5xxServerError());

        testRequest(MockMvcRequestBuilders.get("/api/internal_tables/{database}/{table}?project=default", "SSB", " "),
                false, match5xxServerError());

        testRequest(MockMvcRequestBuilders.get("/api/internal_tables/{database}/{table}?project=default", " ", " "),
                false, match5xxServerError());

        testRequest(MockMvcRequestBuilders.get(
                "/api/internal_tables/{database}/{table}?project=default&page_offset=-1&page_size=-1", "SSB",
                "LINEORDER"), false, match5xxServerError());
    }

    @Test
    public void testUpdateTable() throws Exception {
        testRequest(MockMvcRequestBuilders.put("/api/internal_tables/{database}/{table}?project=default", "SSB",
                "LINEORDER"), true, matchOK());

        testRequest(
                MockMvcRequestBuilders.put("/api/internal_tables/{database}/{table}?project=default", " ", "LINEORDER"),
                true, match5xxServerError());

        testRequest(MockMvcRequestBuilders.put("/api/internal_tables/{database}/{table}?project=default", "SSB", " "),
                true, match5xxServerError());
    }

    @Test
    public void testLoadIntoInternalTable() throws Exception {
        testRequest(MockMvcRequestBuilders.post("/api/internal_tables/{project}/{database}/{table}", "default", "SSB",
                "LINEORDER"), true, matchOK());

        testRequest(MockMvcRequestBuilders.post("/api/internal_tables/{project}/{database}/{table}", "default", " ",
                "LINEORDER"), true, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders.post("/api/internal_tables/{project}/{database}/{table}", "default", "SSB", " "),
                true, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders.post("/api/internal_tables/{project}/{database}/{table}", "default", " ", " "),
                true, match5xxServerError());

    }

    @Test
    public void testTruncateInternalTable() throws Exception {
        testRequest(
                MockMvcRequestBuilders.delete(
                        "/api/internal_tables/truncate_internal_table?project=default&database=SSB&table=LINEORDER"),
                false, matchOK());

        testRequest(
                MockMvcRequestBuilders
                        .delete("/api/internal_tables/truncate_internal_table?project=default&database=SSB"),
                false, match4xxClientError());

        testRequest(
                MockMvcRequestBuilders
                        .delete("/api/internal_tables/truncate_internal_table?project=default&table=LINEORDER"),
                false, match4xxClientError());

        testRequest(
                MockMvcRequestBuilders
                        .delete("/api/internal_tables/truncate_internal_table?project=default&database=SSB&table="),
                false, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders.delete(
                        "/api/internal_tables/truncate_internal_table?project=default&database=&table=LINEORDER"),
                false, match5xxServerError());
    }

    @Test
    public void testDropPartitions() throws Exception {
        testRequest(MockMvcRequestBuilders.delete(
                "/api/internal_tables/partitions?project=default&database=SSB&table=LINEORDER&partitions=a,b,c"), false,
                matchOK());

        testRequest(
                MockMvcRequestBuilders.delete(
                        "/api/internal_tables/partitions?project=default&database=SSB&table=LINEORDER&partitions="),
                false, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders
                        .delete("/api/internal_tables/partitions?project=default&database=SSB&table=LINEORDER"),
                false, match4xxClientError());

        testRequest(
                MockMvcRequestBuilders
                        .delete("/api/internal_tables/partitions?project=default&database=SSB&table=&partitions=a,b,c"),
                false, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders.delete(
                        "/api/internal_tables/partitions?project=default&database=&table=LINEORDER&partitions=a,b,c"),
                false, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders
                        .delete("/api/internal_tables/partitions?project=default&database=&table=&partitions=a,b,c"),
                false, match5xxServerError());

    }

    @Test
    public void testDropInternalTable() throws Exception {
        testRequest(MockMvcRequestBuilders.delete("/api/internal_tables/{database}/{table}?project=default", "SSB",
                "LINEORDER"), false, matchOK());

        testRequest(MockMvcRequestBuilders.delete("/api/internal_tables/{database}/{table}?project=default", " ",
                "LINEORDER"), false, match5xxServerError());

        testRequest(
                MockMvcRequestBuilders.delete("/api/internal_tables/{database}/{table}?project=default", "SSB", " "),
                false, match5xxServerError());
    }

    private void testRequest(MockHttpServletRequestBuilder builders, boolean withRequestBody,
            ResultMatcher resultMatcher) throws Exception {
        builders = builders.contentType(MediaType.APPLICATION_JSON);
        if (withRequestBody) {
            InternalTableRequest internalTableRequest = new InternalTableRequest();
            builders.content(JsonUtil.writeValueAsString(internalTableRequest));
        }
        builders = builders.accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON));
        mockMvc.perform(builders).andExpect(resultMatcher).andReturn();
    }

    private ResultMatcher matchOK() {
        return MockMvcResultMatchers.status().isOk();
    }

    private ResultMatcher match5xxServerError() {
        return MockMvcResultMatchers.status().is5xxServerError();
    }

    private ResultMatcher match4xxClientError() {
        return MockMvcResultMatchers.status().is4xxClientError();
    }

}
