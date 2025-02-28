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

import java.io.File;
import java.nio.file.Files;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.request.SQLValidateRequest;
import org.apache.kylin.rest.service.FavoriteRuleService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class FavoriteQueryControllerTest extends NLocalFileMetadataTestCase {

    private final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private FavoriteRuleService favoriteRuleService;
    @InjectMocks
    private final FavoriteQueryController favoriteQueryController = Mockito.spy(new FavoriteQueryController());

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(favoriteQueryController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testImportSqls() throws Exception {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        MockMultipartFile file = new MockMultipartFile("files", "sqls.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql").toPath()));
        MockMultipartFile file2 = new MockMultipartFile("files", "sqls.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt").toPath()));
        mockMvc.perform(MockMvcRequestBuilders.multipart("/api/query/favorite_queries/sql_files").file(file).file(file2)
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).importSqls(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testSqlValidate() throws Exception {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        SQLValidateRequest request = new SQLValidateRequest(PROJECT, "sql");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/sql_validation")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).sqlValidate(Mockito.any(SQLValidateRequest.class));
    }
}
