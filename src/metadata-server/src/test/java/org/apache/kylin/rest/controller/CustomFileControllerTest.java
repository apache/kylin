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
import java.nio.file.Paths;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.CustomFileService;
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
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class CustomFileControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private CustomFileService customFileService = Mockito.spy(CustomFileService.class);

    @InjectMocks
    private CustomFileController customFileController = Mockito.spy(new CustomFileController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static final String JAR_TYPE = "STREAMING_CUSTOM_PARSER";
    private static String JAR_ABS_PATH;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(customFileController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(customFileController, "customFileService", customFileService);
    }

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
        initJar();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private void initJar() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // ../examples/test_data/21767/metadata
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toString();
    }

    @Test
    public void testUploadNormal() throws Exception {
        MockMultipartFile jarFile = new MockMultipartFile("file", JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/custom/jar").file(jarFile)
                .contentType(MediaType.MULTIPART_FORM_DATA).param("project", PROJECT).param("jar_type", JAR_TYPE)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(customFileController).upload(Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testUploadEmpty() throws Exception {
        MockMultipartFile jarFile2 = new MockMultipartFile("file", "", "", (byte[]) null);
        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/custom/jar").file(jarFile2)
                .contentType(MediaType.MULTIPART_FORM_DATA).param("project", PROJECT).param("jar_type", JAR_TYPE)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(customFileController).upload(Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testRemoveJara() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/custom/jar").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("jar_name", JAR_NAME).param("jar_type", JAR_TYPE)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(customFileController).removeJar(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }
}
