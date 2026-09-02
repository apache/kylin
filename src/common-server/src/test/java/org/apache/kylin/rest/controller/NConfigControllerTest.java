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

import java.util.Properties;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.apache.kylin.rest.service.ConfigService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NConfigControllerTest extends NLocalFileMetadataTestCase {
    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private ConfigService configService;

    @InjectMocks
    private NConfigController nConfigController = Mockito.spy(new NConfigController());

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nConfigController) //
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

    }

    @Before
    public void setupResource() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testIsCloud() throws Exception {

        mockMvc.perform(MockMvcRequestBuilders.get("/api/config/is_cloud") //
                .contentType(MediaType.APPLICATION_JSON) // //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nConfigController).isCloud();
        Mockito.verify(configService).isCloud();
    }

    @Test
    public void testFetchAll() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("kylin.env.channel", "on-premises");
        Mockito.doReturn(properties).when(configService).fetchAll();

        mockMvc.perform(MockMvcRequestBuilders.get("/api/config/all") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data['kylin.env.channel']").value("on-premises"));
        Mockito.verify(nConfigController).fetchAll();
    }

}
