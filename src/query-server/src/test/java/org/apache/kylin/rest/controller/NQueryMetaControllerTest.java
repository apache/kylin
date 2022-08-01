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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_EARLY_JSON;

import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.controller.v2.NQueryMetaController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;

class NQueryMetaControllerTest {
    private MockMvc mockMvc;

    @InjectMocks
    private NQueryMetaController queryMetaController = Mockito.spy(new NQueryMetaController());
    @Mock
    private QueryService queryService;

    @BeforeEach
    private void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(queryMetaController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
    }

    @Test
    void testGetMetadata() throws Exception {
        Mockito.when(queryService.getMetadata("default", "test")).thenReturn(Lists.newArrayList());

        ResultActions result = mockMvc.perform(MockMvcRequestBuilders.get("/api/tables_and_columns")//
                .param("project", "default") //
                .param("cube", "test") //
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_EARLY_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Assertions.assertEquals(0, result.andReturn().getResponse().getContentLength());
    }

    @Test
    void testGetMetadataWhenModelIsNull() throws Exception {
        Mockito.when(queryService.getMetadata("default")).thenReturn(Lists.newArrayList());

        ResultActions result = mockMvc.perform(MockMvcRequestBuilders.get("/api/tables_and_columns") //
                .param("project", "default") //
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_EARLY_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Assertions.assertEquals(0, result.andReturn().getResponse().getContentLength());
    }
}
