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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.IndexGlutenCacheRequest;
import org.apache.kylin.rest.request.InternalTableGlutenCacheRequest;
import org.apache.kylin.rest.response.GlutenCacheResponse;
import org.apache.kylin.rest.service.GlutenCacheService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;

public class GlutenCacheControllerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private GlutenCacheService glutenCacheService;

    @InjectMocks
    private GlutenCacheController glutenCacheController = Mockito.spy(new GlutenCacheController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(glutenCacheController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();

        ReflectionTestUtils.setField(glutenCacheController, "glutenCacheService", glutenCacheService);
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void glutenCache() throws Exception {
        val request = Lists.newArrayList("test command");
        val glutenResponse = new GlutenCacheResponse(true, Lists.newArrayList(), "");
        Mockito.when(glutenCacheService.glutenCache(request)).thenReturn(glutenResponse);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/cache/gluten_cache") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(glutenCacheController).glutenCache(request);
    }

    @Test
    public void glutenCacheAsync() throws Exception {
        val request = Lists.newArrayList("test command");
        val glutenResponse = new GlutenCacheResponse(true, Lists.newArrayList(), "");
        Mockito.when(glutenCacheService.glutenCache(request)).thenReturn(glutenResponse);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/cache/gluten_cache_async") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(glutenCacheController).glutenCacheAsync(request);
    }

    @Test
    public void internalTableGlutenCache() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new InternalTableGlutenCacheRequest();
        request.setProject(PROJECT);
        request.setDatabase("database");
        request.setTable("table");
        Mockito.doNothing().when(glutenCacheService).internalTableGlutenCache(request, servletRequest);
        val mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/internal_table/gluten_cache") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(glutenCacheController).internalTableGlutenCache(request, mvcResult.getRequest());
    }

    @Test
    public void indexGlutenCache() throws Exception {
        val servletRequest = new MockHttpServletRequest();
        val request = new IndexGlutenCacheRequest();
        request.setProject(PROJECT);
        request.setModel("model");
        Mockito.doNothing().when(glutenCacheService).indexGlutenCache(request, servletRequest);
        val mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/index/gluten_cache") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(glutenCacheController).indexGlutenCache(request, mvcResult.getRequest());
    }
}
