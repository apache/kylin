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

import java.util.Arrays;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.SystemPropertiesCache;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.request.EpochRequest;
import org.apache.kylin.rest.service.EpochService;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

public class NEpochControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private EpochService epochService;

    @InjectMocks
    private NEpochController nEpochController = Mockito.spy(new NEpochController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nEpochController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        SystemPropertiesCache.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateEpochOwner() throws Exception {
        val request = mockStreamingEpochRequest();
        val mapRequest1 = mockStreamingEpochRequestMap1();
        val mapRequest2 = mockStreamingEpochRequestMap2();

        mockMvc.perform(MockMvcRequestBuilders.post("/api/epoch")
                .content(JsonUtil.writeValueAsString(mapRequest1))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/epoch")
                .content(JsonUtil.writeValueAsString(mapRequest2))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        request.setProjects(Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/epoch")
                .content(JsonUtil.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        request.setProjects(Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/epoch")
                .content(JsonUtil.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        request.setProjects(Arrays.asList("DEFAULT"));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/epoch")
                .content(JsonUtil.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateAllEpochOwner() throws Exception {
        val request = mockStreamingEpochRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/epoch/all")
                .content(JsonUtil.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nEpochController).updateAllEpochOwner(Mockito.any(request.getClass()));
    }

    @Test
    public void testIsMaintenanceMode() throws Exception {
        Mockito.doReturn(true).when(epochService).isMaintenanceMode();
        mockMvc.perform(MockMvcRequestBuilders.get("/api/epoch/maintenance_mode")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nEpochController).isMaintenanceMode();
    }

    private EpochRequest mockStreamingEpochRequest() {
        val request = new EpochRequest();
        request.setProjects(Arrays.asList("test"));
        request.setForce(false);
        return request;
    }

    private Map<String, Object> mockStreamingEpochRequestMap1() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("projects", "abc");
        return map;
    }

    private Map<String, Object> mockStreamingEpochRequestMap2() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("force", "true");
        return map;
    }


}
