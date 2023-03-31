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
import org.apache.kylin.rest.service.QueryResourceService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class QueryResourceControllerTest {

    private MockMvc mockMvc;

    @InjectMocks
    private QueryResourceController queryResourceController = Mockito.spy(new QueryResourceController());
    @Mock
    private QueryResourceService queryResourceService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(queryResourceController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        ReflectionTestUtils.setField(queryResourceController, "queryResourceService", queryResourceService);
    }

    @Test
    public void testAdjustQueryResource() throws Exception {
        QueryResourceService.QueryResource queryResource = new QueryResourceService.QueryResource();
        queryResource.setInstance(1);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource/query/adjust").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(queryResource))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(queryResourceController).adjustQueryResource(Mockito.any());
        Mockito.when(queryResourceService.isAvailable()).thenReturn(true);
        Mockito.when(queryResourceService.adjustQueryResource(queryResource)).thenReturn(queryResource);
        Assert.assertEquals(queryResource.getInstance(),
                queryResourceController.adjustQueryResource(queryResource).getInstance());
    }

    @Test
    public void testGetExecutorSize() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/resource/query/executor")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(queryResourceController).getExecutorSize();
        Mockito.when(queryResourceService.isAvailable()).thenReturn(true);
        Mockito.when(queryResourceService.getExecutorSize()).thenReturn(1);
        Assert.assertEquals(1, queryResourceController.getExecutorSize());
    }
}
