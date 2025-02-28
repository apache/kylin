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

import static org.mockito.ArgumentMatchers.eq;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.util.JobDriverUIUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;


public class JobDriverUIControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Mock
    private JobDriverUIUtil jobDriverUIUtil;

    @InjectMocks
    private JobDriverUIController jobDriverUIController = Mockito.spy(new JobDriverUIController());

    @Before
    public void setUp() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(jobDriverUIController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
    }
    
    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testProxy() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get(JobDriverUIUtil.DRIVER_UI_BASE + "/project/step"))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobDriverUIUtil).proxy(eq("project"), eq("step"), Mockito.any(HttpServletRequest.class),
                Mockito.any(HttpServletResponse.class));
    }
}
