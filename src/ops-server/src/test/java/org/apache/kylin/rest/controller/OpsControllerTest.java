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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Random;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.DiagPackageRequest;
import org.apache.kylin.rest.request.DiagProgressRequest;
import org.apache.kylin.rest.service.SystemService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class OpsControllerTest extends NLocalFileMetadataTestCase {
    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private SystemService systemService;

    @InjectMocks
    private OpsController opsController = Mockito.spy(new OpsController());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Before
    public void setUp() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(opsController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        getTestConfig().setProperty("spring.application.name", "");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRemoteDumpDiagPackage() throws Exception {
        DiagPackageRequest request = new DiagPackageRequest();
        Mockito.doAnswer(x -> null).when(opsController).generateTaskForRemoteHost(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)).param("host", "ip")
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        request.setStart("start");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        request.setStart("-1");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        Random random = new Random();
        LocalDateTime randomStartLocalDateTime = LocalDateTime.now().minus(random.nextInt(100), ChronoUnit.MONTHS);
        request.setStart(
                String.valueOf(randomStartLocalDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        request.setEnd("end");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        request.setEnd("-2");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        final LocalDateTime endLocalDateTimeFiveMinute = randomStartLocalDateTime.plus(5, ChronoUnit.MINUTES);
        request.setEnd(String
                .valueOf(endLocalDateTimeFiveMinute.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() - 1));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        final LocalDateTime endLocalDateTimeOneMouth = randomStartLocalDateTime.plus(30, ChronoUnit.DAYS);
        request.setEnd(
                String.valueOf(endLocalDateTimeOneMouth.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() + 1));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        request.setEnd(
                String.valueOf(endLocalDateTimeFiveMinute.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());

        request.setEnd(
                String.valueOf(endLocalDateTimeOneMouth.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(opsController, Mockito.times(10)).getRemoteDumpDiagPackage(Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetRemoteDumpDiagPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(opsController).generateTaskForRemoteHost(Mockito.any(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag/status").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "ip").param("project", "project")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(opsController).getRemotePackageStatus(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testRemoteDownloadPackage() throws Exception {
        Mockito.doNothing().when(opsController).downloadFromRemoteHost(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "ip").param("project", "project")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(opsController).remoteDownloadPackage(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetRemoteDumpQueryDiagPackage() throws Exception {
        DiagPackageRequest request = new DiagPackageRequest();
        Mockito.doAnswer(x -> null).when(opsController).generateTaskForRemoteHost(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag/query").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(opsController).getRemoteDumpQueryDiagPackage(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any());
    }

    @Test
    public void testRemoteStopPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(opsController).generateTaskForRemoteHost(Mockito.any(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("host", "ip").param("id", "id").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(opsController).remoteStopPackage(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testUpdateDiagProgress() throws Exception {
        DiagProgressRequest request = new DiagProgressRequest();
        Mockito.doAnswer(x -> null).when(opsController).updateDiagProgress(Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/system/diag/progress").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(opsController).updateDiagProgress(Mockito.any());
    }
}
