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
import java.util.List;
import java.util.Random;

import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.DiagPackageRequest;
import org.apache.kylin.rest.request.DiagProgressRequest;
import org.apache.kylin.rest.response.MaintenanceModeResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.service.MaintenanceModeService;
import org.apache.kylin.rest.service.SystemService;
import org.junit.After;
import org.junit.Assert;
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

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class NSystemControllerTest extends NLocalFileMetadataTestCase {
    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private SystemService systemService = Mockito.spy(new SystemService());

    @InjectMocks
    private NSystemController nSystemController = Mockito.spy(new NSystemController());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    private ClusterManager clusterManager;

    @Mock
    private MaintenanceModeService maintenanceModeService;

    @Before
    public void setUp() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nSystemController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testDumpDiagPackage() throws Exception {
        DiagPackageRequest request = new DiagPackageRequest();
        final long end = System.currentTimeMillis();
        final long start = end - 1000L * 60 * 10;
        request.setStart(String.valueOf(start));
        request.setEnd(String.valueOf(end));
        Mockito.doAnswer(x -> null).when(systemService).dumpLocalDiagPackage(Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)).param("host", "")
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).getRemoteDumpDiagPackage(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testRemoteDumpDiagPackage() throws Exception {
        DiagPackageRequest request = new DiagPackageRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(), Mockito.any());
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

        Mockito.verify(nSystemController, Mockito.times(10)).getRemoteDumpDiagPackage(Mockito.any(),
                Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetDumpDiagPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(systemService).getExtractorStatus(Mockito.any(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag/status").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "").param("project", "project")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).getRemotePackageStatus(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any());
        NBasicController controller = new NBasicController();
        String encodeHost = controller.encodeHost("quickstart.cloudera:8088");
        String decodeHost = controller.decodeHost(encodeHost);
        controller.encodeHost("");
        controller.decodeHost("");
        Assert.assertTrue(decodeHost.equalsIgnoreCase("http://quickstart.cloudera:8088"));
        Assert.assertEquals(
            "http://quickstart.cloudera:8088",
            controller.decodeHost("http://quickstart.cloudera:8088"));
    }

    @Test
    public void testGetRemoteDumpDiagPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(),
                Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag/status").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "ip").param("project", "project")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).getRemotePackageStatus(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDownloadPackage() throws Exception {
        Mockito.when(systemService.getDiagPackagePath(Mockito.anyString(), Mockito.anyString())).thenReturn("");
        Mockito.doNothing().when(nSystemController).setDownloadResponse(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "").param("project", "project")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).remoteDownloadPackage(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testRemoteDownloadPackage() throws Exception {
        Mockito.doNothing().when(nSystemController).downloadFromRemoteHost(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "ip").param("project", "project")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).remoteDownloadPackage(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetRemoteDumpQueryDiagPackage() throws Exception {
        DiagPackageRequest request = new DiagPackageRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag/query").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).getRemoteDumpQueryDiagPackage(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testRemoteStopPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(),
                Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("host", "ip").param("id", "id").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).remoteStopPackage(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testUpdateDiagProgress() throws Exception {
        DiagProgressRequest request = new DiagProgressRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).updateDiagProgress(Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/system/diag/progress").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).updateDiagProgress(Mockito.any());
    }

    @Test
    public void testRollEventLog() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/system/roll_event_log").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).rollEventLog();
    }

    @Test
    public void testGetHostname() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/host").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController, Mockito.times(1)).getHostname();
    }

    @Test
    public void testSimulationInsert_TurnON() throws Exception {
        getTestConfig().setProperty("kylin.env.unitofwork-simulation-enabled", KylinConfigBase.TRUE);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/transaction/simulation/insert_meta")
                .contentType(MediaType.APPLICATION_JSON).param("count", "10").param("sleepSec", "1")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        Mockito.verify(nSystemController).simulateInsertMeta(Mockito.anyInt(), Mockito.anyLong());
    }

    @Test
    public void testSimulationInsert_TurnOff() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/transaction/simulation/insert_meta")
                .contentType(MediaType.APPLICATION_JSON).param("count", "10").param("sleepSec", "100")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).simulateInsertMeta(Mockito.anyInt(), Mockito.anyLong());
    }

    @Test
    public void testGetServer() throws Exception {
        ServerInfoResponse response = new ServerInfoResponse();
        response.setHost("172.168.1.1");
        response.setMode("ALL");
        List<ServerInfoResponse> result = Lists.newArrayList(response);
        Mockito.when(clusterManager.getServers()).thenReturn(result);
        Mockito.when(maintenanceModeService.getMaintenanceMode())
            .thenReturn(new MaintenanceModeResponse(false, ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/servers")
            .param("ext", "true").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
            .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
