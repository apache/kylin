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

import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.MetadataBackupRequest;
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
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NSystemControllerTest extends NLocalFileMetadataTestCase {
    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private SystemService systemService = Mockito.spy(new SystemService());

    @InjectMocks
    private NSystemController nSystemController = Mockito.spy(new NSystemController());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

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
    public void broadcastMetadataBackup() throws Exception {
        MetadataBackupRequest request = new MetadataBackupRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).broadcastMetadataBackup(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/broadcast_metadata_backup")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).broadcastMetadataBackup(Mockito.any(MetadataBackupRequest.class),
                Mockito.any());
    }

    @Test
    public void broadcastMetadataBackupRequiresAdminOrServiceToken() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("analyst", "analyst", Constant.ROLE_ANALYST));
        MetadataBackupRequest request = new MetadataBackupRequest();
        Assert.assertThrows(KylinException.class,
                () -> nSystemController.broadcastMetadataBackup(request, new MockHttpServletRequest()));
    }

    @Test
    public void downloadMetadataBackTmpFile() throws Exception {
        MetadataBackupRequest request = new MetadataBackupRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).downloadMetadataBackTmpFile(Mockito.any(), Mockito.any(),
                Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/metadata_backup_tmp_file")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).downloadMetadataBackTmpFile(Mockito.any(MetadataBackupRequest.class),
                Mockito.any(), Mockito.any());
    }
}
