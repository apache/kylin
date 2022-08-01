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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.SnapshotStatus;
import org.apache.kylin.rest.request.SnapshotRequest;
import org.apache.kylin.rest.request.SnapshotTableConfigRequest;
import org.apache.kylin.rest.request.TablePartitionsRequest;
import org.apache.kylin.rest.request.TableReloadPartitionColRequest;
import org.apache.kylin.rest.service.SnapshotService;
import org.junit.After;
import org.junit.Assert;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SnapshotControllerTest extends NLocalFileMetadataTestCase {

    private static final String APPLICATION_PUBLIC_JSON = HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

    private MockMvc mockMvc;

    @Mock
    private SnapshotService snapshotService;

    @InjectMocks
    private final SnapshotController snapshotController = Mockito.spy(new SnapshotController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(snapshotController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSnapshot() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);

        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables,
                Maps.newHashMap(), false, 3, null, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).buildSnapshotsManually(Mockito.any(SnapshotRequest.class));
    }

    @Test
    public void testBuildSnapshotFail() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet();
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);
        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables,
                Maps.newHashMap(), false, 3, null, null);
        final MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Mockito.verify(snapshotController).buildSnapshotsManually(Mockito.any(SnapshotRequest.class));
        String errorMsg = "KE-010000005(Empty Parameter):You should select at least one table or database to load!!";
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testGetSnapshotPartitionValues() throws Exception {
        TablePartitionsRequest request = new TablePartitionsRequest();
        request.setProject("default");
        Map<String, String> tableCols = Maps.newHashMap();
        tableCols.put("db1.t1", "col1");
        tableCols.put("db1.t2", "col2");
        request.setTableCols(tableCols);
        final MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/snapshots/partitions")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(snapshotController).getSnapshotPartitionValues(Mockito.any(TablePartitionsRequest.class));
    }

    @Test
    public void testRefreshSnapshot() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);

        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables,
                Maps.newHashMap(), false, 3, null, null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).refreshSnapshotsManually(Mockito.any(SnapshotRequest.class));
    }

    @Test
    public void testRefreshSnapshotFail() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet();
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);
        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables,
                Maps.newHashMap(), false, 3, null, null);
        final MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Mockito.verify(snapshotController).refreshSnapshotsManually(Mockito.any(SnapshotRequest.class));
        String errorMsg = "KE-010000005(Empty Parameter):You should select at least one table or database to load!!";
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testCheckBeforeDeleteSnapshot() throws Exception {
        String project = "default";
        Set<String> deleteSnapshot = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(deleteSnapshot);

        Mockito.doAnswer(x -> null).when(snapshotService).checkBeforeDeleteSnapshots(project, deleteSnapshot);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots/check_before_delete")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).checkBeforeDelete(Mockito.any(SnapshotRequest.class));
    }

    @Test
    public void testDeleteSnapshot() throws Exception {
        String project = "default";
        Set<String> deleteSnapshot = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(deleteSnapshot);

        Mockito.doAnswer(x -> null).when(snapshotService).checkBeforeDeleteSnapshots(project, deleteSnapshot);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/snapshots").param("project", project)
                .param("tables", "TEST_ACCOUNT").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).deleteSnapshots(project, deleteSnapshot);
    }

    @Test
    public void testGetSnapshots() throws Exception {
        String project = "default";
        String table = "";
        Set<SnapshotStatus> statusFilter = Sets.newHashSet();
        String sortBy = "last_modified_time";
        boolean isReversed = true;
        Mockito.doAnswer(x -> null).when(snapshotService).getProjectSnapshots(project, table, statusFilter,
                Sets.newHashSet(), sortBy, isReversed);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots").param("project", project)
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).getSnapshots(project, table, 0, 10, statusFilter, Sets.newHashSet(), sortBy,
                isReversed);
    }

    @Test
    public void testGetSnapshotsWithInvalidSortBy() throws Exception {
        String project = "default";
        String table = "";
        Set<SnapshotStatus> statusFilter = Sets.newHashSet();
        String sortBy = "UNKNOWN";
        boolean isReversed = true;
        Mockito.doAnswer(x -> null).when(snapshotService).getProjectSnapshots(project, table, statusFilter, null,
                sortBy, isReversed);
        final MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/snapshots").param("project", project).param("sort_by", sortBy)
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(snapshotController).getSnapshots(project, table, 0, 10, statusFilter, Sets.newHashSet(), sortBy,
                isReversed);
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        String errorMsg = "KE-010000003(Invalid Parameter):No field called 'UNKNOWN'.";
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testTables() throws Exception {
        String project = "default";
        Mockito.doAnswer(x -> null).when(snapshotService).getTables(project, "", 0, 10);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots/tables").param("project", project)
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).getTables(project, "", 0, 10);
    }

    @Test
    public void testLoadMoreTables() throws Exception {
        String project = "default";
        String database = "SSB";
        String table = "";
        Mockito.doAnswer(x -> null).when(snapshotService).getTableNameResponses(project, database, table);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots/tables/more").param("project", project)
                .param("database", database).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).loadMoreTables(project, table, database, 0, 10);
    }

    @Test
    public void testReloadSnapshotCols() throws Exception {
        String project = "default";
        String table = "";
        TableReloadPartitionColRequest request = new TableReloadPartitionColRequest();
        request.setProject(project);
        request.setTable(table);
        Mockito.doAnswer(x -> null).when(snapshotService).reloadPartitionCol(project, table);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots/reload_partition_col")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).getSnapshotCols(Mockito.any(TableReloadPartitionColRequest.class));
    }

    @Test
    public void testGetSnapshotCols() throws Exception {
        String project = "default";
        Set<String> databases = Sets.newHashSet();
        Set<String> tables = Sets.newHashSet();
        String tablePattern = "";
        Mockito.doAnswer(x -> null).when(snapshotService).getSnapshotCol(project, databases, tables, tablePattern,
                true);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots/config").param("project", project)
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        //        Mockito.verify(snapshotController).getSnapshotCols(project, databases, tables, tablePattern, true, 0, 10,false);
    }

    @Test
    public void testConfigSnapshotPartitionCol() throws Exception {
        String project = "default";
        Map<String, String> tablePartitionCol = Maps.newHashMap();
        SnapshotTableConfigRequest configRequest = new SnapshotTableConfigRequest();
        configRequest.setProject(project);
        configRequest.setTablePartitionCol(tablePartitionCol);
        Mockito.doAnswer(x -> null).when(snapshotService).configSnapshotPartitionCol(project, tablePartitionCol);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots/config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(configRequest))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotController).configSnapshotPartitionCol(Mockito.any(SnapshotTableConfigRequest.class));
    }
}
