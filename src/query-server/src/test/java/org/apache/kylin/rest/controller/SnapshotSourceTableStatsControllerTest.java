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

import lombok.val;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.request.SnapshotSourceTableStatsRequest;
import org.apache.kylin.rest.response.SnapshotSourceTableStatsResponse;
import org.apache.kylin.rest.service.SnapshotSourceTableStatsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.mockito.Mockito.when;

@MetadataInfo
class SnapshotSourceTableStatsControllerTest {
    private MockMvc mockMvc;
    private AutoCloseable autoCloseable;

    @Mock
    private SnapshotSourceTableStatsService snapshotSourceTableStatsService;

    @InjectMocks
    private SnapshotSourceTableStatsController snapshotSourceTableStatsController = Mockito
            .spy(new SnapshotSourceTableStatsController());

    @BeforeEach
    public void setup() {
        autoCloseable = MockitoAnnotations.openMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(snapshotSourceTableStatsController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
    }

    @AfterEach
    public void destroy() throws Exception {
        autoCloseable.close();
    }

    private SnapshotSourceTableStatsRequest mockSnapshotSourceTableStatsRequest() {
        val request = new SnapshotSourceTableStatsRequest();
        request.setProject("default");
        request.setTable("table");
        request.setDatabase("database");
        request.setSnapshotPartitionCol("partition");
        return request;
    }

    @Test
    void sourceTableStats() throws Exception {
        val request = mockSnapshotSourceTableStatsRequest();
        val response = Mockito.mock(SnapshotSourceTableStatsResponse.class);
        when(snapshotSourceTableStatsService.checkSourceTableStats(request.getProject(),
                request.getDatabase(), request.getTable(), request.getSnapshotPartitionCol())).thenReturn(response);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots/source_table_stats")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotSourceTableStatsController).sourceTableStats(Mockito.any());
    }

    @Test
    void getSnapshotSourceTableStats() throws Exception {
        val request = mockSnapshotSourceTableStatsRequest();
        when(snapshotSourceTableStatsService.saveSnapshotViewMapping(request.getProject())).thenReturn(true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots/view_mapping")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(snapshotSourceTableStatsController).saveSnapshotViewMapping(Mockito.any());
    }
}
