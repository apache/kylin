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

package org.apache.kylin.rest.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.val;

@MetadataInfo
class CheckSourceTableRunnableTest {
    private final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);

    @Test
    void checkTable() throws JsonProcessingException {
        val thread = new CheckSourceTableRunnable();
        thread.setProject("project");
        thread.setConfig(KylinConfig.readSystemKylinConfig());
        thread.setTableIdentity("default.table");
        thread.setRestTemplate(restTemplate);
        val response = new SnapshotSourceTableStatsResponse();
        response.setNeedRefresh(true);
        response.setNeedRefreshPartitionsValue(Sets.newHashSet("123"));
        val restResult = JsonUtil.writeValueAsString(RestResponse.ok(response));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
        thread.checkTable();

        val checkSourceTableQueue = thread.getCheckSourceTableQueue();
        assertEquals(1, checkSourceTableQueue.size());
        val checkSourceTableResult = Objects.requireNonNull(checkSourceTableQueue.poll());
        assertTrue(checkSourceTableResult.getNeedRefresh());
        assertEquals(1, checkSourceTableResult.getNeedRefreshPartitionsValue().size());
        assertTrue(checkSourceTableResult.getNeedRefreshPartitionsValue().contains("123"));
    }

    @Test
    void checkTableFailed() {
        try {
            val thread = new CheckSourceTableRunnable();
            thread.setProject("project");
            thread.setConfig(KylinConfig.readSystemKylinConfig());
            thread.setTableIdentity("default.table");
            thread.setRestTemplate(restTemplate);
            val resp = new ResponseEntity<>("", HttpStatus.OK);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
            thread.checkTable();
        } catch (Exception e) {
            assertTrue(e instanceof KylinRuntimeException);
            assertEquals("Project[project] Snapshot source table[default.table] check table stats Failed",
                    e.getMessage());
        }
    }
}
