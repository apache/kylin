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

package org.apache.kylin.rest.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kylin.common.constant.Constants.METADATA_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.resourcegroup.KylinInstance;
import org.apache.kylin.metadata.resourcegroup.ResourceGroup;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.val;
import lombok.var;

@MetadataInfo
class ScheduleTenantTest {
    @InjectMocks
    private ScheduleService scheduleService = Mockito.spy(ScheduleService.class);
    @Mock
    private RestTemplate restTemplate = Mockito.spy(RestTemplate.class);
    @Mock
    private FileService fileService = Mockito.spy(FileService.class);
    @Mock
    private Appender appender = Mockito.mock(Appender.class);

    @BeforeEach
    public void beforeEach() throws JsonProcessingException {
        ReflectionTestUtils.setField(scheduleService, "restTemplate", restTemplate);
        ReflectionTestUtils.setField(scheduleService, "fileService", fileService);
        val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
        var resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.doReturn(resp).when(restTemplate).exchange(anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(HttpEntity.class), ArgumentMatchers.<Class<String>> any());

        Mockito.when(appender.getName()).thenReturn("mocked");
        Mockito.when(appender.isStarted()).thenReturn(true);
        ((Logger) LogManager.getRootLogger()).addAppender(appender);
    }

    @AfterEach
    public void afterEach() {
        ((Logger) LogManager.getRootLogger()).removeAppender(appender);
    }

    @Test
    @OverwriteProp(key = "kylin.multi-tenant.enabled", value = "true")
    void executeMetadataBackupInTenantMode() throws Exception {
        val rgManager = Mockito.mock(ResourceGroupManager.class);
        val resourceGroupJson = "{\"create_time\":1669192083398,\"instances\":[],\"mapping_info\":[],\"resource_groups\":[],\"uuid\":\"a5cca6c0-d80a-acee-6be6-94fd6022745d\",\"last_modified\":0,\"version\":\"4.0.0.0\",\"resource_group_enabled\":true}";
        Mockito.when(rgManager.getResourceGroup())
                .thenReturn(JsonUtil.readValue(resourceGroupJson, new TypeReference<ResourceGroup>() {
                }));
        try (val mockStatic = Mockito.mockStatic(ResourceGroupManager.class)) {
            val config = KylinConfig.getInstanceFromEnv();
            mockStatic.when(() -> ResourceGroupManager.getInstance(config)).thenReturn(rgManager);
            Mockito.when(rgManager.isResourceGroupEnabled()).thenReturn(true);
            val startTime = System.currentTimeMillis();
            AtomicReference<Pair<String, String>> backupFolder = new AtomicReference<>(null);
            scheduleService.executeMetadataBackupInTenantMode(config, startTime, backupFolder);

            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            var log = logCaptor.getAllValues().stream()
                    .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                    .filter(event -> event.getLevel().equals(Level.INFO))
                    .map(event -> event.getMessage().getFormattedMessage()).findFirst()
                    .orElseThrow(AssertionError::new);
            assertTrue(StringUtils.contains(log, "ResourceGroupServerNode :"));
        }
    }

    @Test
    @OverwriteProp(key = "kylin.multi-tenant.enabled", value = "true")
    void executeMetadataBackupInTenantMode2() throws Exception {
        val resourceGroupJson = "{\"create_time\":1669704879469,\"instances\":[{\"instance\":\"10.1.2.185:7878\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"instance\":\"10.1.2.184:7878\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],"
                + "\"mapping_info\":[{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"BUILD\"},{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"QUERY\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"BUILD\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"QUERY\"}],\"resource_groups\":[{\"id\":\"c444879a-b3b0-4946-aed1-018cbc946c4a\"},{\"id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],\"uuid\":\"d5c316ed-b977-6efb-aea3-1735feb75d02\",\"last_modified\":1669952899667,\"version\":\"4.0.0.0\",\"resource_group_enabled\":true}\n";
        val rgManager = Mockito.mock(ResourceGroupManager.class);
        Mockito.when(rgManager.getResourceGroup())
                .thenReturn(JsonUtil.readValue(resourceGroupJson, new TypeReference<ResourceGroup>() {
                }));

        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val dir = RandomUtil.randomUUIDStr();
        val dirPath = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()), dir);
        val path = new Path(dirPath, METADATA_FILE);
        try (val out = fileSystem.create(path, true)) {
            out.write("123".getBytes(UTF_8));
        }
        var backupFolder = new AtomicReference<>(Pair.newPair(dirPath.toString(), dir));

        try (val mockStatic = Mockito.mockStatic(ResourceGroupManager.class);
                val mockStatic2 = Mockito.mockStatic(AddressUtil.class)) {
            mockStatic2.when(AddressUtil::getLocalInstance).thenReturn("10.1.2.185:7878");
            val config = KylinConfig.getInstanceFromEnv();
            ReflectionTestUtils.setField(scheduleService, "opsCronTimeout", config.getRoutineOpsTaskTimeOut());
            mockStatic.when(() -> ResourceGroupManager.getInstance(config)).thenReturn(rgManager);
            Mockito.when(rgManager.isResourceGroupEnabled()).thenReturn(true);
            val startTime = System.currentTimeMillis();
            scheduleService.executeMetadataBackupInTenantMode(config, startTime, backupFolder);

            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            val logs = logCaptor.getAllValues().stream()
                    .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                    .filter(event -> event.getLevel().equals(Level.INFO))
                    .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
            var log = logs.stream().findFirst().orElseThrow(AssertionError::new);
            assertTrue(StringUtils.contains(logs.get(0), "ResourceGroupServerNode :"));
            assertEquals("backup file path [" + dirPath + "] broadcast to server success", logs.get(logs.size() - 1));
        }
    }

    @Test
    void broadcastToServer() throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val dir = RandomUtil.randomUUIDStr();
        val dirPath = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()), dir);
        val path = new Path(dirPath, METADATA_FILE);
        try (val out = fileSystem.create(path, true)) {
            out.write("123".getBytes(UTF_8));
        }

        var backupFolder = new AtomicReference<>(Pair.newPair(dirPath.toString(), dir));
        scheduleService.broadcastToServer(Maps.newHashMap(), backupFolder, System.currentTimeMillis());

        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var logOption = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> event.getLevel().equals(Level.INFO))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst();
        assertFalse(logOption.isPresent());

        backupFolder = new AtomicReference<>(Pair.newPair(path.toString(), dir));
        scheduleService.broadcastToServer(Maps.newHashMap(), backupFolder, System.currentTimeMillis());
        logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        val log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> event.getLevel().equals(Level.ERROR))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        assertTrue(StringUtils.contains(log, "broadcast to server has error. reason:"));
    }

    @Test
    void broadcastToServerEmptyKylinInstances() throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val dir = RandomUtil.randomUUIDStr();
        val dirPath = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()), dir);
        val path = new Path(dirPath, METADATA_FILE);
        try (val out = fileSystem.create(path, true)) {
            out.write("123".getBytes(UTF_8));
        }

        val servers = Maps.<String, List<KylinInstance>> newHashMap();
        servers.put("test", Lists.newArrayList());
        var backupFolder = new AtomicReference<>(Pair.newPair(dirPath.toString(), dir));
        scheduleService.broadcastToServer(servers, backupFolder, System.currentTimeMillis());

        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var logOption = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> event.getLevel().equals(Level.INFO))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst();
        assertFalse(logOption.isPresent());
    }

    @Test
    void broadcastToTenantNodeBadResponse() {
        var resp = new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        broadcastToTenantNodeBadResponse(resp,
                "noticeToTenantNode failed, HttpStatus is " + HttpStatus.BAD_REQUEST.value());
    }

    @Test
    void broadcastToTenantNodeBadResponse2() throws JsonProcessingException {
        val restResult = JsonUtil.writeValueAsString(RestResponse.fail());
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        broadcastToTenantNodeBadResponse(resp, "noticeToTenantNode failed, response code is 200");
    }

    private void broadcastToTenantNodeBadResponse(ResponseEntity resp, String errorLog) {
        Mockito.doReturn(resp).when(restTemplate).exchange(anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(HttpEntity.class), ArgumentMatchers.<Class<String>> any());
        val logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        scheduleService.broadcastToTenantNode("test", "test", "test/metadata.zip", 0L, "127.0.0.1:7070");
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        val log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> event.getLevel().equals(Level.ERROR))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        assertEquals(errorLog, log);
    }

    @Test
    void broadcastToTenantNode() {
        ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        scheduleService.broadcastToTenantNode("test", "test", "test/metadata.zip", 0L, "127.0.0.1:7070");
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        val logCount = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> event.getLevel().equals(Level.ERROR)).count();
        assertEquals(0, logCount);

        var resp = new ResponseEntity<>(HttpStatus.OK);
        Mockito.doReturn(resp).when(restTemplate).exchange(anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(HttpEntity.class), ArgumentMatchers.<Class<String>> any());
        logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        scheduleService.broadcastToTenantNode("test", "test", "test/metadata.zip", 0L, "127.0.0.1:7070");
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        var log = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> event.getLevel().equals(Level.ERROR))
                .map(event -> event.getMessage().getFormattedMessage()).findFirst().orElseThrow(AssertionError::new);
        assertTrue(
                log.contains("No content to map due to end-of-input\n at [Source: (String)\"\"; line: 1, column: 0]"));
    }

    @Test
    void getResourceGroupServerNode() throws IOException {
        val resourceGroupJson = "{\"create_time\":1669704879469,\"instances\":[{\"instance\":\"10.1.2.185:7878\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"instance\":\"10.1.2.184:7878\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],"
                + "\"mapping_info\":[{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"BUILD\"},{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"QUERY\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"BUILD\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"QUERY\"}],\"resource_groups\":[{\"id\":\"c444879a-b3b0-4946-aed1-018cbc946c4a\"},{\"id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],\"uuid\":\"d5c316ed-b977-6efb-aea3-1735feb75d02\",\"last_modified\":1669952899667,\"version\":\"4.0.0.0\",\"resource_group_enabled\":true}\n";
        val rgManager = Mockito.mock(ResourceGroupManager.class);
        Mockito.when(rgManager.getResourceGroup())
                .thenReturn(JsonUtil.readValue(resourceGroupJson, new TypeReference<ResourceGroup>() {
                }));
        try (val mockStatic = Mockito.mockStatic(AddressUtil.class)) {
            mockStatic.when(AddressUtil::getLocalInstance).thenReturn("10.1.2.185:7878");
            val resourceGroupServerNode = scheduleService.getResourceGroupServerNode(rgManager);
            assertEquals(1, resourceGroupServerNode.size());
            assertTrue(resourceGroupServerNode.containsKey("cebdfbca-25bc-49b6-8ee4-71946219b4bb"));
            val kylinInstances = resourceGroupServerNode.get("cebdfbca-25bc-49b6-8ee4-71946219b4bb");
            assertEquals(1, kylinInstances.size());
            val instance = kylinInstances.get(0);
            assertEquals("cebdfbca-25bc-49b6-8ee4-71946219b4bb", instance.getResourceGroupId());
            assertEquals("10.1.2.184:7878", instance.getInstance());
        }
    }

    @Test
    void cancelTimeoutAsyncTask() throws InterruptedException {
        cancelTimeOutTask(10, 2);
        cancelTimeOutTask(0, 0);
    }

    private void cancelTimeOutTask(int second1, int second2) throws InterruptedException {
        Map<Future<?>, Long> asyncFutures = Maps.newConcurrentMap();
        val dateTime = new DateTime().plusHours(-4).plusSeconds(second1);
        for (int i = 0; i < 5; i++) {
            val futureTask = new FutureTask<String>(() -> null);
            val startTime = i % 2 == 0 ? dateTime.plusSeconds(-2) : dateTime.plusSeconds(second2);
            asyncFutures.put(futureTask, startTime.getMillis());
        }
        ReflectionTestUtils.setField(scheduleService, "opsCronTimeout",
                KylinConfig.getInstanceFromEnv().getRoutineOpsTaskTimeOut());
        val futures = ((Map) ReflectionTestUtils.getField(scheduleService, "ASYNC_FUTURES"));
        futures.putAll(asyncFutures);
        scheduleService.cancelTimeoutAsyncTask(dateTime.getMillis());
        asyncFutures.keySet().forEach(future -> {
            assertTrue(future.isDone());
        });
    }

    @Test
    void cancelTimeoutAsyncTaskWithEmptyAsyncFutures() throws InterruptedException {
        ReflectionTestUtils.setField(scheduleService, "opsCronTimeout",
                KylinConfig.getInstanceFromEnv().getRoutineOpsTaskTimeOut());
        val logCaptor = ArgumentCaptor.forClass(LogEvent.class);
        scheduleService.cancelTimeoutAsyncTask(System.currentTimeMillis());
        Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
        val logCount = logCaptor.getAllValues().stream()
                .filter(event -> event.getLoggerName().equals("org.apache.kylin.rest.service.ScheduleService"))
                .filter(event -> Sets.newHashSet(Level.INFO, Level.WARN).contains(event.getLevel())).count();
        assertEquals(0, logCount);
    }
}
