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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kylin.common.constant.Constants.MARK;
import static org.apache.kylin.common.constant.Constants.VIEW_MAPPING;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.awaitility.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo
class AutoRefreshSnapshotRunnerTest {
    private final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);

    @Test
    void getInstance() {
        val project = RandomUtil.randomUUIDStr();
        try {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            projectManager.createProject(project, "test", null, Maps.newLinkedHashMap());

            val config = KylinConfig.getInstanceFromEnv();
            assertNull(AutoRefreshSnapshotRunner.getInstanceByProject(project));

            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            val pool = ((ThreadPoolExecutor) runner.getJobPool());
            assertEquals(config.getSnapshotAutoRefreshMaxConcurrentJobLimit(), pool.getCorePoolSize());
            assertEquals(config.getSnapshotAutoRefreshMaxConcurrentJobLimit(), pool.getMaximumPoolSize());

            assertEquals(AutoRefreshSnapshotRunner.getInstanceByProject(project), runner);
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void testDoRun() throws JsonProcessingException {
        val project = RandomUtil.randomUUIDStr();
        try {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            projectManager.createProject(project, "test", null, Maps.newLinkedHashMap());
            projectManager.updateProject(project, copyForWrite -> {
                copyForWrite.putOverrideKylinProps("kylin.snapshot.manual-management-enabled", String.valueOf(true));
                copyForWrite.putOverrideKylinProps("kylin.snapshot.auto-refresh-enabled", String.valueOf(true));
            });
            val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
            val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            ReflectionTestUtils.setField(runner, "restTemplate", restTemplate);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);

            runner.doRun();
            assertTrue(CollectionUtils.isEmpty(runner.getCheckSourceTableQueue()));
            assertTrue(MapUtils.isEmpty(runner.getBuildSnapshotCount()));
            assertTrue(MapUtils.isEmpty(runner.getCheckSourceTableFutures()));
            assertTrue(MapUtils.isEmpty(runner.getSourceTableSnapshotMapping()));
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void testShutdown() {
        val project = "default";
        val runner = AutoRefreshSnapshotRunner.getInstance(project);
        AutoRefreshSnapshotRunner.shutdown(project);
        assertTrue(runner.getJobPool().isShutdown());
        assertNull(AutoRefreshSnapshotRunner.getInstanceByProject(project));
    }

    @Test
    void saveSnapshotViewMapping() throws JsonProcessingException {
        val project = "default";
        try {
            val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
            val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            ReflectionTestUtils.setField(runner, "restTemplate", restTemplate);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
            runner.saveSnapshotViewMapping(project, restTemplate);
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void saveSnapshotViewMappingFailed() throws JsonProcessingException {
        val project = "default";
        val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
        val resp = new ResponseEntity<>(restResult, HttpStatus.ACCEPTED);
        try {
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
            runner.saveSnapshotViewMapping(project, restTemplate);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinRuntimeException);
            assertEquals(String.format(Locale.ROOT, "Project[%s] Save View Mapping Failed", project), e.getMessage());
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void saveSnapshotViewMappingFailed2() throws JsonProcessingException {
        val project = "default";
        val restResult = JsonUtil.writeValueAsString(RestResponse.fail());
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        try {
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            ReflectionTestUtils.setField(runner, "restTemplate", restTemplate);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
            runner.saveSnapshotViewMapping(project, restTemplate);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinRuntimeException);
            assertEquals(String.format(Locale.ROOT, "Project[%s] Save View Mapping Failed", project), e.getMessage());
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }

    }

    @Test
    void saveSnapshotViewMappingFailed3() throws JsonProcessingException {
        val project = "default";
        val restResult = JsonUtil.writeValueAsString(RestResponse.ok(Boolean.FALSE));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        try {
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            ReflectionTestUtils.setField(runner, "restTemplate", restTemplate);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
            runner.saveSnapshotViewMapping(project, restTemplate);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinRuntimeException);
            assertEquals(String.format(Locale.ROOT, "Project[%s] Save View Mapping Failed", project), e.getMessage());
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }

    }

    @Test
    void readViewTableMapping() throws IOException {
        val project = "default";
        try {
            val fileSystem = HadoopUtil.getWorkingFileSystem();
            val pathStr = KylinConfig.readSystemKylinConfig().getSnapshotAutoRefreshDir(project) + VIEW_MAPPING;
            val snapshotTablesPath = new Path(pathStr);
            val viewMapping = Maps.<String, Set<String>> newHashMap();
            viewMapping.put("test", Sets.newHashSet("1", "2", "3"));
            try (val out = fileSystem.create(snapshotTablesPath, true)) {
                out.write(JsonUtil.writeValueAsBytes(viewMapping));
            }

            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            val actual = runner.readViewTableMapping();
            assertEquals(viewMapping.size(), actual.size());
            assertTrue(Sets.newHashSet("1", "2", "3").containsAll(actual.get("test")));
            assertTrue(actual.get("test").containsAll(Sets.newHashSet("1", "2", "3")));
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void getSourceTableSnapshotMapping() {
        val project = "default";
        try {
            val tableManager = NTableMetadataManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
            val allTables = tableManager.listAllTables();
            val tables = Lists.<TableDesc> newArrayList();
            Map<String, Set<String>> viewTableMapping = Maps.newHashMap();
            val excepted = allTables.stream().map(table -> table.getIdentity().toLowerCase(Locale.ROOT))
                    .collect(Collectors.toSet());
            for (int i = 0; i < allTables.size(); i++) {
                if (i < 14) {
                    tables.add(allTables.get(i));
                }
                if (allTables.get(i).isView()) {
                    tables.add(allTables.get(i));
                    val sourceTables = Sets.<String> newHashSet();
                    for (int j = 0; j < 7; j++) {
                        sourceTables.add("default.table_" + j);
                        excepted.add("default.table_" + j);
                    }
                    sourceTables.add(allTables.get(i).getIdentity().toLowerCase(Locale.ROOT));
                    viewTableMapping.put(allTables.get(i).getIdentity(), sourceTables);
                }
                if (i > 7) {
                    val sourceTables = Sets.<String> newHashSet();
                    for (int j = 0; j < 7; j++) {
                        sourceTables.add("default.table_" + j);
                        excepted.add("default.table_" + j);
                    }
                    sourceTables.add(allTables.get(i).getIdentity().toLowerCase(Locale.ROOT));
                    viewTableMapping.put(allTables.get(i).getIdentity(), sourceTables);
                }
            }
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            val sourceTableSnapshotMapping = runner.getSourceTableSnapshotMapping(tables, viewTableMapping);
            assertEquals(28, sourceTableSnapshotMapping.size());
            val actual = sourceTableSnapshotMapping.keySet();
            assertEquals(excepted.size(), actual.size());
            assertTrue(excepted.containsAll(actual));
            assertTrue(actual.containsAll(excepted));

            for (int j = 0; j < 7; j++) {
                val tableDescs = sourceTableSnapshotMapping.get("default.table_" + j);
                assertEquals(13, tableDescs.size());
            }
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void checkTableAndWaitAllTaskDone() throws Exception {
        val project = "default";
        try {
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            val tableManager = NTableMetadataManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
            val allTables = tableManager.listAllTables();
            val tables = Lists.<TableDesc> newArrayList();
            Map<String, Set<String>> viewTableMapping = Maps.newHashMap();

            val exceptedTmp = Lists.<TableDesc> newArrayList();
            for (int i = 0; i < allTables.size(); i++) {
                if (i < 14) {
                    tables.add(allTables.get(i));
                    val checkSourceTableResult = new CheckSourceTableResult();
                    checkSourceTableResult.setNeedRefresh(true);
                    checkSourceTableResult.setTableIdentity(allTables.get(i).getIdentity().toLowerCase(Locale.ROOT));
                    runner.getCheckSourceTableQueue().offer(checkSourceTableResult);
                    exceptedTmp.add(allTables.get(i));
                }
                if (i > 7) {
                    val sourceTables = Sets.<String> newHashSet();
                    for (int j = 0; j < 7; j++) {
                        sourceTables.add("default.table_" + j);
                    }
                    sourceTables.add(allTables.get(i).getIdentity().toLowerCase(Locale.ROOT));
                    viewTableMapping.put(allTables.get(i).getIdentity(), sourceTables);
                }
            }
            val sourceTableSnapshotMapping = runner.getSourceTableSnapshotMapping(tables, viewTableMapping);
            val sourceTables = sourceTableSnapshotMapping.keySet();
            runner.getSourceTableSnapshotMapping().putAll(sourceTableSnapshotMapping);

            try (val ignored = Mockito.mockConstruction(CheckSourceTableRunnable.class,
                    (mock, context) -> Mockito.doNothing().when(mock).checkTable())) {
                try (val ignored2 = Mockito.mockConstruction(BuildSnapshotRunnable.class,
                        (mock, context) -> Mockito.doNothing().when(mock).buildSnapshot())) {
                    runner.checkSourceTable(sourceTables);

                    val checkSourceTableFutures = runner.getCheckSourceTableFutures();
                    assertEquals(28, checkSourceTableFutures.size());

                    for (int j = 0; j < 14; j++) {
                        val checkSourceTableResult = new CheckSourceTableResult();
                        if (j % 2 == 0) {
                            checkSourceTableResult.setNeedRefresh(true);
                            checkSourceTableResult.setTableIdentity("default.table_" + (j / 2));
                        }
                        runner.getCheckSourceTableQueue().offer(checkSourceTableResult);
                    }

                    runner.waitCheckSourceTableTaskDone();

                    assertNull(runner.getCheckSourceTableQueue().peek());

                    val buildSnapshotCount = runner.getBuildSnapshotCount();
                    val tableDescs = sourceTableSnapshotMapping.get("default.table_0");
                    exceptedTmp.addAll(tableDescs);
                    val excepted = exceptedTmp.stream().distinct().collect(Collectors.toList());
                    assertEquals(excepted.size(), buildSnapshotCount.size());
                }
            }
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void cancelTimeoutFuture() {
        val project = RandomUtil.randomUUIDStr();
        try {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            val overrideProps = Maps.<String, String> newLinkedHashMap();
            overrideProps.put("kylin.snapshot.auto-refresh-task-timeout", "1s");
            projectManager.createProject(project, "test", "", overrideProps);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            val tasks = Lists.<Future<String>> newArrayList();
            for (int i = 0; i < 5; i++) {
                val futureTask = new FutureTask<String>(() -> null);
                tasks.add(futureTask);
                runner.getCheckSourceTableFutures().put(futureTask, System.currentTimeMillis());
            }
            await().pollDelay(new Duration(2, SECONDS)).until(() -> true);
            runner.cancelTimeoutFuture(runner.getCheckSourceTableFutures());
            runner.getCheckSourceTableFutures().keySet().forEach(future -> {
                assertTrue(future.isCancelled());
                assertTrue(future.isDone());
            });
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void markFile() throws IOException {
        val project = RandomUtil.randomUUIDStr();
        try {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            val overrideProps = Maps.<String, String> newLinkedHashMap();
            overrideProps.put("kylin.snapshot.auto-refresh-task-timeout", "1s");
            projectManager.createProject(project, "test", "", overrideProps);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);

            runner.saveMarkFile();
            val fs = HadoopUtil.getWorkingFileSystem();
            val markFile = new Path(config.getSnapshotAutoRefreshDir(project) + MARK);
            assertTrue(fs.exists(markFile));

            runner.deleteMarkFile();
            assertFalse(fs.exists(markFile));
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void testRun() throws IOException {
        val project = RandomUtil.randomUUIDStr();
        try {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            projectManager.createProject(project, "test", null, Maps.newLinkedHashMap());
            projectManager.updateProject(project, copyForWrite -> {
                copyForWrite.putOverrideKylinProps("kylin.snapshot.manual-management-enabled", String.valueOf(true));
                copyForWrite.putOverrideKylinProps("kylin.snapshot.auto-refresh-enabled", String.valueOf(true));
            });
            val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
            val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            ReflectionTestUtils.setField(runner, "restTemplate", restTemplate);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);

            runner.run();

            val fs = HadoopUtil.getWorkingFileSystem();
            val markFile = new Path(config.getSnapshotAutoRefreshDir(project) + MARK);
            assertFalse(fs.exists(markFile));
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void cancelFuture() {
        val project = RandomUtil.randomUUIDStr();
        try {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            val overrideProps = Maps.<String, String> newLinkedHashMap();
            projectManager.createProject(project, "test", "", overrideProps);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);

            for (int i = 0; i < 5; i++) {
                val futureTask = new FutureTask<String>(() -> null);
                runner.getCheckSourceTableFutures().put(futureTask, System.currentTimeMillis());
                if (i % 2 == 0) {
                    futureTask.cancel(true);
                }
            }
            runner.cancelFuture(runner.getCheckSourceTableFutures());
            val actual = runner.getCheckSourceTableFutures().keySet().stream().filter(Future::isDone).count();
            assertEquals(runner.getCheckSourceTableFutures().size(), actual);
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }

    @Test
    void testRunWhenSchedulerInit() throws IOException {
        val project = RandomUtil.randomUUIDStr();
        try {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            projectManager.createProject(project, "test", null, Maps.newLinkedHashMap());
            projectManager.updateProject(project, copyForWrite -> {
                copyForWrite.putOverrideKylinProps("kylin.snapshot.manual-management-enabled", String.valueOf(true));
                copyForWrite.putOverrideKylinProps("kylin.snapshot.auto-refresh-enabled", String.valueOf(true));
            });
            val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
            val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
            val runner = AutoRefreshSnapshotRunner.getInstance(project);
            ReflectionTestUtils.setField(runner, "restTemplate", restTemplate);
            Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                    ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);

            val fs = HadoopUtil.getWorkingFileSystem();
            val markFile = new Path(config.getSnapshotAutoRefreshDir(project) + MARK);
            try (val out = fs.create(markFile, true)) {
                out.write(new byte[] {});
            }
            runner.runWhenSchedulerInit();
            assertFalse(fs.exists(markFile));
        } finally {
            AutoRefreshSnapshotRunner.shutdown(project);
        }
    }
}
