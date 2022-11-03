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

import static org.apache.kylin.common.constant.Constants.MARK;
import static org.apache.kylin.common.constant.Constants.SOURCE_TABLE_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableMap;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo
class AutoRefreshSnapshotSchedulerTest {
    @InjectMocks
    private final AutoRefreshSnapshotScheduler snapshotScheduler = Mockito.spy(new AutoRefreshSnapshotScheduler());
    @InjectMocks
    private final TaskScheduler projectScheduler = Mockito.spy(new ThreadPoolTaskScheduler());
    @InjectMocks
    private final RestTemplate restTemplate = Mockito.spy(new RestTemplate());
    private NProjectManager manager;

    @BeforeEach
    void setup() {
        manager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ReflectionTestUtils.setField(snapshotScheduler, "projectScheduler", projectScheduler);
        ReflectionTestUtils.setField(snapshotScheduler, "restTemplate", restTemplate);
    }

    @Test
    void checkSchedulerThreadPoolSize() {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        snapshotScheduler.getSchedulerProjectCount().set(20);
        snapshotScheduler.checkSchedulerThreadPoolSize();
        val poolSizeActual = projectThreadPoolScheduler.getPoolSize();
        assertEquals(snapshotScheduler.getSchedulerProjectCount().get(), poolSizeActual);
        assertEquals(21, poolSizeActual);
    }

    @Test
    void startCron() {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        try {
            projectThreadPoolScheduler.initialize();
            val project = RandomUtil.randomUUIDStr();
            val cron = KylinConfig.getInstanceFromEnv().getSnapshotAutoRefreshCron();
            snapshotScheduler.startCron(project, () -> {
            }, cron);
            val taskFutures = snapshotScheduler.getTaskFutures();
            assertEquals(1, taskFutures.size());
            assertFalse(taskFutures.get(project).getSecond().isDone());
            assertEquals(cron, taskFutures.get(project).getFirst());
            assertEquals(1, snapshotScheduler.getSchedulerProjectCount().get());
        } finally {
            projectThreadPoolScheduler.shutdown();
        }
    }

    @Test
    void stopCron() {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        try {
            projectThreadPoolScheduler.initialize();
            val project = RandomUtil.randomUUIDStr();
            val taskFutures = snapshotScheduler.getTaskFutures();
            val cron = KylinConfig.getInstanceFromEnv().getSnapshotAutoRefreshCron();
            val schedule = projectScheduler.schedule(() -> {
            }, triggerContext -> {
                CronTrigger trigger = new CronTrigger(cron);
                return trigger.nextExecutionTime(triggerContext);
            });
            val taskPair = new Pair<String, ScheduledFuture<?>>(cron, schedule);
            taskFutures.put(project, taskPair);

            snapshotScheduler.stopCron(project);
            assertEquals(0, taskFutures.size());
            assertTrue(taskPair.getSecond().isCancelled());
            assertEquals(snapshotScheduler.getSchedulerProjectCount().get(), -1);
        } finally {
            projectThreadPoolScheduler.shutdown();
        }
    }

    @Test
    void cancelDeletedProject() throws IOException {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        try {
            projectThreadPoolScheduler.initialize();
            val project = RandomUtil.randomUUIDStr();
            val taskFutures = snapshotScheduler.getTaskFutures();
            val cron = KylinConfig.getInstanceFromEnv().getSnapshotAutoRefreshCron();
            val schedule = projectScheduler.schedule(() -> {
            }, triggerContext -> {
                CronTrigger trigger = new CronTrigger(cron);
                return trigger.nextExecutionTime(triggerContext);
            });
            val taskPair = new Pair<String, ScheduledFuture<?>>(cron, schedule);
            taskFutures.put(project, taskPair);

            val project2 = RandomUtil.randomUUIDStr();
            val overrideProps = Maps.<String, String> newLinkedHashMap();
            overrideProps.put("kylin.snapshot.manual-management-enabled", "true");
            overrideProps.put("kylin.snapshot.auto-refresh-enabled", "true");
            manager.createProject(project2, "test", null, overrideProps);

            val schedule2 = projectScheduler.schedule(() -> {
            }, triggerContext -> {
                CronTrigger trigger = new CronTrigger(cron);
                return trigger.nextExecutionTime(triggerContext);
            });
            val taskPair2 = new Pair<String, ScheduledFuture<?>>(cron, schedule2);
            taskFutures.put(project2, taskPair2);

            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            val config = KylinConfig.getInstanceFromEnv();
            createSnapshotAutoRefresh(project, fs, config);
            createSnapshotAutoRefresh(project2, fs, config);

            snapshotScheduler.cancelDeletedProject(manager);
            assertEquals(1, taskFutures.size());
            assertTrue(taskPair.getSecond().isCancelled());
            assertFalse(taskPair2.getSecond().isDone());
            assertEquals(-1, snapshotScheduler.getSchedulerProjectCount().get());
            assertFalse(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project))));
            assertTrue(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project2))));

            var projectInstance2 = manager.getProject(project2);
            overrideProps.put("kylin.snapshot.auto-refresh-enabled", "false");
            manager.updateProject(projectInstance2, projectInstance2.getName(), projectInstance2.getDescription(),
                    overrideProps);

            snapshotScheduler.cancelDeletedProject(manager);
            assertEquals(0, taskFutures.size());
            assertTrue(taskPair.getSecond().isCancelled());
            assertTrue(taskPair2.getSecond().isCancelled());
            assertEquals(-2, snapshotScheduler.getSchedulerProjectCount().get());
            assertFalse(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project))));
            assertFalse(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project2))));
        } finally {
            projectThreadPoolScheduler.shutdown();
        }
    }

    @Test
    void cancelDeletedProject2() throws IOException {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        try {
            projectThreadPoolScheduler.initialize();
            val taskFutures = snapshotScheduler.getTaskFutures();
            val cron = KylinConfig.getInstanceFromEnv().getSnapshotAutoRefreshCron();

            val project2 = RandomUtil.randomUUIDStr();
            val overrideProps = Maps.<String, String> newLinkedHashMap();
            overrideProps.put("kylin.snapshot.manual-management-enabled", "true");
            overrideProps.put("kylin.snapshot.auto-refresh-enabled", "true");
            manager.createProject(project2, "test", null, overrideProps);

            val schedule2 = projectScheduler.schedule(() -> {
            }, triggerContext -> {
                CronTrigger trigger = new CronTrigger(cron);
                return trigger.nextExecutionTime(triggerContext);
            });
            val taskPair2 = new Pair<String, ScheduledFuture<?>>(cron, schedule2);
            taskFutures.put(project2, taskPair2);

            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            val config = KylinConfig.getInstanceFromEnv();
            createSnapshotAutoRefresh(project2, fs, config);

            snapshotScheduler.cancelDeletedProject(manager);
            assertEquals(1, taskFutures.size());
            assertFalse(taskPair2.getSecond().isDone());
            assertEquals(0, snapshotScheduler.getSchedulerProjectCount().get());
            assertTrue(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project2))));

            var projectInstance2 = manager.getProject(project2);
            overrideProps.put("kylin.snapshot.manual-management-enabled", "false");
            manager.updateProject(projectInstance2, projectInstance2.getName(), projectInstance2.getDescription(),
                    overrideProps);

            snapshotScheduler.cancelDeletedProject(manager);
            assertEquals(0, taskFutures.size());
            assertTrue(taskPair2.getSecond().isCancelled());
            assertEquals(-1, snapshotScheduler.getSchedulerProjectCount().get());
            assertFalse(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project2))));
        } finally {
            projectThreadPoolScheduler.shutdown();
        }
    }

    @Test
    void deleteProjectSnapshotAutoUpdateDir() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        val config = KylinConfig.getInstanceFromEnv();
        createSnapshotAutoRefresh("default", fs, config);

        snapshotScheduler.deleteProjectSnapshotAutoUpdateDir("default");
        assertFalse(fs.exists(new Path(config.getSnapshotAutoRefreshDir("default"))));
    }

    private void createSnapshotAutoRefresh(String project, FileSystem fs, KylinConfig config) throws IOException {
        val sourceTableStatsDir = new Path(config.getSnapshotAutoRefreshDir(project), SOURCE_TABLE_STATS);
        fs.mkdirs(sourceTableStatsDir);
        val sourceTableStats = new Path(sourceTableStatsDir, "default.table");

        val expected = new ImmutableMap.Builder().put("test", "test").build();
        try (val out = fs.create(sourceTableStats, true)) {
            out.write(JsonUtil.writeValueAsBytes(expected));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void AutoRefreshSnapshot() {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        try {
            projectThreadPoolScheduler.initialize();

            val project = RandomUtil.randomUUIDStr();
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            val overrideProps = Maps.<String, String> newLinkedHashMap();
            projectManager.createProject(project, "test", null, overrideProps);
            var projectInstance = projectManager.getProject(project);
            var result = snapshotScheduler.autoRefreshSnapshot(projectInstance);
            assertFalse(result);

            overrideProps.put("kylin.snapshot.manual-management-enabled", "true");
            overrideProps.put("kylin.snapshot.auto-refresh-enabled", "true");
            projectManager.updateProject(projectInstance, projectInstance.getName(), projectInstance.getDescription(),
                    overrideProps);
            projectInstance = projectManager.getProject(project);
            try (val epochManagerMockedStatic = Mockito.mockStatic(EpochManager.class)) {
                val epoch = mock(Epoch.class);
                when(epoch.getCurrentEpochOwner()).thenReturn(AddressUtil.getLocalInstance().replace('0', '1'));
                val epochManger = mock(EpochManager.class);
                when(epochManger.getEpoch(project)).thenReturn(epoch);
                epochManagerMockedStatic.when(EpochManager::getInstance).thenReturn(epochManger);
                result = snapshotScheduler.autoRefreshSnapshot(projectInstance);
                assertFalse(result);

                when(epoch.getCurrentEpochOwner()).thenReturn(AddressUtil.getLocalInstance());
                snapshotScheduler.getSchedulerProjectCount().incrementAndGet();
                val taskFutures = snapshotScheduler.getTaskFutures();
                val cron = KylinConfig.getInstanceFromEnv().getSnapshotAutoRefreshCron();
                val schedule = projectScheduler.schedule(() -> {
                }, triggerContext -> {
                    CronTrigger trigger = new CronTrigger(cron);
                    return trigger.nextExecutionTime(triggerContext);
                });
                val taskPair = new Pair<String, ScheduledFuture<?>>(cron, schedule);
                taskFutures.put(project, taskPair);
                result = snapshotScheduler.autoRefreshSnapshot(projectInstance);
                assertFalse(result);
                assertEquals(1, taskFutures.size());
                assertFalse(taskFutures.get(project).getSecond().isDone());
                assertEquals(cron, taskFutures.get(project).getFirst());
                assertEquals(1, snapshotScheduler.getSchedulerProjectCount().get());

                val cronNew = "1 1 1 */1 * ?";
                overrideProps.put("kylin.snapshot.auto-refresh-cron", cronNew);
                projectManager.updateProject(projectInstance, projectInstance.getName(),
                        projectInstance.getDescription(), overrideProps);
                projectInstance = projectManager.getProject(project);
                result = snapshotScheduler.autoRefreshSnapshot(projectInstance);
                assertTrue(result);
                assertEquals(1, taskFutures.size());
                assertFalse(taskFutures.get(project).getSecond().isDone());
                assertEquals(cronNew, taskFutures.get(project).getFirst());
                assertEquals(1, snapshotScheduler.getSchedulerProjectCount().get());
            }
        } finally {
            projectThreadPoolScheduler.shutdown();
        }
    }

    @Test
    void checkRefreshRunnerJobPool() {
        val project = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        val overrideProps = Maps.<String, String> newLinkedHashMap();
        projectManager.createProject(project, "test", null, overrideProps);
        AutoRefreshSnapshotRunner.getInstance(project);
        snapshotScheduler.checkRefreshRunnerJobPool(KylinConfig.newKylinConfig(), project);
        var snapshotRunner = AutoRefreshSnapshotRunner.getInstanceByProject(project);
        assertEquals(20, ((ThreadPoolExecutor) snapshotRunner.getJobPool()).getCorePoolSize());
        assertEquals(20, ((ThreadPoolExecutor) snapshotRunner.getJobPool()).getMaximumPoolSize());

        overrideProps.put("kylin.snapshot.auto-refresh-max-concurrent-jobs", "21");
        var projectInstance = projectManager.getProject(project);
        projectManager.updateProject(projectInstance, projectInstance.getName(), projectInstance.getDescription(),
                overrideProps);
        projectInstance = projectManager.getProject(project);
        snapshotScheduler.checkRefreshRunnerJobPool(projectInstance.getConfig(), project);
        snapshotRunner = AutoRefreshSnapshotRunner.getInstanceByProject(project);
        assertEquals(21, ((ThreadPoolExecutor) snapshotRunner.getJobPool()).getCorePoolSize());
        assertEquals(21, ((ThreadPoolExecutor) snapshotRunner.getJobPool()).getMaximumPoolSize());
    }

    @Test
    void schedulerAutoRefresh() {
        snapshotScheduler.schedulerAutoRefresh();
        assertEquals(0, snapshotScheduler.getSchedulerProjectCount().get());
        assertEquals(0, snapshotScheduler.getTaskFutures().size());
    }

    @Test
    void markFile() throws Exception {
        val project = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        val overrideProps = Maps.<String, String> newLinkedHashMap();
        projectManager.createProject(project, "test", null, overrideProps);
        var projectInstance = projectManager.getProject(project);
        overrideProps.put("kylin.snapshot.manual-management-enabled", "true");
        overrideProps.put("kylin.snapshot.auto-refresh-enabled", "true");
        projectManager.updateProject(projectInstance, projectInstance.getName(), projectInstance.getDescription(),
                overrideProps);
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val markFilePath = new Path(KylinConfig.readSystemKylinConfig().getSnapshotAutoRefreshDir(project) + MARK);
        try (val mockedStatic = Mockito.mockStatic(AutoRefreshSnapshotRunner.class);
                val mockedStatic2 = Mockito.mockStatic(EpochManager.class)) {
            val runner = Mockito.mock(AutoRefreshSnapshotRunner.class);
            val epochManager = Mockito.mock(EpochManager.class);
            mockedStatic.when(() -> AutoRefreshSnapshotRunner.getInstance(anyString())).thenReturn(runner);
            mockedStatic2.when(EpochManager::getInstance).thenReturn(epochManager);
            doNothing().when(runner).doRun();
            ReflectionTestUtils.setField(runner, "projectConfig", KylinConfig.readSystemKylinConfig());
            ReflectionTestUtils.setField(runner, "project", project);
            Mockito.doCallRealMethod().when(runner).runWhenSchedulerInit();
            Mockito.doCallRealMethod().when(runner).deleteMarkFile();
            val epoch = Mockito.mock(Epoch.class);
            when(epochManager.getEpoch(anyString())).thenReturn(epoch);
            when(epoch.getCurrentEpochOwner()).thenReturn(AddressUtil.getLocalInstance());

            assertFalse(fileSystem.exists(markFilePath));
            snapshotScheduler.afterPropertiesSet();
            assertFalse(fileSystem.exists(markFilePath));

            try (val out = fileSystem.create(markFilePath, true)) {
                out.write(new byte[] {});
            }

            snapshotScheduler.afterPropertiesSet();
            assertFalse(fileSystem.exists(markFilePath));
        } finally {
            if (fileSystem.exists(markFilePath)) {
                fileSystem.delete(markFilePath, true);
            }
        }
    }

    @Test
    void afterPropertiesSetDeleteSnapshotAutoRefresh() throws Exception {
        val project = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        val overrideProps = Maps.<String, String> newLinkedHashMap();
        projectManager.createProject(project, "test", null, overrideProps);

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        val config = KylinConfig.getInstanceFromEnv();
        createSnapshotAutoRefresh("default", fs, config);
        snapshotScheduler.afterPropertiesSet();
        assertFalse(fs.exists(new Path(config.getSnapshotAutoRefreshDir(project))));
    }
}
