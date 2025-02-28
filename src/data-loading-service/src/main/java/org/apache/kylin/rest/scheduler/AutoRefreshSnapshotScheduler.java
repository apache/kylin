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
import static org.apache.kylin.job.factory.JobFactoryConstant.AUTO_REFRESH_JOB_FACTORY;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * File hierarchy is
 * <p>
 * /working_dir
 * |--/${project_name}
 *    |--/snapshot_auto_refresh
 *       |--/_mark
 *       |--/view_mapping
 *       |--/source_table_stats
 *          |--/${source_table_qualified_name}
 *       |--/snapshot_job
 *          |--/${snapshot_table_qualified_name}
 */
@Slf4j
@Component
public class AutoRefreshSnapshotScheduler {
    private static final Integer THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE = 20;
    static {
        JobFactory.register(AUTO_REFRESH_JOB_FACTORY, new AutoRefreshJob.AutoRefreshJobFactory());
    }
    @Autowired
    @Qualifier("projectScheduler")
    private TaskScheduler projectScheduler;

    @Getter
    private final Map<String, Pair<String, ScheduledFuture<?>>> taskFutures = Maps.newConcurrentMap();
    @Getter
    private final AtomicInteger schedulerProjectCount = new AtomicInteger(0);

    public void startCron(String project, Runnable task, String cron) {
        stopCron(project);
        checkSchedulerThreadPoolSize();

        ScheduledFuture<?> scheduledFuture = projectScheduler.schedule(task, triggerContext -> {
            CronTrigger trigger = new CronTrigger(cron);
            return trigger.nextExecutionTime(triggerContext);
        });
        log.info("Project[{}] start cron[{}]", project, cron);
        taskFutures.put(project, new Pair<>(cron, scheduledFuture));
    }

    public void checkSchedulerThreadPoolSize() {
        val projectThreadPoolScheduler = (ThreadPoolTaskScheduler) projectScheduler;
        val poolSize = projectThreadPoolScheduler.getPoolSize();
        val projectCount = schedulerProjectCount.incrementAndGet();
        if (projectCount > poolSize) {
            projectThreadPoolScheduler.setPoolSize(projectCount);
        } else if (projectCount < THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE
                && poolSize > THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE) {
            projectThreadPoolScheduler.setPoolSize(THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE);
        }
    }

    public void stopCron(String project) {
        val scheduledFuturePair = taskFutures.get(project);
        if (scheduledFuturePair != null) {
            ScheduledFuture<?> future = scheduledFuturePair.getSecond();
            if (future != null) {
                log.info("Project[{}] stop cron", project);
                future.cancel(true);
            }
            taskFutures.remove(project);
            schedulerProjectCount.decrementAndGet();
        }
    }

    @Scheduled(cron = "*/30 * * * * ?")
    public void schedulerAutoRefresh() {
        val projectManager = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig());
        schedulerProject(projectManager);
        cancelDeletedProject(projectManager);
    }

    private void schedulerProject(NProjectManager projectManager) {
        val projectInstances = projectManager.listAllProjects();
        for (ProjectInstance projectInstance : projectInstances) {
            autoRefreshSnapshot(projectInstance);
        }
    }

    public boolean autoRefreshSnapshot(ProjectInstance projectInstance) {
        val projectConfig = projectInstance.getConfig();
        if (projectConfig.isSnapshotManualManagementEnabled() && projectConfig.isSnapshotAutoRefreshEnabled()) {
            val projectName = projectInstance.getName();

            // check future cron
            val scheduledFuturePair = taskFutures.get(projectName);
            val cronFromConfig = projectConfig.getSnapshotAutoRefreshCron();
            if (scheduledFuturePair != null) {
                ScheduledFuture<?> future = scheduledFuturePair.getSecond();
                if (future != null && StringUtils.equals(scheduledFuturePair.getFirst(), cronFromConfig)) {
                    log.info("Project[{}] skip schedulerAutoRefresh, because is running, cron[{}]", projectName,
                            cronFromConfig);
                    return false;
                }
            }
            // start/restart cron
            startCron(projectName, () -> submitJob(projectName), cronFromConfig);
            return true;
        }
        return false;
    }

    private void submitJob(String projectName) {
        if (JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobScheduler().isMaster()) {
            ExecutableManager manager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
            manager.checkAndSubmitCronJob(AUTO_REFRESH_JOB_FACTORY, JobTypeEnum.AUTO_REFRESH);
        }
    }

    public void cancelDeletedProject(NProjectManager projectManager) {
        for (val project : taskFutures.keySet()) {
            val projectInstance = projectManager.getProject(project);
            if (null == projectInstance) {
                AutoRefreshSnapshotRunner.shutdown(project);
                stopCron(project);
                deleteProjectSnapshotAutoUpdateDir(project);
                log.info("Project[{}] is deleted...", project);
            } else {
                val projectConfig = projectInstance.getConfig();
                if (!projectConfig.isSnapshotManualManagementEnabled()
                        || !projectConfig.isSnapshotAutoRefreshEnabled()) {
                    AutoRefreshSnapshotRunner.shutdown(project);
                    stopCron(project);
                    deleteProjectSnapshotAutoUpdateDir(project);
                    log.info("Project[{}] stop auto fresh snapshot...", project);
                }
            }
        }
    }

    public void deleteProjectSnapshotAutoUpdateDir(String project) {
        try {
            val fs = HadoopUtil.getWorkingFileSystem();
            val projectSnapshotAutoUpdateDirStr = KylinConfig.readSystemKylinConfig()
                    .getSnapshotAutoRefreshDir(project);
            val projectSnapshotAutoUpdateDir = new Path(projectSnapshotAutoUpdateDirStr);
            if (fs.exists(projectSnapshotAutoUpdateDir)) {
                fs.delete(projectSnapshotAutoUpdateDir, true);
                log.debug("delete project[{}] snapshot auto update dir success", project);
            }
        } catch (IOException e) {
            log.error("delete project[{}] snapshot auto update dir has error", project, e);
        }
    }

    public void afterPropertiesSet() throws Exception {
        log.info("AutoRefreshSnapshotScheduler init...");
        val fs = HadoopUtil.getWorkingFileSystem();
        val projectManager = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig());
        val allProject = projectManager.listAllProjects();
        for (ProjectInstance project : allProject) {
            val projectConfig = project.getConfig();
            if (projectConfig.isSnapshotManualManagementEnabled() && projectConfig.isSnapshotAutoRefreshEnabled()) {
                val projectName = project.getName();

                val markFilepath = new Path(projectConfig.getSnapshotAutoRefreshDir(projectName) + MARK);
                if (fs.exists(markFilepath)) {
                    log.error("Project[{}] last cron task was stopped manually, autoRefreshSnapshotRunner doRun",
                            projectName);
                    val autoRefreshSnapshotRunner = AutoRefreshSnapshotRunner.getInstance(projectName);
                    autoRefreshSnapshotRunner.runWhenSchedulerInit();
                }
            } else {
                deleteProjectSnapshotAutoUpdateDir(project.getName());
            }
        }
    }
}
