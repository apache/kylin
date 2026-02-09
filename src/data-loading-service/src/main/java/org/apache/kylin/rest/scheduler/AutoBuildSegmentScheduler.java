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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AutoBuildSegmentScheduler {
    private static final int THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE = 20;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Autowired
    @Qualifier("projectScheduler")
    private TaskScheduler projectScheduler;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    @Getter
    private final Map<String, Pair<String, ScheduledFuture<?>>> taskFutures = Maps.newConcurrentMap();
    @Getter
    private final AtomicInteger schedulerModelCount = new AtomicInteger(0);

    @Scheduled(cron = "*/30 * * * * ?")
    public void schedulerAutoBuildSegment() {
        val projectManager = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig());
        reconcile(projectManager);
    }

    private void reconcile(NProjectManager projectManager) {
        Map<String, String> expected = Maps.newHashMap();
        for (ProjectInstance project : projectManager.listAllProjects()) {
            val projectName = project.getName();
            val modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(), projectName);
            for (NDataModel model : modelManager.listAllModels()) {
                val segmentConfig = model.getSegmentConfig();
                if (segmentConfig == null || segmentConfig.getAutoSegmentBuild() == null
                        || !segmentConfig.getAutoSegmentBuild().isEnabled()) {
                    continue;
                }
                if (model.isStreaming() || model.isMultiPartitionModel()
                        || PartitionDesc.isEmptyPartitionDesc(model.getPartitionDesc())) {
                    continue;
                }
                val triggerTime = segmentConfig.getAutoSegmentBuild().getTriggerTime();
                if (StringUtils.isBlank(triggerTime)) {
                    continue;
                }
                val cron = toDailyCron(triggerTime);
                if (cron == null) {
                    continue;
                }
                expected.put(buildTaskKey(projectName, model.getUuid()), cron);
            }
        }
        for (val entry : expected.entrySet()) {
            val key = entry.getKey();
            val cron = entry.getValue();
            val scheduled = taskFutures.get(key);
            if (scheduled != null && StringUtils.equals(scheduled.getFirst(), cron)) {
                continue;
            }
            startCron(key, cron);
        }
        for (val key : Lists.newArrayList(taskFutures.keySet())) {
            if (!expected.containsKey(key)) {
                stopCron(key);
            }
        }
    }

    private void startCron(String key, String cron) {
        stopCron(key);
        checkSchedulerThreadPoolSize();
        val scheduledFuture = projectScheduler.schedule(() -> submitJob(key), triggerContext -> {
            val trigger = new org.springframework.scheduling.support.CronTrigger(cron);
            return trigger.nextExecutionTime(triggerContext);
        });
        taskFutures.put(key, Pair.newPair(cron, scheduledFuture));
        log.info("Auto build segment start cron, key: {}, cron: {}", key, cron);
    }

    private void stopCron(String key) {
        val scheduledFuturePair = taskFutures.get(key);
        if (scheduledFuturePair != null) {
            val future = scheduledFuturePair.getSecond();
            if (future != null) {
                future.cancel(true);
            }
            taskFutures.remove(key);
            schedulerModelCount.decrementAndGet();
            log.info("Auto build segment stop cron, key: {}", key);
        }
    }

    private void checkSchedulerThreadPoolSize() {
        val scheduler = (ThreadPoolTaskScheduler) projectScheduler;
        val poolSize = scheduler.getPoolSize();
        val modelCount = schedulerModelCount.incrementAndGet();
        if (modelCount > poolSize) {
            scheduler.setPoolSize(modelCount);
        } else if (modelCount < THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE
                && poolSize > THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE) {
            scheduler.setPoolSize(THREAD_POOL_TASK_SCHEDULER_DEFAULT_POOL_SIZE);
        }
    }

    private void submitJob(String key) {
        if (!JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobScheduler().isMaster()) {
            return;
        }
        val parts = key.split("/", 2);
        if (parts.length != 2) {
            return;
        }
        val project = parts[0];
        val modelId = parts[1];
        val modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
        val model = modelManager.getDataModelDesc(modelId);
        if (model == null) {
            stopCron(key);
            return;
        }
        val autoSegmentBuild = model.getSegmentConfig() == null ? null
                : model.getSegmentConfig().getAutoSegmentBuild();
        if (autoSegmentBuild == null || !autoSegmentBuild.isEnabled()) {
            return;
        }
        if (model.isStreaming() || model.isMultiPartitionModel()
                || PartitionDesc.isEmptyPartitionDesc(model.getPartitionDesc())) {
            return;
        }
        if (hasRunningBuildJob(project, modelId)) {
            log.info("Skip auto build segment because model has running build job, project: {}, model: {}", project,
                    modelId);
            return;
        }
        try {
            val projectConfig = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig()).getProject(project)
                    .getConfig();
            val zoneId = ZoneId.of(projectConfig.getTimeZone());
            val logicalDate = LocalDate.now(zoneId).minusDays(autoSegmentBuild.getLogicalDateOffsetDays());
            val start = parseTime(autoSegmentBuild.getDataRangeStartTime(), false);
            val end = parseTime(autoSegmentBuild.getDataRangeEndTime(), true);
            val startDateTime = LocalDateTime.of(logicalDate, start.getFirst());
            val endDate = end.getSecond() ? logicalDate.plusDays(1) : logicalDate;
            val endDateTime = LocalDateTime.of(endDate, end.getFirst());
            if (!startDateTime.isBefore(endDateTime)) {
                log.warn("Skip auto build segment because invalid range, project: {}, model: {}", project, modelId);
                return;
            }
            val startMillis = String.valueOf(startDateTime.atZone(zoneId).toInstant().toEpochMilli());
            val endMillis = String.valueOf(endDateTime.atZone(zoneId).toInstant().toEpochMilli());
            val params = new IncrementBuildSegmentParams(project, modelId, startMillis, endMillis,
                    model.getPartitionDesc(), model.getMultiPartitionDesc(), Lists.newArrayList(), true, null);
            modelBuildService.incrementBuildSegmentsByScheduler(params, "System");
            log.info("Auto build segment submitted, project: {}, model: {}, range: [{}, {})", project, modelId,
                    startMillis, endMillis);
        } catch (Exception e) {
            log.error("Auto build segment submit failed, project: {}, model: {}", project, modelId, e);
        }
    }

    private boolean hasRunningBuildJob(String project, String modelId) {
        val executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        JobTypeEnum[] buildJobTypes = JobTypeEnum.getJobTypeByCategory(JobTypeEnum.Category.BUILD)
                .toArray(new JobTypeEnum[0]);
        List<AbstractExecutable> jobs = executableManager.listExecByModelAndStatus(modelId,
                ExecutableState::isProgressing, buildJobTypes);
        return !jobs.isEmpty();
    }

    private String toDailyCron(String triggerTime) {
        try {
            val time = LocalTime.parse(triggerTime, TIME_FORMATTER);
            return String.format("%d %d %d * * ?", time.getSecond(), time.getMinute(), time.getHour());
        } catch (DateTimeParseException e) {
            log.warn("Invalid trigger_time for auto build segment: {}", triggerTime);
            return null;
        }
    }

    private Pair<LocalTime, Boolean> parseTime(String time, boolean allow24Hour) {
        if (allow24Hour && StringUtils.equals(time, "24:00:00")) {
            return Pair.newPair(LocalTime.MIDNIGHT, true);
        }
        return Pair.newPair(LocalTime.parse(time, TIME_FORMATTER), false);
    }

    private String buildTaskKey(String project, String modelId) {
        return project + "/" + modelId;
    }
}
