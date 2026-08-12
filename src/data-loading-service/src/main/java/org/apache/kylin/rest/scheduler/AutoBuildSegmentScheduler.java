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

import static org.apache.kylin.metadata.model.AutoSegmentBuildConfig.END_OF_DAY;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.AutoSegmentBuildConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AutoBuildSegmentScheduler {
    private static final Duration INITIAL_TRIGGER_LOOKBACK = Duration.ofMinutes(1);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    private final AtomicBoolean dispatching = new AtomicBoolean(false);
    private final AtomicReference<Instant> lastDispatchTime = new AtomicReference<>();

    @Scheduled(cron = "${kylin.model.auto-segment-build.dispatcher-cron:*/30 * * * * ?}")
    public void schedulerAutoBuildSegment() {
        val currentTime = Instant.now();
        if (!JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobScheduler().isMaster()) {
            lastDispatchTime.set(currentTime);
            return;
        }
        if (!dispatching.compareAndSet(false, true)) {
            log.warn("Skip auto build segment dispatch because the previous dispatch is still running");
            return;
        }

        val previousTime = getPreviousDispatchTime(currentTime);
        try {
            dispatch(previousTime, currentTime);
        } finally {
            lastDispatchTime.set(currentTime);
            dispatching.set(false);
        }
    }

    private Instant getPreviousDispatchTime(Instant currentTime) {
        val previousTime = lastDispatchTime.get();
        if (previousTime == null || previousTime.isAfter(currentTime)) {
            return currentTime.minus(INITIAL_TRIGGER_LOOKBACK);
        }
        return previousTime;
    }

    @VisibleForTesting
    void dispatch(Instant previousTime, Instant currentTime) {
        val systemConfig = KylinConfig.readSystemKylinConfig();
        val projectManager = NProjectManager.getInstance(systemConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            try {
                dispatchProject(systemConfig, project, previousTime, currentTime);
            } catch (Exception e) {
                log.error("Auto build segment dispatch failed for project: {}", project.getName(), e);
            }
        }
    }

    private void dispatchProject(KylinConfig systemConfig, ProjectInstance project, Instant previousTime,
            Instant currentTime) {
        val projectName = project.getName();
        val zoneId = ZoneId.of(project.getConfig().getTimeZone());
        val dataflowManager = NDataflowManager.getInstance(systemConfig, projectName);
        for (NDataModel model : dataflowManager.listOnlineDataModels()) {
            try {
                dispatchModel(projectName, model, zoneId, previousTime, currentTime);
            } catch (Exception e) {
                log.error("Auto build segment dispatch failed, project: {}, model: {}", projectName, model.getUuid(),
                        e);
            }
        }
    }

    private void dispatchModel(String project, NDataModel model, ZoneId zoneId, Instant previousTime,
            Instant currentTime) {
        val autoSegmentBuild = getEligibleConfig(model);
        if (autoSegmentBuild == null) {
            return;
        }
        if (StringUtils.isBlank(autoSegmentBuild.getTriggerTime())) {
            log.warn("Skip auto build segment because trigger_time is blank, project: {}, model: {}", project,
                    model.getUuid());
            return;
        }

        val scheduledTime = getLatestScheduledTime(autoSegmentBuild.getTriggerTime(), zoneId, currentTime);
        val scheduledInstant = scheduledTime.toInstant();
        if (!scheduledInstant.isAfter(previousTime) || scheduledInstant.isAfter(currentTime)) {
            return;
        }
        submitJob(project, model, autoSegmentBuild, scheduledTime);
    }

    private AutoSegmentBuildConfig getEligibleConfig(NDataModel model) {
        if (model.isBroken() || model.isStreaming() || model.isMultiPartitionModel()
                || PartitionDesc.isEmptyPartitionDesc(model.getPartitionDesc()) || model.getSegmentConfig() == null) {
            return null;
        }
        val autoSegmentBuild = model.getSegmentConfig().getAutoSegmentBuild();
        return autoSegmentBuild != null && autoSegmentBuild.isEnabled() ? autoSegmentBuild : null;
    }

    @VisibleForTesting
    ZonedDateTime getLatestScheduledTime(String triggerTime, ZoneId zoneId, Instant currentTime) {
        val localCurrentTime = currentTime.atZone(zoneId);
        val parsedTriggerTime = LocalTime.parse(triggerTime, TIME_FORMATTER);
        ZonedDateTime scheduledTime = ZonedDateTime.of(localCurrentTime.toLocalDate(), parsedTriggerTime, zoneId);
        if (scheduledTime.toInstant().isAfter(currentTime)) {
            scheduledTime = scheduledTime.minusDays(1);
        }
        return scheduledTime;
    }

    @VisibleForTesting
    void submitJob(String project, NDataModel model, AutoSegmentBuildConfig autoSegmentBuild,
            ZonedDateTime scheduledTime) {
        val modelId = model.getUuid();
        if (hasRunningModelBuildJob(project, modelId)) {
            log.info("Skip auto build segment because the model has a progressing build job, project: {}, model: {}",
                    project, modelId);
            return;
        }
        try {
            val zoneId = scheduledTime.getZone();
            val logicalDate = scheduledTime.toLocalDate().minusDays(autoSegmentBuild.getLogicalDateOffsetDays());
            val start = parseTime(autoSegmentBuild.getDataRangeStartTime(), false);
            val end = parseTime(autoSegmentBuild.getDataRangeEndTime(), true);
            val startDateTime = LocalDateTime.of(logicalDate, start.getFirst());
            val endDate = end.getSecond() ? logicalDate.plusDays(1) : logicalDate;
            val endDateTime = LocalDateTime.of(endDate, end.getFirst());
            if (!startDateTime.isBefore(endDateTime)) {
                log.warn("Skip auto build segment because of an invalid range, project: {}, model: {}", project,
                        modelId);
                return;
            }
            val startMillis = String.valueOf(startDateTime.atZone(zoneId).toInstant().toEpochMilli());
            val endMillis = String.valueOf(endDateTime.atZone(zoneId).toInstant().toEpochMilli());
            val params = new IncrementBuildSegmentParams(project, modelId, startMillis, endMillis,
                    model.getPartitionDesc(), model.getMultiPartitionDesc(), Lists.newArrayList(), true, null);
            modelBuildService.incrementBuildSegmentsByScheduler(params, "System");
            log.info("Auto build segment submitted, project: {}, model: {}, scheduled time: {}, range: [{}, {})",
                    project, modelId, scheduledTime, startMillis, endMillis);
        } catch (DateTimeParseException e) {
            log.error("Invalid time in auto build segment config, project: {}, model: {}", project, modelId, e);
        } catch (Exception e) {
            log.error("Auto build segment submit failed, project: {}, model: {}", project, modelId, e);
        }
    }

    @VisibleForTesting
    boolean hasRunningModelBuildJob(String project, String modelId) {
        // Segment and model metadata mutations are serialized with any progressing build job on the same model.
        val executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        JobTypeEnum[] buildJobTypes = JobTypeEnum.getJobTypeByCategory(JobTypeEnum.Category.BUILD)
                .toArray(new JobTypeEnum[0]);
        List<AbstractExecutable> jobs = executableManager.listExecByModelAndStatus(modelId,
                ExecutableState::isProgressing, buildJobTypes);
        return !jobs.isEmpty();
    }

    private Pair<LocalTime, Boolean> parseTime(String time, boolean allow24Hour) {
        if (allow24Hour && StringUtils.equals(time, END_OF_DAY)) {
            return Pair.newPair(LocalTime.MIDNIGHT, true);
        }
        return Pair.newPair(LocalTime.parse(time, TIME_FORMATTER), false);
    }
}
