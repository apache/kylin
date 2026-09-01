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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AutoBuildSegmentScheduler {
    private static final long DEFAULT_DISPATCHER_INTERVAL_MILLIS = 30_000L;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    @Value("${kylin.model.auto-segment-build.dispatcher-interval-ms:30000}")
    private long dispatcherIntervalMillis = DEFAULT_DISPATCHER_INTERVAL_MILLIS;

    private final AtomicBoolean dispatching = new AtomicBoolean(false);
    private final AtomicReference<Instant> lastDispatchTime = new AtomicReference<>();
    // Avoid repeating the same malformed-metadata warning on every dispatcher scan.
    private final Cache<String, String> invalidConfigWarnings = CacheBuilder.newBuilder().maximumSize(10_000)
            .expireAfterAccess(1, TimeUnit.DAYS).build();

    @Scheduled(fixedDelayString = "${kylin.model.auto-segment-build.dispatcher-interval-ms:30000}")
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
            return currentTime.minusMillis(Math.max(1L, dispatcherIntervalMillis));
        }
        return previousTime;
    }

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
        val autoSegmentBuild = getEligibleConfig(project, model);
        if (autoSegmentBuild == null) {
            return;
        }

        val scheduledTime = getLatestScheduledTime(autoSegmentBuild.getTriggerTime(), zoneId, currentTime);
        val scheduledInstant = scheduledTime.toInstant();
        if (!scheduledInstant.isAfter(previousTime) || scheduledInstant.isAfter(currentTime)) {
            return;
        }
        submitJob(project, model, autoSegmentBuild, scheduledTime);
    }

    private AutoSegmentBuildConfig getEligibleConfig(String project, NDataModel model) {
        val configKey = project + "/" + model.getUuid();
        if (model.isBroken() || model.isStreaming() || model.isMultiPartitionModel()
                || PartitionDesc.isEmptyPartitionDesc(model.getPartitionDesc()) || model.getSegmentConfig() == null) {
            invalidConfigWarnings.invalidate(configKey);
            return null;
        }
        val autoSegmentBuild = model.getSegmentConfig().getAutoSegmentBuild();
        if (autoSegmentBuild == null || !autoSegmentBuild.isEnabled()) {
            invalidConfigWarnings.invalidate(configKey);
            return null;
        }

        val invalidReason = getInvalidConfigReason(autoSegmentBuild);
        if (invalidReason == null) {
            invalidConfigWarnings.invalidate(configKey);
            return autoSegmentBuild;
        }
        if (!StringUtils.equals(invalidReason, invalidConfigWarnings.getIfPresent(configKey))) {
            log.warn("Skip auto build segment because of invalid config: {}, project: {}, model: {}", invalidReason,
                    project, model.getUuid());
            invalidConfigWarnings.put(configKey, invalidReason);
        }
        return null;
    }

    private String getInvalidConfigReason(AutoSegmentBuildConfig config) {
        List<String> missingFields = Lists.newArrayList();
        if (StringUtils.isBlank(config.getTriggerTime())) {
            missingFields.add("trigger_time");
        }
        if (config.getLogicalDateOffsetDays() == null) {
            missingFields.add("logical_date_offset_days");
        }
        if (StringUtils.isBlank(config.getDataRangeStartTime())) {
            missingFields.add("data_range_start_time");
        }
        if (StringUtils.isBlank(config.getDataRangeEndTime())) {
            missingFields.add("data_range_end_time");
        }
        if (!missingFields.isEmpty()) {
            return "missing required field(s): " + StringUtils.join(missingFields, ", ");
        }
        if (config.getLogicalDateOffsetDays() < 1) {
            return "logical_date_offset_days must be >= 1";
        }

        try {
            LocalTime.parse(config.getTriggerTime(), TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            return "invalid trigger_time, expected HH:mm:ss";
        }

        Pair<LocalTime, Boolean> start;
        Pair<LocalTime, Boolean> end;
        try {
            start = parseTime(config.getDataRangeStartTime(), false);
        } catch (DateTimeParseException e) {
            return "invalid data_range_start_time, expected HH:mm:ss";
        }
        try {
            end = parseTime(config.getDataRangeEndTime(), true);
        } catch (DateTimeParseException e) {
            return "invalid data_range_end_time, expected HH:mm:ss or " + END_OF_DAY;
        }
        if (!end.getSecond() && !start.getFirst().isBefore(end.getFirst())) {
            return "data_range_start_time must be earlier than data_range_end_time";
        }
        return null;
    }

    ZonedDateTime getLatestScheduledTime(String triggerTime, ZoneId zoneId, Instant currentTime) {
        val localCurrentTime = currentTime.atZone(zoneId);
        val parsedTriggerTime = LocalTime.parse(triggerTime, TIME_FORMATTER);
        ZonedDateTime scheduledTime = ZonedDateTime.of(localCurrentTime.toLocalDate(), parsedTriggerTime, zoneId);
        if (scheduledTime.toInstant().isAfter(currentTime)) {
            scheduledTime = scheduledTime.minusDays(1);
        }
        return scheduledTime;
    }

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
            modelBuildService.incrementBuildSegmentsByScheduler(params);
            log.info("Auto build segment submitted, project: {}, model: {}, scheduled time: {}, range: [{}, {})",
                    project, modelId, scheduledTime, startMillis, endMillis);
        } catch (DateTimeParseException e) {
            log.error("Invalid time in auto build segment config, project: {}, model: {}", project, modelId, e);
        } catch (Exception e) {
            log.error("Auto build segment submit failed, project: {}, model: {}", project, modelId, e);
        }
    }

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
