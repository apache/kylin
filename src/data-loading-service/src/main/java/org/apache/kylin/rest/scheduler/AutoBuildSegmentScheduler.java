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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.AutoSegmentBuildConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
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
    private static final int MAX_TRACKED_CYCLES = 10_000;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    private final AtomicBoolean dispatching = new AtomicBoolean(false);
    // A completed cycle is re-evaluated after failover/restart, with segment metadata providing idempotency.
    private final Cache<String, TargetState> cycleStates = CacheBuilder.newBuilder().maximumSize(MAX_TRACKED_CYCLES)
            .expireAfterAccess(2, TimeUnit.DAYS).build();
    // Avoid repeating the same malformed-metadata warning on every dispatcher scan.
    private final Cache<String, String> invalidConfigWarnings = CacheBuilder.newBuilder()
            .maximumSize(MAX_TRACKED_CYCLES).expireAfterAccess(1, TimeUnit.DAYS).build();

    @Scheduled(fixedDelayString = "${kylin.model.auto-segment-build.dispatcher-interval-ms:30000}")
    public void schedulerAutoBuildSegment() {
        val currentTime = Instant.now();
        if (!JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobScheduler().isMaster()) {
            return;
        }
        if (!dispatching.compareAndSet(false, true)) {
            log.warn("Skip auto build segment dispatch because the previous dispatch is still running");
            return;
        }

        try {
            dispatch(currentTime);
        } finally {
            dispatching.set(false);
        }
    }

    void dispatch(Instant currentTime) {
        val systemConfig = KylinConfig.readSystemKylinConfig();
        val projectManager = NProjectManager.getInstance(systemConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            try {
                dispatchProject(systemConfig, project, currentTime);
            } catch (Exception e) {
                log.error("Auto build segment dispatch failed for project: {}", project.getName(), e);
            }
        }
    }

    private void dispatchProject(KylinConfig systemConfig, ProjectInstance project, Instant currentTime) {
        if (project.getSegmentConfig() == null
                || !Boolean.TRUE.equals(project.getSegmentConfig().getAutoSegmentBuildEnabled())) {
            return;
        }
        val projectName = project.getName();
        val zoneId = ZoneId.of(project.getConfig().getTimeZone());
        val dataflowManager = NDataflowManager.getInstance(systemConfig, projectName);
        for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
            try {
                dispatchModel(projectName, model, zoneId, currentTime);
            } catch (Exception e) {
                log.error("Auto build segment dispatch failed, project: {}, model: {}", projectName, model.getUuid(),
                        e);
            }
        }
    }

    private void dispatchModel(String project, NDataModel model, ZoneId zoneId, Instant currentTime) {
        val autoSegmentBuild = getEligibleConfig(project, model);
        if (autoSegmentBuild == null) {
            return;
        }

        val scheduledTime = getLatestScheduledTime(autoSegmentBuild.getTriggerTime(), zoneId, currentTime);
        if (scheduledTime.toInstant().isAfter(currentTime)) {
            return;
        }
        dispatchTarget(project, model, autoSegmentBuild, scheduledTime);
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

    private void dispatchTarget(String project, NDataModel model, AutoSegmentBuildConfig autoSegmentBuild,
            ZonedDateTime scheduledTime) {
        val modelId = model.getUuid();
        val target = createBuildTarget(model, autoSegmentBuild, scheduledTime);
        val cycleKey = createCycleKey(project, modelId, scheduledTime, target.range);
        val previousState = cycleStates.getIfPresent(cycleKey);
        if (previousState != null && previousState.isComplete()) {
            return;
        }

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        List<NDataSegment> segments = dataflow == null ? Lists.newArrayList() : dataflow.getSegments();
        val jobs = getModelBuildJobs(project, modelId);
        TargetState state = evaluateTargetState(segments, jobs, target.range);
        if (state == TargetState.NO_OVERLAP) {
            try {
                submitJob(project, model, target);
                state = TargetState.SUBMITTED;
            } catch (Exception e) {
                state = TargetState.RETRYABLE_FAILURE;
                if (previousState != TargetState.RETRYABLE_FAILURE) {
                    log.error("Auto build segment submit failed, project: {}, model: {}, range: {}", project, modelId,
                            target.range, e);
                } else {
                    log.debug("Auto build segment submit still failing, project: {}, model: {}, range: {}", project,
                            modelId, target.range, e);
                }
            }
        }
        recordCycleState(cycleKey, previousState, state, project, modelId, scheduledTime, target.range, segments);
    }

    private BuildTarget createBuildTarget(NDataModel model, AutoSegmentBuildConfig autoSegmentBuild,
            ZonedDateTime scheduledTime) {
        val zoneId = scheduledTime.getZone();
        val logicalDate = scheduledTime.toLocalDate().minusDays(autoSegmentBuild.getLogicalDateOffsetDays());
        val start = parseTime(autoSegmentBuild.getDataRangeStartTime(), false);
        val end = parseTime(autoSegmentBuild.getDataRangeEndTime(), true);
        val startDateTime = LocalDateTime.of(logicalDate, start.getFirst());
        val endDate = end.getSecond() ? logicalDate.plusDays(1) : logicalDate;
        val endDateTime = LocalDateTime.of(endDate, end.getFirst());
        if (!startDateTime.isBefore(endDateTime)) {
            throw new IllegalArgumentException("Auto build segment range start must be earlier than end");
        }

        val startMillis = String.valueOf(startDateTime.atZone(zoneId).toInstant().toEpochMilli());
        val endMillis = String.valueOf(endDateTime.atZone(zoneId).toInstant().toEpochMilli());
        val partitionDateFormat = model.getPartitionDesc().getPartitionDateFormat();
        val normalizedStart = DateFormat.getFormatTimeStamp(startMillis, partitionDateFormat);
        val normalizedEnd = DateFormat.getFormatTimeStamp(endMillis, partitionDateFormat);
        if (normalizedStart >= normalizedEnd) {
            throw new IllegalArgumentException(
                    "Auto build segment range is empty after partition format normalization");
        }
        return new BuildTarget(startMillis, endMillis,
                new SegmentRange.TimePartitionedSegmentRange(normalizedStart, normalizedEnd));
    }

    private String createCycleKey(String project, String modelId, ZonedDateTime scheduledTime,
            SegmentRange<Long> targetRange) {
        return project + "/" + modelId + "/" + scheduledTime.toInstant().toEpochMilli() + "/"
                + targetRange.getStart() + "-" + targetRange.getEnd();
    }

    private void submitJob(String project, NDataModel model, BuildTarget target) throws Exception {
        val params = new IncrementBuildSegmentParams(project, model.getUuid(), target.startMillis, target.endMillis,
                model.getPartitionDesc(), model.getMultiPartitionDesc(), Lists.newArrayList(), true, null);
        modelBuildService.incrementBuildSegmentsByScheduler(params);
    }

    List<AbstractExecutable> getModelBuildJobs(String project, String modelId) {
        val executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        JobTypeEnum[] buildJobTypes = JobTypeEnum.getJobTypeByCategory(JobTypeEnum.Category.BUILD)
                .toArray(new JobTypeEnum[0]);
        return executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, buildJobTypes);
    }

    TargetState evaluateTargetState(List<NDataSegment> segments, List<AbstractExecutable> jobs,
            SegmentRange<Long> targetRange) {
        val coverageState = getCoverageState(segments, targetRange);
        if (coverageState != null) {
            return coverageState;
        }

        List<NDataSegment> buildingSegments = segments.stream()
                .filter(segment -> segment.getStatus() == SegmentStatusEnum.NEW)
                .filter(segment -> segment.getSegRange().overlaps(targetRange)).collect(Collectors.toList());
        if (!buildingSegments.isEmpty()) {
            boolean hasProgressingJob = false;
            for (NDataSegment segment : buildingSegments) {
                boolean hasRelatedJob = false;
                for (AbstractExecutable job : jobs) {
                    if (job.getTargetSegments() == null || !job.getTargetSegments().contains(segment.getId())) {
                        continue;
                    }
                    val jobState = job.getStatusInMem();
                    if (!jobState.isRunning()) {
                        continue;
                    }
                    hasRelatedJob = true;
                    if (!jobState.isProgressing()) {
                        return TargetState.BLOCKED;
                    }
                    hasProgressingJob = true;
                }
                if (!hasRelatedJob) {
                    return TargetState.BLOCKED;
                }
            }
            return hasProgressingJob ? TargetState.IN_PROGRESS : TargetState.BLOCKED;
        }

        boolean hasAvailableOverlap = segments.stream()
                .filter(segment -> segment.getStatus() == SegmentStatusEnum.READY
                        || segment.getStatus() == SegmentStatusEnum.WARNING)
                .anyMatch(segment -> segment.getSegRange().overlaps(targetRange));
        return hasAvailableOverlap ? TargetState.PARTIAL_OVERLAP : TargetState.NO_OVERLAP;
    }

    private TargetState getCoverageState(List<NDataSegment> segments, SegmentRange<Long> targetRange) {
        List<NDataSegment> availableSegments = new ArrayList<>();
        for (NDataSegment segment : segments) {
            if ((segment.getStatus() == SegmentStatusEnum.READY || segment.getStatus() == SegmentStatusEnum.WARNING)
                    && segment.getSegRange().overlaps(targetRange)) {
                availableSegments.add(segment);
            }
        }
        availableSegments.sort(Comparator.comparingLong(segment -> (Long) segment.getSegRange().getStart()));

        long coveredUntil = targetRange.getStart();
        boolean includesWarning = false;
        for (NDataSegment segment : availableSegments) {
            long segmentStart = (Long) segment.getSegRange().getStart();
            long segmentEnd = (Long) segment.getSegRange().getEnd();
            if (segmentEnd <= coveredUntil) {
                continue;
            }
            if (segmentStart > coveredUntil) {
                break;
            }
            coveredUntil = Math.max(coveredUntil, segmentEnd);
            includesWarning |= segment.getStatus() == SegmentStatusEnum.WARNING;
            if (coveredUntil >= targetRange.getEnd()) {
                return includesWarning ? TargetState.COVERED_WITH_WARNING : TargetState.COVERED;
            }
        }
        return null;
    }

    private void recordCycleState(String cycleKey, TargetState previousState, TargetState state, String project,
            String modelId, ZonedDateTime scheduledTime, SegmentRange<Long> targetRange,
            List<NDataSegment> segments) {
        cycleStates.put(cycleKey, state);
        if (state == previousState) {
            return;
        }

        val relatedSegments = segments.stream().filter(segment -> segment.getSegRange().overlaps(targetRange))
                .map(segment -> segment.getId() + ":" + segment.getStatus() + segment.getSegRange())
                .collect(Collectors.joining(",", "[", "]"));
        switch (state) {
        case COVERED:
            log.info("Auto build segment already covered, project: {}, model: {}, range: {}", project, modelId,
                    targetRange);
            break;
        case COVERED_WITH_WARNING:
            log.warn("Auto build segment covered by warning segment, project: {}, model: {}, range: {}, segments: {}",
                    project, modelId, targetRange, relatedSegments);
            break;
        case IN_PROGRESS:
            log.info("Auto build segment waiting for overlapping build job, project: {}, model: {}, range: {}, "
                    + "segments: {}", project, modelId, targetRange, relatedSegments);
            break;
        case BLOCKED:
            log.warn("Auto build segment blocked by non-progressing job or orphan segment, project: {}, model: {}, "
                    + "range: {}, segments: {}", project, modelId, targetRange, relatedSegments);
            break;
        case PARTIAL_OVERLAP:
            log.warn("Auto build segment has a partial overlap, project: {}, model: {}, range: {}, segments: {}",
                    project, modelId, targetRange, relatedSegments);
            break;
        case SUBMITTED:
            log.info("Auto build segment submitted, project: {}, model: {}, scheduled time: {}, range: {}", project,
                    modelId, scheduledTime, targetRange);
            break;
        default:
            break;
        }
    }

    private static class BuildTarget {
        private final String startMillis;
        private final String endMillis;
        private final SegmentRange<Long> range;

        BuildTarget(String startMillis, String endMillis, SegmentRange<Long> range) {
            this.startMillis = startMillis;
            this.endMillis = endMillis;
            this.range = range;
        }
    }

    enum TargetState {
        NO_OVERLAP, COVERED, COVERED_WITH_WARNING, IN_PROGRESS, BLOCKED, PARTIAL_OVERLAP, SUBMITTED, RETRYABLE_FAILURE;

        boolean isComplete() {
            return this == COVERED || this == COVERED_WITH_WARNING;
        }
    }

    private Pair<LocalTime, Boolean> parseTime(String time, boolean allow24Hour) {
        if (allow24Hour && StringUtils.equals(time, END_OF_DAY)) {
            return Pair.newPair(LocalTime.MIDNIGHT, true);
        }
        return Pair.newPair(LocalTime.parse(time, TIME_FORMATTER), false);
    }
}
