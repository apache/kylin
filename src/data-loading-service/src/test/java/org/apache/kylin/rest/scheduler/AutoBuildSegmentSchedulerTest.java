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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.AutoSegmentBuildConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

@MetadataInfo
class AutoBuildSegmentSchedulerTest {
    private static final String PROJECT = "default";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @Test
    void testLatestScheduledTimeUsesProjectTimeZone() {
        val scheduler = new AutoBuildSegmentScheduler();
        val zoneId = ZoneId.of("Asia/Shanghai");
        val currentTime = Instant.parse("2026-08-12T16:00:20Z");

        assertEquals(ZonedDateTime.of(2026, 8, 13, 0, 0, 0, 0, zoneId),
                scheduler.getLatestScheduledTime("00:00:00", zoneId, currentTime));
        assertEquals(ZonedDateTime.of(2026, 8, 12, 0, 1, 0, 0, zoneId),
                scheduler.getLatestScheduledTime("00:01:00", zoneId, currentTime));
        assertThrows(DateTimeParseException.class,
                () -> scheduler.getLatestScheduledTime("24:00:00", zoneId, currentTime));
    }

    @Test
    void testDispatchCatchesUpLatestCycleAndBuildsConfiguredRange() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(Collections.emptyList()).when(scheduler).getModelBuildJobs(PROJECT, MODEL_ID);
        Mockito.doReturn(AutoBuildSegmentScheduler.TargetState.NO_OVERLAP).when(scheduler)
                .evaluateTargetState(Mockito.anyList(), Mockito.anyList(), Mockito.any());
        enableAutoSegmentBuild();

        val projectConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig();
        val zoneId = ZoneId.of(projectConfig.getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.plusHours(6).toInstant());

        val paramsCaptor = ArgumentCaptor.forClass(IncrementBuildSegmentParams.class);
        Mockito.verify(modelBuildService).incrementBuildSegmentsByScheduler(paramsCaptor.capture());
        val logicalDate = scheduledTime.toLocalDate().minusDays(1);
        assertEquals(String.valueOf(LocalDateTime.of(logicalDate, LocalTime.MIDNIGHT).atZone(zoneId).toInstant()
                .toEpochMilli()), paramsCaptor.getValue().getStart());
        assertEquals(String.valueOf(LocalDateTime.of(logicalDate.plusDays(1), LocalTime.MIDNIGHT).atZone(zoneId)
                .toInstant().toEpochMilli()), paramsCaptor.getValue().getEnd());
    }

    @Test
    void testCompletedCycleIsNotEvaluatedAgain() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(Collections.emptyList()).when(scheduler).getModelBuildJobs(PROJECT, MODEL_ID);
        Mockito.doReturn(AutoBuildSegmentScheduler.TargetState.COVERED).when(scheduler)
                .evaluateTargetState(Mockito.anyList(), Mockito.anyList(), Mockito.any());
        enableAutoSegmentBuild();

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(2026, 8, 12, 1, 0, 0, 0, zoneId);
        scheduler.dispatch(scheduledTime.plusMinutes(1).toInstant());
        scheduler.dispatch(scheduledTime.plusMinutes(2).toInstant());

        Mockito.verifyNoInteractions(modelBuildService);
        Mockito.verify(scheduler).evaluateTargetState(Mockito.anyList(), Mockito.anyList(), Mockito.any());
    }

    @Test
    void testBuildOfflineModel() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(Collections.emptyList()).when(scheduler).getModelBuildJobs(PROJECT, MODEL_ID);
        Mockito.doReturn(AutoBuildSegmentScheduler.TargetState.NO_OVERLAP).when(scheduler)
                .evaluateTargetState(Mockito.anyList(), Mockito.anyList(), Mockito.any());
        enableAutoSegmentBuild();
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflowStatus(MODEL_ID,
                RealizationStatusEnum.OFFLINE);

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.plusSeconds(1).toInstant());

        Mockito.verify(modelBuildService).incrementBuildSegmentsByScheduler(Mockito.any());
    }

    @Test
    void testSkipProjectWhenAutoSegmentBuildDisabled() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        enableAutoSegmentBuild();
        setProjectAutoSegmentBuildEnabled(false);

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.plusSeconds(1).toInstant());

        Mockito.verifyNoInteractions(modelBuildService);
    }

    @Test
    void testSkipIncompleteConfig() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        enableAutoSegmentBuild();
        NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataModel(MODEL_ID,
                copyForWrite -> copyForWrite.getSegmentConfig().getAutoSegmentBuild()
                        .setLogicalDateOffsetDays(null));

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.plusSeconds(1).toInstant());

        Mockito.verifyNoInteractions(modelBuildService);
    }

    @Test
    void testTargetCoveredByExistingSegments() {
        val scheduler = new AutoBuildSegmentScheduler();
        val target = range(100L, 200L);

        assertEquals(AutoBuildSegmentScheduler.TargetState.COVERED,
                scheduler.evaluateTargetState(Collections.singletonList(segment("superset", 50L, 250L,
                        SegmentStatusEnum.READY)), Collections.emptyList(), target));
        assertEquals(AutoBuildSegmentScheduler.TargetState.COVERED,
                scheduler.evaluateTargetState(Arrays.asList(segment("first", 100L, 150L, SegmentStatusEnum.READY),
                        segment("second", 150L, 200L, SegmentStatusEnum.READY)), Collections.emptyList(), target));
        assertEquals(AutoBuildSegmentScheduler.TargetState.COVERED_WITH_WARNING,
                scheduler.evaluateTargetState(Collections.singletonList(segment("warning", 100L, 200L,
                        SegmentStatusEnum.WARNING)), Collections.emptyList(), target));
    }

    @Test
    void testProgressingJobIsCheckedByRange() {
        val scheduler = new AutoBuildSegmentScheduler();
        val target = range(100L, 200L);

        NDataSegment larger = segment("larger", 50L, 250L, SegmentStatusEnum.NEW);
        assertEquals(AutoBuildSegmentScheduler.TargetState.IN_PROGRESS, scheduler.evaluateTargetState(
                Collections.singletonList(larger), Collections.singletonList(job(ExecutableState.RUNNING, "larger")),
                target));

        NDataSegment smaller = segment("smaller", 100L, 150L, SegmentStatusEnum.NEW);
        assertEquals(AutoBuildSegmentScheduler.TargetState.IN_PROGRESS, scheduler.evaluateTargetState(
                Collections.singletonList(smaller), Collections.singletonList(job(ExecutableState.PENDING, "smaller")),
                target));

        NDataSegment disjoint = segment("disjoint", 200L, 300L, SegmentStatusEnum.NEW);
        assertEquals(AutoBuildSegmentScheduler.TargetState.NO_OVERLAP, scheduler.evaluateTargetState(
                Collections.singletonList(disjoint),
                Collections.singletonList(job(ExecutableState.RUNNING, "disjoint")), target));
    }

    @Test
    void testErrorPausedAndOrphanSegmentsBlockTarget() {
        val scheduler = new AutoBuildSegmentScheduler();
        val target = range(100L, 200L);
        NDataSegment building = segment("building", 100L, 200L, SegmentStatusEnum.NEW);

        assertEquals(AutoBuildSegmentScheduler.TargetState.BLOCKED, scheduler.evaluateTargetState(
                Collections.singletonList(building), Collections.singletonList(job(ExecutableState.ERROR, "building")),
                target));
        assertEquals(AutoBuildSegmentScheduler.TargetState.BLOCKED, scheduler.evaluateTargetState(
                Collections.singletonList(building), Collections.singletonList(job(ExecutableState.PAUSED, "building")),
                target));
        assertEquals(AutoBuildSegmentScheduler.TargetState.BLOCKED, scheduler.evaluateTargetState(
                Collections.singletonList(building), Collections.emptyList(), target));
    }

    @Test
    void testPartialAvailableSegmentIsConflict() {
        val scheduler = new AutoBuildSegmentScheduler();
        val target = range(100L, 200L);
        List<NDataSegment> segments = Collections
                .singletonList(segment("partial", 100L, 150L, SegmentStatusEnum.READY));

        assertEquals(AutoBuildSegmentScheduler.TargetState.PARTIAL_OVERLAP,
                scheduler.evaluateTargetState(segments, Collections.emptyList(), target));
    }

    @Test
    void testInProgressCycleIsReevaluated() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(Collections.emptyList()).when(scheduler).getModelBuildJobs(PROJECT, MODEL_ID);
        Mockito.doReturn(AutoBuildSegmentScheduler.TargetState.IN_PROGRESS,
                AutoBuildSegmentScheduler.TargetState.NO_OVERLAP).when(scheduler)
                .evaluateTargetState(Mockito.anyList(), Mockito.anyList(), Mockito.any());
        enableAutoSegmentBuild();

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.plusMinutes(1).toInstant());
        scheduler.dispatch(scheduledTime.plusMinutes(2).toInstant());

        Mockito.verify(modelBuildService).incrementBuildSegmentsByScheduler(Mockito.any());
    }

    @Test
    void testRetryableSubmissionFailureIsReevaluated() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(Collections.emptyList()).when(scheduler).getModelBuildJobs(PROJECT, MODEL_ID);
        Mockito.doReturn(AutoBuildSegmentScheduler.TargetState.NO_OVERLAP).when(scheduler)
                .evaluateTargetState(Mockito.anyList(), Mockito.anyList(), Mockito.any());
        Mockito.doThrow(new IllegalStateException("transient failure")).doReturn(null).when(modelBuildService)
                .incrementBuildSegmentsByScheduler(Mockito.any());
        enableAutoSegmentBuild();

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.plusMinutes(1).toInstant());
        scheduler.dispatch(scheduledTime.plusMinutes(2).toInstant());

        Mockito.verify(modelBuildService, Mockito.times(2)).incrementBuildSegmentsByScheduler(Mockito.any());
    }

    private SegmentRange<Long> range(long start, long end) {
        return new SegmentRange.TimePartitionedSegmentRange(start, end);
    }

    private NDataSegment segment(String id, long start, long end, SegmentStatusEnum status) {
        NDataSegment segment = Mockito.mock(NDataSegment.class);
        Mockito.when(segment.getId()).thenReturn(id);
        Mockito.when(segment.getStatus()).thenReturn(status);
        Mockito.when(segment.getSegRange()).thenReturn(range(start, end));
        return segment;
    }

    private AbstractExecutable job(ExecutableState state, String... segmentIds) {
        AbstractExecutable job = Mockito.mock(AbstractExecutable.class);
        Mockito.when(job.getStatusInMem()).thenReturn(state);
        Mockito.when(job.getTargetSegments()).thenReturn(Arrays.asList(segmentIds));
        return job;
    }

    private NDataModel enableAutoSegmentBuild() {
        setProjectAutoSegmentBuildEnabled(true);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        return modelManager.updateDataModel(MODEL_ID, copyForWrite -> {
            AutoSegmentBuildConfig config = new AutoSegmentBuildConfig();
            config.setEnabled(true);
            config.setTriggerTime("01:00:00");
            config.setLogicalDateOffsetDays(1);
            config.setDataRangeStartTime("00:00:00");
            config.setDataRangeEndTime(AutoSegmentBuildConfig.END_OF_DAY);
            copyForWrite.getSegmentConfig().setAutoSegmentBuild(config);
        });
    }

    private void setProjectAutoSegmentBuildEnabled(boolean enabled) {
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject(PROJECT,
                copyForWrite -> copyForWrite.getSegmentConfig().setAutoSegmentBuildEnabled(enabled));
    }
}
