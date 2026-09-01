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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.AutoSegmentBuildConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
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
    void testInitialLookbackUsesDispatcherInterval() {
        val scheduler = new AutoBuildSegmentScheduler();
        val currentTime = Instant.parse("2026-08-12T16:00:20Z");
        ReflectionTestUtils.setField(scheduler, "dispatcherIntervalMillis", 300_000L);

        Instant previousTime = ReflectionTestUtils.invokeMethod(scheduler, "getPreviousDispatchTime", currentTime);

        assertEquals(currentTime.minusSeconds(300), previousTime);
    }

    @Test
    void testDispatchModelOnlyOnceForTriggerWindow() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(false).when(scheduler).hasRunningModelBuildJob(PROJECT, MODEL_ID);
        enableAutoSegmentBuild();

        val projectConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig();
        val zoneId = ZoneId.of(projectConfig.getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.minusSeconds(1).toInstant(), scheduledTime.plusSeconds(1).toInstant());

        val paramsCaptor = ArgumentCaptor.forClass(IncrementBuildSegmentParams.class);
        Mockito.verify(modelBuildService).incrementBuildSegmentsByScheduler(paramsCaptor.capture());
        val logicalDate = scheduledTime.toLocalDate().minusDays(1);
        assertEquals(String.valueOf(LocalDateTime.of(logicalDate, LocalTime.MIDNIGHT).atZone(zoneId).toInstant()
                .toEpochMilli()), paramsCaptor.getValue().getStart());
        assertEquals(String.valueOf(LocalDateTime.of(logicalDate.plusDays(1), LocalTime.MIDNIGHT).atZone(zoneId)
                .toInstant().toEpochMilli()), paramsCaptor.getValue().getEnd());

        Mockito.clearInvocations(modelBuildService);
        scheduler.dispatch(scheduledTime.toInstant(), scheduledTime.plusSeconds(30).toInstant());
        Mockito.verifyNoInteractions(modelBuildService);
    }

    @Test
    void testSkipWhenModelHasProgressingBuildJob() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(true).when(scheduler).hasRunningModelBuildJob(PROJECT, MODEL_ID);
        val model = enableAutoSegmentBuild();
        val config = model.getSegmentConfig().getAutoSegmentBuild();

        scheduler.submitJob(PROJECT, model, config, ZonedDateTime.of(2026, 8, 12, 1, 0, 0, 0, ZoneId.of("UTC")));

        Mockito.verifyNoInteractions(modelBuildService);
    }

    @Test
    void testSkipOfflineModel() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(false).when(scheduler).hasRunningModelBuildJob(PROJECT, MODEL_ID);
        enableAutoSegmentBuild();
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflowStatus(MODEL_ID,
                RealizationStatusEnum.OFFLINE);

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.minusSeconds(1).toInstant(), scheduledTime.plusSeconds(1).toInstant());

        Mockito.verifyNoInteractions(modelBuildService);
    }

    @Test
    void testSkipIncompleteConfig() throws Exception {
        val modelBuildService = Mockito.mock(ModelBuildService.class);
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        ReflectionTestUtils.setField(scheduler, "modelBuildService", modelBuildService);
        Mockito.doReturn(false).when(scheduler).hasRunningModelBuildJob(PROJECT, MODEL_ID);
        enableAutoSegmentBuild();
        NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataModel(MODEL_ID,
                copyForWrite -> copyForWrite.getSegmentConfig().getAutoSegmentBuild()
                        .setLogicalDateOffsetDays(null));

        val zoneId = ZoneId.of(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT)
                .getConfig().getTimeZone());
        val scheduledTime = ZonedDateTime.of(LocalDate.of(2026, 8, 12), LocalTime.of(1, 0), zoneId);
        scheduler.dispatch(scheduledTime.minusSeconds(1).toInstant(), scheduledTime.plusSeconds(1).toInstant());

        Mockito.verifyNoInteractions(modelBuildService);
    }

    private NDataModel enableAutoSegmentBuild() {
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
}
