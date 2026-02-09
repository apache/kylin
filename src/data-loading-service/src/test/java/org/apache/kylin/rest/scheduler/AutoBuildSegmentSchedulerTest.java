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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.AutoSegmentBuildConfig;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.service.ModelBuildService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

@MetadataInfo
class AutoBuildSegmentSchedulerTest {

    @Test
    void testReconcileStartAndStopTask() {
        val scheduler = Mockito.spy(new AutoBuildSegmentScheduler());
        val taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.initialize();
        ReflectionTestUtils.setField(scheduler, "projectScheduler", taskScheduler);
        ReflectionTestUtils.setField(scheduler, "modelBuildService", Mockito.mock(ModelBuildService.class));

        try {
            val project = "default";
            val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            modelManager.updateDataModel(modelId, copyForWrite -> {
                AutoSegmentBuildConfig config = new AutoSegmentBuildConfig();
                config.setEnabled(true);
                config.setTriggerTime("01:00:00");
                config.setLogicalDateOffsetDays(1);
                config.setDataRangeStartTime("00:00:00");
                config.setDataRangeEndTime("24:00:00");
                copyForWrite.getSegmentConfig().setAutoSegmentBuild(config);
            });
            scheduler.schedulerAutoBuildSegment();
            assertFalse(scheduler.getTaskFutures().isEmpty());

            modelManager.updateDataModel(modelId, copyForWrite -> copyForWrite.getSegmentConfig().setAutoSegmentBuild(null));
            scheduler.schedulerAutoBuildSegment();
            assertTrue(scheduler.getTaskFutures().isEmpty());
        } finally {
            taskScheduler.shutdown();
        }
    }
}
