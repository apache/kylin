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

package org.apache.kylin.rest.service.task;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo
class SpringSessionCleanSchedulerTest {

    @Test
    void testTaskSubmit() {
        SpringSessionCleanScheduler springSessionCleanScheduler = new SpringSessionCleanScheduler();
        springSessionCleanScheduler.init();
        Assertions.assertEquals(0, cleanJobCount());
        springSessionCleanScheduler.submitJob();
        Assertions.assertEquals(1, cleanJobCount());
    }

    private static long cleanJobCount() {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), ResourceStore.GLOBAL_PROJECT)
                .getAllJobs().stream().filter(job -> job.getJobType() == JobTypeEnum.SPRING_SESSION_CLEAN_EXPIRED)
                .count();
    }
}
