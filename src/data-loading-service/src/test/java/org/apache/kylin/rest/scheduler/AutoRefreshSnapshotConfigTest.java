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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.scheduler.EpochStartedNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

class AutoRefreshSnapshotConfigTest {
    @Test
    void testRegisterScheduler() throws Exception {
        registerScheduler(true);
        registerScheduler(false);
    }

    void registerScheduler(Boolean isJobNode) throws Exception {
        try (val mockStatic = Mockito.mockStatic(EventBusFactory.class);
                val configStatic = Mockito.mockStatic(KylinConfig.class)) {
            val config = Mockito.mock(KylinConfig.class);
            Mockito.when(config.isJobNode()).thenReturn(isJobNode);
            configStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(config);

            val buildConfig = new AutoRefreshSnapshotConfig();
            val eventBus = Mockito.mock(EventBusFactory.class);
            mockStatic.when(EventBusFactory::getInstance).thenReturn(eventBus);
            buildConfig.init();
            val scheduler = Mockito.mock(AutoRefreshSnapshotScheduler.class);
            ReflectionTestUtils.setField(buildConfig, "scheduler", scheduler);

            Mockito.doNothing().when(scheduler).afterPropertiesSet();
            buildConfig.registerScheduler(new EpochStartedNotifier());

            Mockito.doThrow(new Exception("test")).when(scheduler).afterPropertiesSet();
            buildConfig.registerScheduler(new EpochStartedNotifier());
        }
    }
}
