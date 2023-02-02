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

package org.apache.kylin.metadata.epoch;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
class EpochOrchestratorTest {

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.enabled", value = "true")
    void testPoolSize1() {
        KylinConfig config = getTestConfig();
        Assertions.assertTrue(config.getEpochCheckerEnabled());

        val epochOrchestrator = new EpochOrchestrator(config);
        val obj = ReflectionTestUtils.getField(epochOrchestrator, "checkerPool");
        Assertions.assertTrue(Objects.nonNull(obj));
        Assertions.assertTrue(obj instanceof ScheduledExecutorService);

        val pool = (ScheduledExecutorService) obj;
        Object obj2 = ReflectionTestUtils.getField(pool, "corePoolSize");
        Assertions.assertNotNull(obj2);
        Assertions.assertEquals(2, ((Integer) obj2).intValue());
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.enabled", value = "false")
    void testPoolSize2() {
        KylinConfig config = getTestConfig();
        Assertions.assertFalse(config.getEpochCheckerEnabled());

        val epochOrchestrator = new EpochOrchestrator(config);
        val obj = ReflectionTestUtils.getField(epochOrchestrator, "checkerPool");
        Assertions.assertTrue(Objects.nonNull(obj));
        Assertions.assertTrue(obj instanceof ScheduledExecutorService);

        val pool = (ScheduledExecutorService) obj;
        Object obj2 = ReflectionTestUtils.getField(pool, "e");
        Assertions.assertNotNull(obj2);
        Assertions.assertTrue(obj2 instanceof ScheduledExecutorService);
        ScheduledExecutorService executors = (ScheduledExecutorService) obj2;
        Object obj3 = ReflectionTestUtils.getField(executors, "corePoolSize");
        Assertions.assertNotNull(obj3);
        Assertions.assertEquals(1, ((Integer) obj3).intValue());
    }
}
