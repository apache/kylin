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

package org.apache.kylin.rest.service;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.LogOutputTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopRecsUpdateSchedulerTest extends LogOutputTestCase {

    private final RawRecService rawRecService = Mockito.mock(RawRecService.class);
    private final TopRecsUpdateScheduler scheduler = Mockito.spy(new TopRecsUpdateScheduler());
    private static final String PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        super.createTestMetadata();
        ReflectionTestUtils.setField(scheduler, "rawRecService", rawRecService);
    }

    @After
    public void tearDown() throws Exception {
        super.cleanupTestMetadata();
        scheduler.close();
    }

    @Test
    public void testSaveTimeFail() {
        overwriteSystemProp("kylin.smart.update-topn-time-gap", "1000");
        overwriteSystemProp("kylin.smart.frequency-rule-enable", "false");
        Mockito.doNothing().when(rawRecService).updateCostsAndTopNCandidates(PROJECT);
        Mockito.doThrow(RuntimeException.class).doCallRealMethod().when(scheduler).saveTaskTime(PROJECT);
        scheduler.addProject(PROJECT);
        await().atMost(3, TimeUnit.SECONDS)
                .until(() -> containsLog("Updating default cost and topN recommendations finished."));
    }

    @Test
    public void testSchedulerTask() {
        ReflectionTestUtils.setField(scheduler, "rawRecService", rawRecService);
        overwriteSystemProp("kylin.smart.update-topn-time-gap", "1000");
        overwriteSystemProp("kylin.smart.frequency-rule-enable", "false");
        Mockito.doNothing().when(rawRecService).updateCostsAndTopNCandidates(PROJECT);
        scheduler.addProject(PROJECT);
        await().atMost(3, TimeUnit.SECONDS)
                .until(() -> containsLog("Updating default cost and topN recommendations finished."));
    }
}
