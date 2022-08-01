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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHistoryTaskSchedulerRunnerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private QueryHistoryTaskScheduler qhAccelerateScheduler;

    @Before
    public void setUp() {
        createTestMetadata();
        new SpringContext().setApplicationContext(null);
        qhAccelerateScheduler = Mockito.spy(new QueryHistoryTaskScheduler(PROJECT));
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testQueryHistoryAccelerateRunner() {

        long startTime = System.currentTimeMillis();
        val internalExecute = Lists.<Long> newArrayList();

        val mockSleepTimeSecs = 1;
        val mockSchedulerDelay = 2;

        val queryHistoryAccelerateRunnerMock = qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false) {
            @Override
            public void work() {
                try {
                    TimeUnit.SECONDS.sleep(mockSleepTimeSecs);
                    internalExecute.add((System.currentTimeMillis() - startTime) / 1000);

                    //mock exception
                    throw new RuntimeException("test for exception");
                } catch (InterruptedException e) {
                    log.error("queryHistoryAccelerateRunnerMock is interrupted", e);
                }
            }

        };

        val queryHistoryMetaUpdateRunnerMock = qhAccelerateScheduler.new QueryHistoryMetaUpdateRunner() {
            @Override
            public void work() {
                try {
                    TimeUnit.SECONDS.sleep(mockSleepTimeSecs);
                } catch (InterruptedException e) {
                    log.error("queryHistoryMetaUpdateRunner is interrupted", e);
                }
            }

        };

        ReflectionTestUtils.setField(qhAccelerateScheduler, "taskScheduler", Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("QueryHistoryWorker(project:" + PROJECT + ")")));

        try {
            val schedulerService = (ScheduledExecutorService) ReflectionTestUtils.getField(qhAccelerateScheduler,
                    "taskScheduler");

            schedulerService.scheduleWithFixedDelay(queryHistoryAccelerateRunnerMock, 0, mockSchedulerDelay,
                    TimeUnit.SECONDS);
            schedulerService.scheduleWithFixedDelay(queryHistoryMetaUpdateRunnerMock, 0, mockSchedulerDelay,
                    TimeUnit.SECONDS);

            val schedulerNum = 10;

            TimeUnit.SECONDS.sleep(schedulerNum);

            Assert.assertEquals(internalExecute.size(), schedulerNum / (mockSchedulerDelay + mockSleepTimeSecs));

            for (int i = 0; i < internalExecute.size(); i++) {
                Assert.assertEquals(internalExecute.get(i), i * mockSchedulerDelay + mockSleepTimeSecs * (i + 1), 1);
            }
        } catch (Exception e) {
            log.error("test qhAccelerateScheduler error :", e);
        } finally {
            QueryHistoryTaskScheduler.shutdownByProject(PROJECT);
        }
    }

}
