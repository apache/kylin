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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import org.apache.kylin.metadata.epoch.EpochManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=")
public class ScheduleServiceTest extends NLocalFileMetadataTestCase {
    @Mock
    private MetadataBackupService backupService = Mockito.spy(MetadataBackupService.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Mock
    private ScheduleService scheduleService = Mockito.spy(ScheduleService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(scheduleService, "projectService", projectService);
        ReflectionTestUtils.setField(scheduleService, "backupService", backupService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testMetadataBackupException() {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "300000ms");
        ReflectionTestUtils.setField(scheduleService, "backupService", new MetadataBackupService() {
            @SneakyThrows(IOException.class)
            public void backupAll() {
                throw new IOException("backup exception");
            }
        });
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.updateAllEpochs();
        scheduleService.routineTask();
    }

    @Test
    public void testRoutineTask() {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "300000ms");
        doNothing().when(projectService).garbageCleanup(anyLong());
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.updateAllEpochs();
        scheduleService.routineTask();
    }

    @Test
    public void testTimeoutException() {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "1000ms");
        ReflectionTestUtils.setField(scheduleService, "backupService", new MetadataBackupService() {
            @SneakyThrows(Exception.class)
            public void backupAll() {
                synchronized (this) {
                    wait(2000);
                }
            }
        });
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.updateAllEpochs();
        doNothing().when(projectService).garbageCleanup(anyLong());
        scheduleService.routineTask();
    }

    @Test
    public void testTimeoutException2() throws Exception {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "1000ms");
        ReflectionTestUtils.setField(scheduleService, "backupService", new MetadataBackupService() {
            @SneakyThrows(Exception.class)
            public void backupAll() {
                synchronized (this) {
                    wait(2000);
                }
            }
        });
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.updateAllEpochs();
        doNothing().when(projectService).garbageCleanup(anyLong());
        doThrow(TimeoutException.class).when(scheduleService).executeTask(any(), anyString(), anyLong());
        scheduleService.routineTask();
    }
}
