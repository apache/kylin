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

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;
import static org.apache.kylin.job.execution.ExecutableState.SUCCEED;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.cluster.MockClusterManager;
import org.apache.kylin.rest.constant.Constant;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=")
public class ScheduleServiceTest extends NLocalFileMetadataTestCase {
    @Mock
    private MetadataBackupService backupService = Mockito.spy(MetadataBackupService.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Mock
    private ScheduleService scheduleService = Mockito.spy(ScheduleService.class);

    @Mock
    private ApplicationContext applicationContext = Mockito.spy(ApplicationContext.class);

    @Mock
    private RestTemplate restTemplate = Mockito.mock(RestTemplate.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws JsonProcessingException {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(scheduleService, "projectService", projectService);
        ReflectionTestUtils.setField(scheduleService, "backupService", backupService);
        ReflectionTestUtils.setField(scheduleService, "clusterManager", new MockClusterManager());
        ReflectionTestUtils.setField(scheduleService, "restTemplate", restTemplate);

        val restResult = JsonUtil.writeValueAsString(RestResponse.ok());
        var resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.doReturn(resp).when(restTemplate).exchange(anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(HttpEntity.class), ArgumentMatchers.<Class<String>> any());
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testMetadataBackupException() throws Exception {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "300000ms");
        ReflectionTestUtils.setField(scheduleService, "backupService", new MetadataBackupService() {
            @SneakyThrows(IOException.class)
            public Pair<String, String> backupAll() {
                throw new IOException("backup exception");
            }
        });
        scheduleService.doRoutineTaskForGlobal();
    }

    @Test
    public void testRoutineTask() throws Exception {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "300000ms");
        doNothing().when(projectService).garbageCleanup(anyString(), anyLong());
        scheduleService.doRoutineTaskForProject("default");
    }

    @Test
    public void testRunRoutineJob() {
        prepareBeans(scheduleService);

        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        conf.setProperty("kylin.job.max-concurrent-jobs", "0");
        NProjectManager.getInstance(conf).updateProject("default", copyForWrite -> {
            copyForWrite.putOverrideKylinProps("kylin.job.max-concurrent-jobs", "1");
        });
        await().atMost(Duration.TEN_SECONDS)
                .until(() -> JobContextUtil.getJobContext(conf).getJobScheduler().isMaster());
        ExecutableManager globalExecManager = ExecutableManager.getInstance(conf, GLOBAL_PROJECT);
        ExecutableManager defaultExecManager = ExecutableManager.getInstance(conf, "default");
        Assert.assertTrue(globalExecManager.getAllJobs().isEmpty() && defaultExecManager.getAllJobs().isEmpty());
        scheduleService.routineTask();
        Assert.assertFalse(globalExecManager.getAllJobs().isEmpty() || defaultExecManager.getAllJobs().isEmpty());
        log.info("Start to check job status, at " + System.currentTimeMillis());
        await().atMost(Duration.ONE_MINUTE)
                .until(() -> globalExecManager.getAllExecutables().get(0).getStatus() == SUCCEED
                        && defaultExecManager.getAllExecutables().get(0).getStatus() == SUCCEED);
    }

    @Test
    public void testTimeoutException() throws Exception {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "1000ms");
        ReflectionTestUtils.setField(scheduleService, "backupService", new MetadataBackupService() {
            @SneakyThrows(Exception.class)
            public Pair<String, String> backupAll() {
                synchronized (this) {
                    wait(2000);
                }
                return null;
            }
        });
        scheduleService.doRoutineTaskForGlobal();
    }

    @Test
    public void testTimeoutException2() throws Exception {
        getTestConfig().setProperty("kylin.metadata.ops-cron-timeout", "1000ms");
        ReflectionTestUtils.setField(scheduleService, "backupService", new MetadataBackupService() {
            @SneakyThrows(Exception.class)
            public Pair<String, String> backupAll() {
                synchronized (this) {
                    wait(2000);
                }
                return null;
            }
        });
        doThrow(TimeoutException.class).when(scheduleService).executeTask(any(), anyString(), anyLong());
        scheduleService.doRoutineTaskForGlobal();
    }
}
