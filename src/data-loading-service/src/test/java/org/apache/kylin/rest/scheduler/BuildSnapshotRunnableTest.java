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

import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_REFRESH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.val;
import lombok.var;

@MetadataInfo
class BuildSnapshotRunnableTest {
    private final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);

    @Test
    void buildSnapshot() throws JsonProcessingException {
        val thread = new BuildSnapshotRunnable();
        thread.setProject("project");
        thread.setConfig(KylinConfig.readSystemKylinConfig());
        thread.setRestTemplate(restTemplate);
        thread.setNeedRefresh(true);
        thread.setTableIdentity("default.table_" + RandomUtil.randomUUIDStr().replace("-", "_"));

        val response = new JobInfoResponse();
        val jobInfo = new JobInfo();
        jobInfo.setJobName("test_" + RandomUtil.randomUUIDStr());
        jobInfo.setJobId(RandomUtil.randomUUIDStr());
        response.setJobs(Lists.newArrayList(jobInfo));
        var restResult = JsonUtil.writeValueAsString(RestResponse.ok(response));
        var resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);

        thread.buildSnapshot();
        var snapshotJobFile = thread.readSnapshotJobFile();
        assertEquals(3, snapshotJobFile.size());
        assertEquals("false", snapshotJobFile.get("build_error"));
        assertEquals("", snapshotJobFile.get("error_message"));
        assertEquals(jobInfo.getJobId(), snapshotJobFile.get("job_id"));

        restResult = JsonUtil.writeValueAsString(new RestResponse<>(KylinException.CODE_UNDEFINED, response, ""));
        resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);
        var errorMessage = "";
        try {
            thread.buildSnapshot();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinRuntimeException);
            errorMessage = e.getMessage();
            assertEquals("Project[project] Snapshot[" + thread.getTableIdentity() + "] buildSnapshot failed",
                    errorMessage);
        }
        snapshotJobFile = thread.readSnapshotJobFile();
        assertEquals(3, snapshotJobFile.size());
        assertEquals("true", snapshotJobFile.get("build_error"));
        assertEquals("Project[project] Snapshot[" + thread.getTableIdentity() + "] buildSnapshot failed",
                snapshotJobFile.get("error_message"));
        assertEquals("", snapshotJobFile.get("job_id"));
    }

    @Test
    void buildSnapshotFailed() throws JsonProcessingException {
        val thread = new BuildSnapshotRunnable();
        thread.setProject("project");
        thread.setConfig(KylinConfig.readSystemKylinConfig());
        thread.setRestTemplate(restTemplate);
        thread.setNeedRefresh(true);
        thread.setTableIdentity("default.table_" + RandomUtil.randomUUIDStr().replace("-", "_"));

        val response = new JobInfoResponse();
        val jobInfo = new JobInfo();
        jobInfo.setJobName("test_" + RandomUtil.randomUUIDStr());
        jobInfo.setJobId(RandomUtil.randomUUIDStr());
        response.setJobs(Lists.newArrayList(jobInfo));
        val restResult = JsonUtil.writeValueAsString(RestResponse.ok(response));
        val resp = new ResponseEntity<>(restResult, HttpStatus.NO_CONTENT);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<String>> any())).thenReturn(resp);

        var errorMessage = "";
        try {
            thread.buildSnapshot();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinRuntimeException);
            errorMessage = e.getMessage();
            assertEquals("Project[project] Snapshot[" + thread.getTableIdentity() + "] buildSnapshot failed",
                    errorMessage);
        }
        val snapshotJobFile = thread.readSnapshotJobFile();
        assertEquals(3, snapshotJobFile.size());
        assertEquals("true", snapshotJobFile.get("build_error"));
        assertEquals("Project[project] Snapshot[" + thread.getTableIdentity() + "] buildSnapshot failed",
                snapshotJobFile.get("error_message"));
        assertEquals("", snapshotJobFile.get("job_id"));
    }

    @Test
    void checkSnapshotJobFile() {
        val thread = new BuildSnapshotRunnable();
        thread.setConfig(KylinConfig.getInstanceFromEnv());
        thread.setTableIdentity("default.table_" + RandomUtil.randomUUIDStr().replace("-", "_"));
        val jobId = RandomUtil.randomUUIDStr();
        thread.saveSnapshotJobFile(true, "error_message", jobId);
        assertTrue(thread.checkSnapshotJobFile());

        thread.saveSnapshotJobFile(false, "error_message", "");
        assertTrue(thread.checkSnapshotJobFile());

        thread.saveSnapshotJobFile(false, "error_message", jobId);
        assertTrue(thread.checkSnapshotJobFile());

        try (val executableManagerMockedStatic = Mockito.mockStatic(ExecutableManager.class)) {
            val executableManager = Mockito.mock(ExecutableManager.class);
            executableManagerMockedStatic.when(() -> ExecutableManager.getInstance(Mockito.any(), Mockito.any()))
                    .thenReturn(executableManager);
            assertTrue(thread.checkSnapshotJobFile());

            val autoRefreshJob = Mockito.mock(AbstractExecutable.class);
            when(executableManager.getJob(any())).thenReturn(autoRefreshJob);

            when(autoRefreshJob.getStatus()).thenReturn(ExecutableState.PAUSED);
            assertFalse(thread.checkSnapshotJobFile());

            when(autoRefreshJob.getStatus()).thenReturn(ExecutableState.SUCCEED);
            assertFalse(thread.checkSnapshotJobFile());
        }
    }

    @Test
    void checkAutoRefreshJobSuccessOrRunning() {
        val jobId = RandomUtil.randomUUIDStr();
        val thread = new BuildSnapshotRunnable();
        thread.setConfig(KylinConfig.getInstanceFromEnv());
        assertFalse(thread.checkAutoRefreshJobSuccessOrRunning(jobId));

        try (val executableManagerMockedStatic = Mockito.mockStatic(ExecutableManager.class)) {
            val executableManager = Mockito.mock(ExecutableManager.class);
            executableManagerMockedStatic.when(() -> ExecutableManager.getInstance(Mockito.any(), Mockito.any()))
                    .thenReturn(executableManager);
            assertFalse(thread.checkAutoRefreshJobSuccessOrRunning(jobId));

            val autoRefreshJob = Mockito.mock(AbstractExecutable.class);
            when(executableManager.getJob(any())).thenReturn(autoRefreshJob);

            when(autoRefreshJob.getStatus()).thenReturn(ExecutableState.PAUSED);
            assertTrue(thread.checkAutoRefreshJobSuccessOrRunning(jobId));

            when(autoRefreshJob.getStatus()).thenReturn(ExecutableState.SUCCEED);
            assertTrue(thread.checkAutoRefreshJobSuccessOrRunning(jobId));
        }
    }

    @Test
    void snapshotJobFile() {
        val thread = new BuildSnapshotRunnable();
        thread.setConfig(KylinConfig.getInstanceFromEnv());
        thread.setTableIdentity("default.table_" + RandomUtil.randomUUIDStr().replace("-", "_"));
        val jobId = RandomUtil.randomUUIDStr();
        thread.saveSnapshotJobFile(false, "error_message", jobId);
        val snapshotJob = thread.readSnapshotJobFile();
        assertEquals(3, snapshotJob.size());
        assertEquals("false", snapshotJob.get("build_error"));
        assertEquals("error_message", snapshotJob.get("error_message"));
        assertEquals(jobId, snapshotJob.get("job_id"));
    }

    @Test
    void snapshotJobFileNotExists() {
        val thread = new BuildSnapshotRunnable();
        thread.setConfig(KylinConfig.getInstanceFromEnv());
        thread.setTableIdentity("default.table_" + RandomUtil.randomUUIDStr().replace("-", "_"));
        val snapshotJob = thread.readSnapshotJobFile();
        assertEquals(0, snapshotJob.size());
    }

    @Test
    void checkNeedBuildPartitionAndSetTableOption() throws JsonProcessingException {
        val thread = new BuildSnapshotRunnable();
        thread.setTableIdentity("default.table");
        val req = Maps.newHashMap();
        val runningJobs = Lists.<NSparkSnapshotJob> newArrayList();
        var result = thread.checkNeedBuildPartitionAndSetTableOption(req, runningJobs);
        assertTrue(result);

        thread.setPartitionColumn("partition");
        result = thread.checkNeedBuildPartitionAndSetTableOption(req, runningJobs);
        assertTrue(result);

        thread.setNeedRefreshPartitionsValue(Sets.newHashSet("1", "2", "3"));
        val job1 = new NSparkSnapshotJob();
        job1.setParam(NBatchConstants.P_SELECTED_PARTITION_VALUE,
                JsonUtil.writeValueAsString(Sets.newHashSet("1", "2")));
        val job2 = new NSparkSnapshotJob();
        job2.setParam(NBatchConstants.P_SELECTED_PARTITION_VALUE,
                JsonUtil.writeValueAsString(Sets.newHashSet("3", "4")));
        runningJobs.add(job1);
        runningJobs.add(job2);
        result = thread.checkNeedBuildPartitionAndSetTableOption(req, runningJobs);
        assertTrue(result);

        runningJobs.remove(job2);
        result = thread.checkNeedBuildPartitionAndSetTableOption(req, runningJobs);
        assertFalse(result);
        assertEquals(1, req.size());
        val options = ((HashMap) req.get("options"));
        assertEquals(1, options.size());
        val tableOptions = ((HashMap) options.get(thread.getTableIdentity()));
        assertEquals(3, tableOptions.size());
        assertEquals("partition", tableOptions.get("partition_col"));
        assertTrue((Boolean) tableOptions.get("incremental_build"));
        assertEquals(Sets.newHashSet("3"), tableOptions.get("partitions_to_build"));
    }

    @Test
    void createRequestAndCheckRunningJob() throws JsonProcessingException {
        try (val mockStatic = Mockito.mockStatic(ExecutableManager.class)) {
            val executableManager = Mockito.mock(ExecutableManager.class);
            mockStatic.when(() -> ExecutableManager.getInstance(any(), anyString())).thenReturn(executableManager);
            val runningJobs = Lists.<AbstractExecutable> newArrayList();
            val job1 = new NSparkSnapshotJob();
            job1.setParam(NBatchConstants.P_SELECTED_PARTITION_VALUE,
                    JsonUtil.writeValueAsString(Sets.newHashSet("1", "2")));
            val job2 = new NSparkSnapshotJob();
            job2.setParam(NBatchConstants.P_SELECTED_PARTITION_VALUE,
                    JsonUtil.writeValueAsString(Sets.newHashSet("3", "4")));
            Mockito.when(executableManager.jobInfoToExecutable(executableManager.fetchJobsByTypesAndStates(null,
                    Lists.newArrayList(SNAPSHOT_BUILD.name(), SNAPSHOT_REFRESH.name()), null,
                    ExecutableState.getNotFinalStates()))).thenReturn(runningJobs);

            val thread = new BuildSnapshotRunnable();
            thread.setTableIdentity("default.table");
            thread.setProject("default");
            try {
                thread.createRequestAndCheckRunningJob();
            } catch (Exception e) {
                assertTrue(e instanceof KylinRuntimeException);
                assertEquals(
                        "Project[default] Snapshot[default.table] buildSnapshot failed, because has running snapshot job",
                        e.getMessage());
            }

            thread.setPartitionColumn("partition");
            try {
                thread.createRequestAndCheckRunningJob();
            } catch (Exception e) {
                assertTrue(e instanceof KylinRuntimeException);
                assertEquals(
                        "Project[default] Snapshot[default.table] buildSnapshot failed, because none partitions need build",
                        e.getMessage());
            }
        }
    }
}
