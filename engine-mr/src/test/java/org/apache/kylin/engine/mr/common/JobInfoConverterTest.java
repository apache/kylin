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

package org.apache.kylin.engine.mr.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.junit.Test;

public class JobInfoConverterTest {
    @Test
    public void testParseToJobInstance() {
        TestJob task = new TestJob();
        JobInstance instance = JobInfoConverter.parseToJobInstanceQuietly(task, Maps.newHashMap());
        // no exception thrown is expected
        assertTrue(instance == null);
    }

    @Test
    public void testParseToJobStep() {
        TestJob task = new TestJob();
        JobInstance.JobStep step = JobInfoConverter.parseToJobStep(task, 0, null);
        assertEquals(step.getStatus(), JobStepStatusEnum.PENDING);

        step = JobInfoConverter.parseToJobStep(task, 0, new TestOutput());
        assertEquals(step.getStatus(), JobStepStatusEnum.FINISHED);
    }

    public static class TestJob extends CubingJob {
        public TestJob() {
            super();
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "");
        }
    }

    public static class TestOutput implements Output {

        @Override
        public Map<String, String> getExtra() {
            Map<String, String> extra = Maps.newHashMap();
            extra.put("testkey", "testval");
            return extra;
        }

        @Override
        public String getVerboseMsg() {
            return null;
        }

        @Override
        public ExecutableState getState() {
            return ExecutableState.SUCCEED;
        }

        @Override
        public long getLastModified() {
            return 0;
        }
    }

    @Test
    public void testParseToJobStepStatusReturnsJobStepStatusStopped() {
        ExecutableState executableState = ExecutableState.STOPPED;
        JobStepStatusEnum jobStepStatusEnum = JobInfoConverter.parseToJobStepStatus(executableState);

        assertFalse(jobStepStatusEnum.isComplete());
        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(128, jobStepStatusEnum.getCode());
        assertEquals(JobStepStatusEnum.STOPPED, jobStepStatusEnum);
    }

    @Test
    public void testParseToJobStepStatusReturnsJobStepStatusFinished() {
        ExecutableState executableState = ExecutableState.SUCCEED;
        JobStepStatusEnum jobStepStatusEnum = JobInfoConverter.parseToJobStepStatus(executableState);

        assertTrue(jobStepStatusEnum.isComplete());
        assertEquals(4, jobStepStatusEnum.getCode());
        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(JobStepStatusEnum.FINISHED, jobStepStatusEnum);
    }

    @Test
    public void testParseToJobStepStatusReturnsJobStepStatusDiscarded() {
        ExecutableState executableState = ExecutableState.DISCARDED;
        JobStepStatusEnum jobStepStatusEnum = JobInfoConverter.parseToJobStepStatus(executableState);

        assertTrue(jobStepStatusEnum.isComplete());
        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(16, jobStepStatusEnum.getCode());
        assertEquals(JobStepStatusEnum.DISCARDED, jobStepStatusEnum);
    }

    @Test
    public void testParseToJobStepStatusReturnsJobStepStatusRunning() {
        ExecutableState executableState = ExecutableState.RUNNING;
        JobStepStatusEnum jobStepStatusEnum = JobInfoConverter.parseToJobStepStatus(executableState);

        assertEquals(2, jobStepStatusEnum.getCode());
        assertFalse(jobStepStatusEnum.isComplete());
        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(JobStepStatusEnum.RUNNING, jobStepStatusEnum);
    }

    @Test
    public void testParseToJobStepStatusReturnsJobStepStatusError() {
        ExecutableState executableState = ExecutableState.ERROR;
        JobStepStatusEnum jobStepStatusEnum = JobInfoConverter.parseToJobStepStatus(executableState);

        assertTrue(jobStepStatusEnum.isRunable());
        assertTrue(jobStepStatusEnum.isComplete());
        assertEquals(8, jobStepStatusEnum.getCode());
        assertEquals(JobStepStatusEnum.ERROR, jobStepStatusEnum);
    }

    @Test
    public void testParseToJobStepStatusReturnsJobStepStatusPending() {
        ExecutableState executableState = ExecutableState.READY;
        JobStepStatusEnum jobStepStatusEnum = JobInfoConverter.parseToJobStepStatus(executableState);

        assertTrue(jobStepStatusEnum.isRunable());
        assertEquals(1, jobStepStatusEnum.getCode());
        assertEquals(JobStepStatusEnum.PENDING, jobStepStatusEnum);
    }

    @Test
    public void testParseToJobStatusReturnsJobStatusStopped() {
        ExecutableState executableState = ExecutableState.STOPPED;
        JobStatusEnum jobStatusEnum = JobInfoConverter.parseToJobStatus(executableState);

        assertEquals(32, jobStatusEnum.getCode());
        assertEquals(JobStatusEnum.STOPPED, jobStatusEnum);
    }

    @Test
    public void testParseToJobStatusReturnsJobStatusFinished() {
        ExecutableState executableState = ExecutableState.SUCCEED;
        JobStatusEnum jobStatusEnum = JobInfoConverter.parseToJobStatus(executableState);

        assertEquals(4, jobStatusEnum.getCode());
        assertEquals(JobStatusEnum.FINISHED, jobStatusEnum);
    }

    @Test
    public void testParseToJobStatusReturnsJobStatusError() {
        ExecutableState executableState = ExecutableState.ERROR;
        JobStatusEnum jobStatusEnum = JobInfoConverter.parseToJobStatus(executableState);

        assertEquals(8, jobStatusEnum.getCode());
        assertEquals(JobStatusEnum.ERROR, jobStatusEnum);
    }

    @Test
    public void testParseToJobStatusReturnsJobStatusRunning() {
        ExecutableState executableState = ExecutableState.RUNNING;
        JobStatusEnum jobStatusEnum = JobInfoConverter.parseToJobStatus(executableState);

        assertEquals(2, jobStatusEnum.getCode());
        assertEquals(JobStatusEnum.RUNNING, jobStatusEnum);
    }

    @Test
    public void testParseToJobStatusReturnsJobStatusDiscarded() {
        ExecutableState executableState = ExecutableState.DISCARDED;
        JobStatusEnum jobStatusEnum = JobInfoConverter.parseToJobStatus(executableState);

        assertEquals(16, jobStatusEnum.getCode());
        assertEquals(JobStatusEnum.DISCARDED, jobStatusEnum);
    }

    @Test
    public void testParseToJobStatusReturnsJobStatusPending() {
        ExecutableState executableState = ExecutableState.READY;
        JobStatusEnum jobStatusEnum = JobInfoConverter.parseToJobStatus(executableState);

        assertEquals(1, jobStatusEnum.getCode());
        assertEquals(JobStatusEnum.PENDING, jobStatusEnum);
    }

    @Test
    public void testParseToJobInstanceQuietlyUsingNullCheckpointExecutable() {
        TreeMap<String, Output> treeMap = new TreeMap<>();
        JobInstance jobInstance = JobInfoConverter.parseToJobInstanceQuietly((CheckpointExecutable) null, treeMap);

        assertNull(jobInstance);
    }

    @Test
    public void testParseToJobInstanceQuietlyUsingNullCubingJob() {
        ConcurrentHashMap<String, Output> concurrentHashMap = new ConcurrentHashMap<>();
        JobInstance jobInstance = JobInfoConverter.parseToJobInstanceQuietly((CubingJob) null, concurrentHashMap);

        assertNull(jobInstance);
    }

}
