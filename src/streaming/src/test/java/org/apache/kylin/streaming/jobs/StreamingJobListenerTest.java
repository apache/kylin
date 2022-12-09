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
package org.apache.kylin.streaming.jobs;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.event.StreamingJobDropEvent;
import org.apache.kylin.streaming.event.StreamingJobKillEvent;
import org.apache.kylin.streaming.event.StreamingJobMetaCleanEvent;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.apache.spark.launcher.SparkAppHandle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import lombok.val;
import lombok.var;

public class StreamingJobListenerTest extends StreamingTestCase {

    private static final String PROJECT = "streaming_test";
    private static final String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();
    private final StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        EventBusFactory.getInstance().register(eventListener, true);
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().restart();
        this.cleanupTestMetadata();
    }

    @Test
    public void testStateChangedToRunning() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val listener = new StreamingJobListener(PROJECT, jobId);
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> copyForWrite.setSkipListener(true));
        listener.stateChanged(mockRunningState());
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        Assert.assertFalse(jobMeta.isSkipListener());
    }

    @Test
    public void testStateChangedToFailure() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING));
        val listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockFailedState());
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STOPPING);
            copyForWrite.setSkipListener(true);
        });
        listener.stateChanged(mockFailedState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPING, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() - 2 * 60 * 1000);
            copyForWrite.setLastUpdateTime(simpleFormat.format(cal.getTime()));
        });
        listener.stateChanged(mockKilledState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPING, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedToKilled() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING));
        val listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockKilledState());
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
            copyForWrite.setSkipListener(true);
        });
        listener.stateChanged(mockKilledState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());

        listener.stateChanged(mockKilledState());
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
    }

    @Test
    public void testStateChangedToFinish() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.toString());
        val listener = new StreamingJobListener(PROJECT, jobId);
        listener.stateChanged(mockRunningState());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        listener.stateChanged(mockFinishedState());
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
    }

    @Test
    public void testOnStreamingJobKill() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        String project = "streaming_test";
        val config = getTestConfig();
        var mgr = StreamingJobManager.getInstance(config, project);
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        mgr.updateStreamingJob(buildJobId, copyForWrite -> copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING));
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING));
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        var mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
        EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, modelId));
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testOnStreamingJobDrop() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        String project = "streaming_test";
        val config = getTestConfig();
        var mgr = StreamingJobManager.getInstance(config, project);
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        var mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNotNull(buildJobMeta);
        Assert.assertNotNull(mergeJobMeta);
        EventBusFactory.getInstance().postSync(new StreamingJobDropEvent(project, modelId));
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNull(buildJobMeta);
        Assert.assertNull(mergeJobMeta);
    }

    @Test
    public void testOnStreamingJobMetaCleanEvent() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        Assert.assertTrue(mainDir.exists());

        EventBusFactory.getInstance().postSync(
                new StreamingJobMetaCleanEvent(Collections.singletonList(new Path(mainDir.getAbsolutePath()))));

        //delete success
        Assert.assertFalse(mainDir.exists());
    }

    @Test
    public void testOnStreamingJobMetaCleanEvent_EmptyPath() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        Assert.assertTrue(mainDir.exists());

        EventBusFactory.getInstance().postSync(new StreamingJobMetaCleanEvent(Collections.emptyList()));

        Assert.assertTrue(mainDir.exists());
    }

    @Test
    public void testOnStreamingJobMetaCleanEvent_InvalidPath() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        Assert.assertTrue(mainDir.exists());

        EventBusFactory.getInstance().postSync(
                new StreamingJobMetaCleanEvent(Collections.singletonList(new Path(mainDir.getAbsolutePath() + "xxx"))));

        Assert.assertTrue(mainDir.exists());
    }

    private SparkAppHandle mockRunningState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.RUNNING;
            }
        };
    }

    private SparkAppHandle mockFailedState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.FAILED;
            }
        };
    }

    private SparkAppHandle mockKilledState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.KILLED;
            }
        };
    }

    private SparkAppHandle mockFinishedState() {
        return new AbstractSparkAppHandle() {
            @Override
            public State getState() {
                return State.FINISHED;
            }
        };
    }

    public static abstract class AbstractSparkAppHandle implements SparkAppHandle {
        @Override
        public void addListener(Listener listener) {

        }

        @Override
        public String getAppId() {
            return "local-" + RandomUtil.randomUUID();
        }

        @Override
        public void stop() {

        }

        @Override
        public void kill() {

        }

        @Override
        public void disconnect() {

        }

        @Override
        public Optional<Throwable> getError() {
            return Optional.empty();
        }
    }
}
