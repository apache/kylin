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
package org.apache.kylin.streaming.jobs.thread;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.app.StreamingMergeEntry;
import org.apache.kylin.streaming.jobs.impl.StreamingJobLauncher;
import org.apache.kylin.streaming.util.AwaitUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class StreamingJobRunnerTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private StreamingJobRunner runner;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testStop() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val app = new StreamingMergeEntry();
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, buildJobId));
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, mergeJobId));

        runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.init();
        runner.stop();
        Assert.assertTrue(app.isGracefulShutdown(PROJECT, buildJobId));
        runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        runner.init();
        runner.stop();
        Assert.assertTrue(app.isGracefulShutdown(PROJECT, mergeJobId));
    }

    @Test
    public void testStopWithNoInitial() {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.stop();
        val app = new StreamingMergeEntry();
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, buildJobId));
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        Assert.assertFalse(app.isGracefulShutdown(PROJECT, mergeJobId));
    }

    /**
     * test StreamingJobRunner class 's  run method
     */
    @Test
    public void testBuildJobRunner_run() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.init();
        AwaitUtils.await(() -> runner.run(), 10000, () -> {
        });
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testBuildJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        AwaitUtils.await(() -> launcher.launch(), 10000, () -> {
        });
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testMergeJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        AwaitUtils.await(() -> launcher.launch(), 10000, () -> {
        });
    }
}
