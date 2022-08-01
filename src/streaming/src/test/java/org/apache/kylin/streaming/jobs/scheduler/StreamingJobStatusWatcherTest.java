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
package org.apache.kylin.streaming.jobs.scheduler;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

public class StreamingJobStatusWatcherTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExecute() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        runningJobs.add(buildJobId);
        val watcher = new StreamingJobStatusWatcher();
        var jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertTrue(jobMap.isEmpty());
        watcher.execute(runningJobs);
        jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertTrue(jobMap.containsKey(mergeJobId));
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testJobMap() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() - 2 * 60 * 1000);
            copyForWrite.setLastUpdateTime(simpleFormat.format(cal.getTime()));
            copyForWrite.setCurrentStatus(JobStatusEnum.ERROR);
        });
        runningJobs.add(buildJobId);
        val watcher = new StreamingJobStatusWatcher();
        var jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertTrue(jobMap.isEmpty());
        watcher.execute(runningJobs);
        jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertTrue(jobMap.containsKey(mergeJobId));
        jobMap.clear();

        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() - 50 * 60 * 1000);
            copyForWrite.setLastUpdateTime(simpleFormat.format(cal.getTime()));
            copyForWrite.setCurrentStatus(JobStatusEnum.ERROR);
        });
        jobMap = (Map<String, AtomicInteger>) ReflectionUtils.getField(watcher, "jobMap");
        Assert.assertFalse(jobMap.containsKey(mergeJobId));
    }

    @Test
    public void testKillBuildJob() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        runningJobs.add(mergeJobId);
        val watcher = new StreamingJobStatusWatcher();
        for (int i = 0; i < 5; i++) {
            watcher.execute(runningJobs);
        }
        val mergeJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillMergeJob() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STOPPING);

        });
        runningJobs.add(buildJobId);
        val watcher = new StreamingJobStatusWatcher();
        for (int i = 0; i < 8; i++) {
            watcher.execute(runningJobs);
        }
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testKillStartingJob() {
        val runningJobs = new ArrayList<String>();
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.STARTING);

        });
        runningJobs.add(buildJobId);
        val watcher = new StreamingJobStatusWatcher();
        for (int i = 0; i < 8; i++) {
            watcher.execute(runningJobs);
        }
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.ERROR, mergeJobMeta.getCurrentStatus());
    }
}
