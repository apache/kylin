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
package org.apache.kylin.streaming.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

public class MetaInfoUpdaterTest extends NLocalFileMetadataTestCase {

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
    public void testUpdate() {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236633";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);
        Assert.assertEquals(17, seg.getLayoutSize());
        val layout = NDataLayout.newDataLayout(df, seg.getId(), 10002L);
        MetaInfoUpdater.update(PROJECT, seg, layout);
        NDataflowManager mgr1 = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df1 = mgr1.getDataflow(dataflowId);
        val seg1 = df1.getSegment(segId);
        Assert.assertEquals(17, seg1.getLayoutSize());
    }

    @Test
    public void testUpdateJobState() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastUpdateTime());
        Assert.assertNull(jobMeta.getLastEndTime());
        Assert.assertNull(jobMeta.getLastStartTime());

        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.ERROR);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.ERROR, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastEndTime());

        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.RUNNING);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastStartTime());

        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.STOPPED);
        mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, jobMeta.getCurrentStatus());
        Assert.assertNotNull(jobMeta.getLastEndTime());
        Assert.assertNotNull(jobMeta.getLastUpdateTime());

        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setYarnAppId("application_1626786933603_1752");
            copyForWrite
                    .setYarnAppUrl("http://sandbox.hortonworks.com:8088/cluster/app/application_1626786933603_1752");
        });
        MetaInfoUpdater.updateJobState(PROJECT, jobId, JobStatusEnum.STARTING);
        jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(StringUtils.EMPTY, jobMeta.getYarnAppId());
        Assert.assertEquals(StringUtils.EMPTY, jobMeta.getYarnAppUrl());
    }

    @Test
    public void testMarkGracefulShutdown() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val testConfig = getTestConfig();
        MetaInfoUpdater.markGracefulShutdown(PROJECT, jobId);
        var mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        var jobMeta = mgr.getStreamingJobByUuid(jobId);
        Assert.assertEquals(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN, jobMeta.getAction());
    }
}
