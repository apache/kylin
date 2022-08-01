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

package org.apache.kylin.tool.garbage;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExecutableCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    private NExecutableManager manager;
    private NExecutableDao dao;

    @Before
    public void init() {
        createTestMetadata();
        manager = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        dao = NExecutableDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupWithUnexpiredJob() {
        String jobId = RandomUtil.randomUUIDStr();
        createUnexpiredJob(jobId);
        Assert.assertEquals(1, manager.getJobs().size());
        manager.discardJob(jobId);
        new ExecutableCleaner(DEFAULT_PROJECT).cleanup();
        Assert.assertEquals(1, manager.getJobs().size());
    }

    @Test
    public void testCleanupWithRunningJob() {
        createExpiredJob(RandomUtil.randomUUIDStr());
        Assert.assertEquals(1, manager.getJobs().size());
        new ExecutableCleaner(DEFAULT_PROJECT).cleanup();
        Assert.assertEquals(1, manager.getJobs().size());
    }

    @Test
    public void testCleanupWithCleanableJob() {
        String jobId = RandomUtil.randomUUIDStr();
        createExpiredJob(jobId);
        manager.discardJob(jobId);
        Assert.assertEquals(1, manager.getJobs().size());
        new ExecutableCleaner(DEFAULT_PROJECT).cleanup();
        Assert.assertEquals(0, manager.getJobs().size());
    }

    private void createExpiredJob(String jobId) {
        long survivalTime = getTestConfig().getExecutableSurvivalTimeThreshold();
        createJob(jobId, System.currentTimeMillis() - survivalTime - 2000);
    }

    private void createUnexpiredJob(String jobId) {
        long survivalTime = getTestConfig().getExecutableSurvivalTimeThreshold();
        createJob(jobId, System.currentTimeMillis() - survivalTime + 2000);
    }

    private void createJob(String jobId, long createTime) {
        MockCleanableExecutable executable = new MockCleanableExecutable();
        executable.setParam("test1", "test1");
        executable.setId(jobId);
        executable.setProject(DEFAULT_PROJECT);
        ExecutablePO po = NExecutableManager.toPO(executable, DEFAULT_PROJECT);
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setCreateTime(createTime);
        po.setOutput(executableOutputPO);
        dao.addJob(po);
    }
}
