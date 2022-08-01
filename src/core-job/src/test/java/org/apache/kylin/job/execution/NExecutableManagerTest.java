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

package org.apache.kylin.job.execution;

import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.assertj.core.util.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

/**
 *
 */
public class NExecutableManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private NExecutableManager manager;

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);

        for (String jobPath : manager.getJobs()) {
            System.out.println("deleting " + jobPath);
            manager.deleteJob(jobPath);
        }

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() {
        assertNotNull(manager);
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        long createTime = manager.getJob(executable.getId()).getCreateTime();
        assertNotEquals(0L, createTime);
        List<AbstractExecutable> result = manager.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = manager.getJob(executable.getId());
        assertJobEqual(executable, another);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, "test output");
        assertNotEquals(0L, manager.getJob(executable.getId()).getStartTime());
        Assert.assertEquals(createTime, manager.getJob(executable.getId()).getCreateTime());
        assertNotEquals(0L, manager.getJob(executable.getId()).getLastModified());
        assertJobEqual(executable, manager.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(DEFAULT_PROJECT);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable1 = new SucceedTestExecutable();
        job.addTask(executable1);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);
        assertEquals(2, job.getTasks().size());
        assertNotNull(job.getTask(SucceedTestExecutable.class));
        AbstractExecutable anotherJob = manager.getJob(job.getId());
        assertEquals(DefaultChainedExecutable.class, anotherJob.getClass());
        assertEquals(2, ((DefaultChainedExecutable) anotherJob).getTasks().size());
        assertNotNull(((DefaultChainedExecutable) anotherJob).getTask(SucceedTestExecutable.class));

        job.setProject(DEFAULT_PROJECT);
        executable.setProject(DEFAULT_PROJECT);
        executable1.setProject(DEFAULT_PROJECT);

        assertJobEqual(job, anotherJob);
    }

    @Test
    public void testValidStateTransfer() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        String id = job.getId();
        UnitOfWork.doInTransactionWithRetry(() -> {
            manager.addJob(job);
            manager.updateJobOutput(id, ExecutableState.RUNNING);
            manager.updateJobOutput(id, ExecutableState.ERROR);
            manager.updateJobOutput(id, ExecutableState.READY);
            manager.updateJobOutput(id, ExecutableState.RUNNING);
            manager.updateJobOutput(id, ExecutableState.READY);
            manager.updateJobOutput(id, ExecutableState.RUNNING);
            manager.updateJobOutput(id, ExecutableState.SUCCEED);
            return null;
        }, DEFAULT_PROJECT);
    }

    @Test
    public void testValidStateTransfer_clear_sparkInfo() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        String id = job.getId();
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_URL, "yarn app url");
        UnitOfWork.doInTransactionWithRetry(() -> {
            manager.addJob(job);
            for (ExecutableState state : ExecutableState.values()) {
                if (Arrays.asList(ExecutableState.RUNNING, ExecutableState.ERROR, ExecutableState.PAUSED)
                        .contains(state)) {
                    manager.updateJobOutput(id, state, extraInfo, null, null);
                    Assert.assertTrue(
                            manager.getJob(job.getId()).getExtraInfo().containsKey(ExecutableConstants.YARN_APP_URL));
                    manager.updateJobOutput(id, ExecutableState.READY);
                    assertFalse(
                            manager.getJob(job.getId()).getExtraInfo().containsKey(ExecutableConstants.YARN_APP_URL));
                }
            }
            return null;
        }, DEFAULT_PROJECT);
    }

    @Test(expected = IllegalStateException.class)
    public void testDropJobException() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.deleteJob(executable.getId());
    }

    @Test
    public void testDropJobSucceed() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED);
        manager.deleteJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        assertFalse(executables.contains(executable));
    }

    @Test
    public void testDropJobSuicidal() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUICIDAL);
        manager.suicideJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        assertFalse(executables.contains(executable));
    }

    @Test
    public void testDiscardAndDropJob() throws InterruptedException {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.discardJob(executable.getId());

        val duration = executable.getDuration();
        Thread.sleep(3000);
        assertEquals(duration, executable.getDuration());

        manager.deleteJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        assertFalse(executables.contains(executable));
    }

    @Test
    public void testResumeAndPauseJob() throws InterruptedException {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(DEFAULT_PROJECT);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        executable.setProject(DEFAULT_PROJECT);
        job.addTask(executable);
        SucceedTestExecutable executable1 = new SucceedTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);
        manager.pauseJob(job.getId());
        AbstractExecutable anotherJob = manager.getJob(job.getId());
        assertEquals(ExecutableState.PAUSED, anotherJob.getStatus());
        manager.resumeJob(job.getId());
        assertEquals(ExecutableState.READY, anotherJob.getStatus());
        manager.pauseJob(job.getId());
        val duration = job.getDuration();
        Thread.sleep(3000);
        assertEquals(duration, job.getDuration());
        manager.resumeJob(job.getId());
        assertEquals(ExecutableState.READY, anotherJob.getStatus());
    }

    @Test(expected = KylinException.class)
    public void testInvalidStateTransfer() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);
        manager.updateJobOutput(job.getId(), ExecutableState.ERROR);
        manager.updateJobOutput(job.getId(), ExecutableState.PAUSED);
    }

    @Test
    public void testResumeAllRunningJobsHappyCase() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_URL, "yarn app url");
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, extraInfo, null, null);

        AbstractExecutable job = manager.getJob(executable.getId());
        assertEquals(ExecutableState.RUNNING, job.getStatus());

        job = manager.getJob(executable.getId());
        Assert.assertTrue(job.getExtraInfo().containsKey(ExecutableConstants.YARN_APP_URL));
        manager.resumeAllRunningJobs();

        job = manager.getJob(executable.getId());
        Assert.assertEquals(job.getStatus(), ExecutableState.READY);
        assertFalse(job.getExtraInfo().containsKey(ExecutableConstants.YARN_APP_URL));
    }

    @Test
    public void testResumeRunningJobs() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());

        thrown.expect(KylinException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Can't RESUME job"));
        manager.resumeJob(job.getId());
    }

    @Test
    public void testResumeReadyJobs() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.READY);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.READY, job.getStatus());

        thrown.expect(KylinException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Can't RESUME job"));
        manager.resumeJob(job.getId());
    }

    @Test
    public void testResumeDiscardedJobs() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.DISCARDED);
        AbstractExecutable job = manager.getJob(executable.getId());
        assertEquals(ExecutableState.DISCARDED, job.getStatus());

        thrown.expect(KylinException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Can't RESUME job"));
        manager.resumeJob(job.getId());
    }

    @Test
    public void testResumeErrorJobs() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.ERROR);
        AbstractExecutable job = manager.getJob(executable.getId());
        assertEquals(ExecutableState.ERROR, job.getStatus());
        manager.resumeJob(job.getId());
        Assert.assertEquals(ExecutableState.READY, job.getStatus());
    }

    @Test
    public void testResumeSuicidalJobs() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUICIDAL);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, job.getStatus());

        thrown.expect(KylinException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Can't RESUME job"));
        manager.resumeJob(job.getId());
    }

    @Test
    public void testResumeSucceedJobs() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, job.getStatus());

        thrown.expect(KylinException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Can't RESUME job"));
        manager.resumeJob(job.getId());
    }

    @Test
    public void testResumeAllRunningJobsIsolationWithProject() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        AbstractExecutable job = manager.getJob(executable.getId());
        assertEquals(ExecutableState.RUNNING, job.getStatus());

        // another NExecutableManager in project ssb
        NExecutableManager ssbManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "ssb");
        BaseTestExecutable ssbExecutable = new SucceedTestExecutable();
        ssbExecutable.setJobType(JobTypeEnum.INDEX_BUILD);
        ssbManager.addJob(ssbExecutable);
        ssbManager.updateJobOutput(ssbExecutable.getId(), ExecutableState.RUNNING);

        AbstractExecutable ssbJob = ssbManager.getJob(ssbExecutable.getId());
        assertEquals(ssbJob.getStatus(), ExecutableState.RUNNING);

        manager.resumeAllRunningJobs();

        job = manager.getJob(executable.getId());
        // it only resume running jobs in project default, so the status of the job convert to ready
        assertEquals(ExecutableState.READY, job.getStatus());

        job = ssbManager.getJob(ssbExecutable.getId());
        // the status of jobs in project ssb is still running
        assertEquals(ExecutableState.RUNNING, job.getStatus());

    }

    private static void assertJobEqual(Executable one, Executable another) {
        assertEquals(one.getClass(), another.getClass());
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());

        assertTrue((one.getParams() == null && another.getParams() == null)
                || (one.getParams() != null && another.getParams() != null));

        if (one.getParams() != null) {
            assertEquals(one.getParams().size(), another.getParams().size());
            for (String key : one.getParams().keySet()) {
                assertEquals(one.getParams().get(key), another.getParams().get(key));
            }
        }
        if (one instanceof ChainedExecutable) {
            assertTrue(another instanceof ChainedExecutable);
            List<? extends Executable> onesSubs = ((ChainedExecutable) one).getTasks();
            List<? extends Executable> anotherSubs = ((ChainedExecutable) another).getTasks();
            assertTrue((onesSubs == null && anotherSubs == null) || (onesSubs != null && anotherSubs != null));
            if (onesSubs != null) {
                assertEquals(onesSubs.size(), anotherSubs.size());
                for (int i = 0; i < onesSubs.size(); ++i) {
                    assertJobEqual(onesSubs.get(i), anotherSubs.get(i));
                }
            }
        }
    }

    @Test
    public void testResumeJob_AllStep() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("test");
        job.setProject(DEFAULT_PROJECT);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable2 = new SucceedTestExecutable();
        job.addTask(executable2);
        manager.addJob(job);
        manager.pauseJob(job.getId());
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable2.getId(), ExecutableState.PAUSED);

        manager.restartJob(job.getId());
        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.READY, job1.getStatus());

        job1.getTasks().forEach(task -> {
            Assert.assertEquals(ExecutableState.READY, task.getStatus());
        });
    }

    @Test
    public void testPauseJob_IncBuildJobDataFlowStatusChange() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INC_BUILD.toString());
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject(DEFAULT_PROJECT);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        manager.addJob(job);
        manager.pauseJob(job.getId());

        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.PAUSED, job1.getStatus());

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT)
                .getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testPauseJob_IndexBuildJobDataFlowStatusNotChange() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject(DEFAULT_PROJECT);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        manager.addJob(job);
        manager.pauseJob(job.getId());

        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.PAUSED, job1.getStatus());

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT)
                .getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dataflow.getStatus());
    }

    @Test
    public void testEmptyType_ThrowException() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject(DEFAULT_PROJECT);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        val po = NExecutableManager.toPO(job, DEFAULT_PROJECT);
        po.setType(null);

        val executableDao = NExecutableDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val savedPO = executableDao.addJob(po);

        Assert.assertNull(manager.getJob(savedPO.getId()));
    }

    @Test
    public void testForCoverage() throws IOException {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309gg");
        job.setProject(DEFAULT_PROJECT);
        job.setPriority(1);
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        val po = NExecutableManager.toPO(job, DEFAULT_PROJECT);
        val executableDao = NExecutableDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        executableDao.addJob(po);
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);
        val runExecutables = executableManager.getRunningExecutables(DEFAULT_PROJECT, null);
        Assert.assertEquals(1, runExecutables.size());
        val runJobTypeExecutables = executableManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD);
        Assert.assertEquals(0, runJobTypeExecutables.size());
        val executables = executableManager.getExecutablesByStatus(
                com.google.common.collect.Lists.newArrayList(job.getId()),
                com.google.common.collect.Lists.newArrayList(ExecutableState.READY));
        Assert.assertEquals(1, executables.size());
        val executables2 = executableManager.getExecutablesByStatusList(Sets.newHashSet(ExecutableState.READY));
        val executables21 = executableManager.getPartialExecutablesByStatusList(Sets.newHashSet(ExecutableState.READY),
                path -> StringUtils.endsWith(path, "89af4ee2-2cdb-4b07-b39e-4c29856309gg"));
        val executables22 = executableManager.getPartialExecutablesByStatusList(Sets.newHashSet(ExecutableState.READY),
                path -> StringUtils.endsWith(path, "89af4ee2-2cdb-4b07-b39e-4c29856309gg12"));
        val executables3 = executableManager.getExecutablesByStatus(ExecutableState.READY);
        Assert.assertEquals(executables2.size(), executables3.size());
        Assert.assertEquals(executables2.size(), executables21.size());
        Assert.assertEquals(executables2.size(), executables22.size());
        val executables4 = executableManager.getAllExecutables(0L, Long.MAX_VALUE);
        Assert.assertEquals(1, executables4.size());
        val executables5 = executableManager.getRunningJobs(10);
        Assert.assertEquals(1, executables5.size());
        val executables6 = executableManager.getAllJobs(0L, Long.MAX_VALUE);
        Assert.assertEquals(1, executables6.size());
    }

    @Test
    public void testEmailNotificationContent() {
        val project = DEFAULT_PROJECT;
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setProject(project);
        val start = "2015-01-01 00:00:00";
        val end = "2015-02-01 00:00:00";
        job.setParam(NBatchConstants.P_DATA_RANGE_START, SegmentRange.dateToLong(start) + "");
        job.setParam(NBatchConstants.P_DATA_RANGE_END, SegmentRange.dateToLong(end) + "");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        EmailNotificationContent content = EmailNotificationContent.createContent(JobIssueEnum.JOB_ERROR, job);
        Assert.assertTrue(content.getEmailTitle().contains(JobIssueEnum.JOB_ERROR.getDisplayName()));
        Assert.assertTrue(!content.getEmailBody().contains("$"));
        Assert.assertTrue(content.getEmailBody().contains(project));
        Assert.assertTrue(content.getEmailBody().contains(job.getName()));

        content = EmailNotificationContent.createContent(JobIssueEnum.LOAD_EMPTY_DATA, job);
        Assert.assertTrue(content.getEmailBody().contains(job.getTargetModelAlias()));
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", job.getTargetModelId());
        content = EmailNotificationContent.createContent(JobIssueEnum.SOURCE_RECORDS_CHANGE, job);
        Assert.assertTrue(content.getEmailBody().contains(start));
        Assert.assertTrue(content.getEmailBody().contains(end));

    }

    @Test
    public void testGetSampleDataFromHDFS() throws IOException {
        final String junitFolder = temporaryFolder.getRoot().getAbsolutePath();
        final String mainFolder = junitFolder + "/testGetSampleDataFromHDFS";
        File file = new File(mainFolder);
        if (!file.exists()) {
            Assert.assertTrue(file.mkdir());
        } else {
            Assert.fail("exist the test case folder: " + mainFolder);
        }

        int nLines = 100;
        for (Integer logLines : Arrays.asList(0, 1, 70, 150, 230, 1024, nLines)) {
            String hdfsPath = mainFolder + "/hdfs.log" + logLines;
            List<String> text = Lists.newArrayList();
            for (int i = 0; i < logLines; i++) {
                text.add("INFO: this is line " + i);
            }

            FileUtils.writeLines(new File(hdfsPath), text);

            Assert.assertTrue(manager.isHdfsPathExists(hdfsPath));

            String sampleLog = manager.getSampleDataFromHDFS(hdfsPath, nLines);

            String[] logArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(sampleLog, "\n");

            int expectedLines;
            if (logLines <= nLines) {
                expectedLines = logLines;
            } else if (logLines < nLines * 2) {
                expectedLines = logLines + 1;
            } else {
                expectedLines = nLines * 2 + 1;
            }

            Assert.assertEquals(expectedLines, logArray.length);
            if (logLines > 0) {
                Assert.assertEquals("INFO: this is line 0", logArray[0]);
                Assert.assertEquals("INFO: this is line " + (logLines - 1), logArray[logArray.length - 1]);
            }
        }
    }

    @Test
    public void testUpdateYarnApplicationJob() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);

        var appIds = manager.getYarnApplicationJobs(executable.getId());
        Assert.assertEquals(0, appIds.size());

        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_ID, "test1");
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, extraInfo, null, null);
        appIds = manager.getYarnApplicationJobs(executable.getId());
        Assert.assertEquals(1, appIds.size());
        Assert.assertTrue(appIds.contains("test1"));

        extraInfo.put(ExecutableConstants.YARN_APP_ID, "test2");
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, extraInfo, null, null);
        appIds = manager.getYarnApplicationJobs(executable.getId());
        Assert.assertEquals(2, appIds.size());
        Assert.assertTrue(appIds.contains("test1"));
        Assert.assertTrue(appIds.contains("test2"));

    }

    @Test
    public void testGetLastSuccessExecByModel() {
        String modelId = "1";
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setTargetSubject(modelId);
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, Collections.emptyMap(), null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, Collections.emptyMap(), null, null);

        modelId = "2";
        executable = new SucceedTestExecutable();
        executable.setId(executable.getId() + "-" + modelId);
        executable.setTargetSubject(modelId);
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, Collections.emptyMap(), null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, Collections.emptyMap(), null, null);

        long result = manager.getLastSuccessExecDurationByModel(modelId, manager.getAllJobs());
        Assert.assertEquals(result, executable.getDuration());

        result = manager.getLastSuccessExecDurationByModel("3", manager.getAllJobs());
        Assert.assertEquals(0, result);

    }

    @Test
    public void testGetMaxDurationRunningExecByModel() {
        String modelId = "1";
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setTargetSubject(modelId);
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, Collections.emptyMap(), null, null);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        modelId = "2";
        executable = new SucceedTestExecutable();
        executable.setId(executable.getId() + "-" + modelId);
        executable.setTargetSubject(modelId);
        executable.setProject(DEFAULT_PROJECT);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, Collections.emptyMap(), null, null);

        long result = manager.getMaxDurationRunningExecDurationByModel(modelId, manager.getAllJobs());
        Assert.assertTrue(Math.abs(result - executable.getDuration()) < 3000);

        result = manager.getMaxDurationRunningExecDurationByModel("3", manager.getAllJobs());
        Assert.assertEquals(0, result);
    }

    @Test
    public void testGetgetTargetModelAlias() {
        val project = "streaming_test";
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setProject(project);
        val start = "2015-01-01 00:00:00";
        val end = "2015-02-01 00:00:00";
        job.setParam(NBatchConstants.P_DATA_RANGE_START, SegmentRange.dateToLong(start) + "");
        job.setParam(NBatchConstants.P_DATA_RANGE_END, SegmentRange.dateToLong(end) + "");

        job.setTargetSubject("334671fd-e383-4fc9-b5c2-94fce832f77a");
        Assert.assertEquals("streaming_test", job.getTargetModelAlias());

        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .dropDataflow("334671fd-e383-4fc9-b5c2-94fce832f77a");
        Assert.assertEquals("streaming_test_b05034a8", job.getTargetModelAlias());

        job.setTargetSubject("554671fd-e383-4fc9-b5c2-94fce832f77a");
        Assert.assertEquals("batch", job.getTargetModelAlias());

        job.setTargetSubject("554671fd-e383-4fc9-b5c2-94fce832f77b");
        Assert.assertEquals(null, job.getTargetModelAlias());

    }

    @Test
    public void testGetStreamingOutputFromHDFSByJobId() throws IOException {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de_build";

        File emptyFile = temporaryFolder.newFile("driver.0000000000000.log");
        File file = temporaryFolder.newFile("driver." + System.currentTimeMillis() + ".log");
        for (int i = 0; i < 200; i++) {
            Files.write(file.toPath(), String.format(Locale.ROOT, "lines: %s\n", i).getBytes(Charset.defaultCharset()),
                    StandardOpenOption.APPEND);
        }

        String[] exceptLines = Files.readAllLines(file.toPath()).toArray(new String[0]);
        String jobLogDir = KylinConfig.getInstanceFromEnv().getStreamingJobTmpOutputStorePath("default", jobId);

        // The test log directory does not exist
        assertEquals("", manager.getStreamingOutputFromHDFS(jobId).getVerboseMsg());
        try {
            manager.getFilePathsFromHDFSDir(jobLogDir);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
        }

        // The test log directory exists but there are no log files
        Path jobLogDirPath = new Path(jobLogDir);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(jobLogDirPath);
        try {
            manager.getStreamingOutputFromHDFS(jobId);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("The current job has not been started and no log has been generated: " + jobLogDir,
                    e.getMessage());
        }
        assertTrue(CollectionUtils.isEmpty(manager.getFilePathsFromHDFSDir(jobLogDir, false)));

        // There are multiple log files in the test
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), jobLogDirPath);
        fs.copyFromLocalFile(new Path(emptyFile.getAbsolutePath()), jobLogDirPath);

        List<String> logFilePathList = manager.getFilePathsFromHDFSDir(jobLogDir, false);
        assertEquals(2, logFilePathList.size());
        assertEquals("driver.0000000000000.log", new Path(logFilePathList.get(0)).getName());

        // Test log file sampling
        String verboseMsg = manager.getStreamingOutputFromHDFS(jobId).getVerboseMsg();
        String[] actualVerboseMsgLines = StringUtils.splitByWholeSeparatorPreserveAllTokens(verboseMsg, "\n");
        ArrayList<String> exceptLinesL = com.google.common.collect.Lists.newArrayList(exceptLines);
        exceptLinesL.add("================================================================");
        assertTrue(Sets.newHashSet(exceptLinesL).containsAll(Sets.newHashSet(actualVerboseMsgLines)));

        // Test log file InputStream
        String sampleLog = "";
        try (InputStream verboseMsgStream = manager.getStreamingOutputFromHDFS(jobId, Integer.MAX_VALUE)
                .getVerboseMsgStream();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(verboseMsgStream, Charset.defaultCharset()))) {

            String line;
            StringBuilder sampleData = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                if (sampleData.length() > 0) {
                    sampleData.append('\n');
                }
                sampleData.append(line);
            }

            sampleLog = sampleData.toString();
        }
        String[] actualLines = StringUtils.splitByWholeSeparatorPreserveAllTokens(sampleLog, "\n");
        assertTrue(Arrays.deepEquals(exceptLines, actualLines));

    }
}
