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

package org.apache.kylin.job.impl.threadpool;

import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ErrorTestExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FailedTestExecutable;
import org.apache.kylin.job.execution.FiveSecondErrorTestExecutable;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.LongRunningTestExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.NoErrorStatusExecutableOnModel;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.junit.rule.Repeat;
import org.apache.kylin.junit.rule.RepeatRule;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class NDefaultSchedulerTest extends BaseSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultSchedulerTest.class);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public NDefaultSchedulerTest() {
        super("default");
    }

    @Override
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.auto-set-concurrent-jobs", "true");
        overwriteSystemProp("kylin.env", "UT");
        overwriteSystemProp("kylin.job.check-quota-storage-enabled", "true");
        super.setup();
    }

    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestRule chain = RuleChain.outerRule(new RepeatRule()).around(thrown);

    @Test
    public void testSingleTaskJob() {
        logger.info("testSingleTaskJob");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testGetLogStream() throws IOException {
        File file = temporaryFolder.newFile("execute_output.json." + System.currentTimeMillis() + ".log");
        for (int i = 0; i < 200; i++) {
            Files.write(file.toPath(), String.format(Locale.ROOT, "lines: %s\n", i).getBytes(Charset.defaultCharset()),
                    StandardOpenOption.APPEND);
        }
        InputStream logStream = executableManager.getLogStream(file.getAbsolutePath());
        Assert.assertNotNull(logStream);
        Assert.assertTrue(logStream instanceof FSDataInputStream);

        InputStream logStreamNull = executableManager.getLogStream(file.getAbsolutePath() + "/123");
        Assert.assertNull(logStreamNull);
    }

    @Test
    public void testGetOutputFromHDFSByJobId() throws IOException {
        File file = temporaryFolder.newFile("execute_output.json." + System.currentTimeMillis() + ".log");
        for (int i = 0; i < 200; i++) {
            Files.write(file.toPath(), String.format(Locale.ROOT, "lines: %s\n", i).getBytes(Charset.defaultCharset()),
                    StandardOpenOption.APPEND);
        }

        String[] exceptLines = Files.readAllLines(file.toPath()).toArray(new String[0]);

        NExecutableManager manager = executableManager;
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setStatus("SUCCEED");
        executableOutputPO.setContent("succeed");
        executableOutputPO.setLogPath(file.getAbsolutePath());
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de"), executableOutputPO);

        String sampleLog = "";
        try (InputStream verboseMsgStream = executableManager
                .getOutputFromHDFSByJobId("e1ad7bb0-522e-456a-859d-2eab1df448de",
                        "e1ad7bb0-522e-456a-859d-2eab1df448de", Integer.MAX_VALUE)
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
        Assert.assertTrue(Arrays.deepEquals(exceptLines, actualLines));

        String verboseMsg = executableManager.getOutputFromHDFSByJobId("e1ad7bb0-522e-456a-859d-2eab1df448de",
                "e1ad7bb0-522e-456a-859d-2eab1df448de", 100).getVerboseMsg();
        String[] actualVerboseMsgLines = StringUtils.splitByWholeSeparatorPreserveAllTokens(verboseMsg, "\n");
        ArrayList<String> exceptLinesL = Lists.newArrayList(exceptLines);
        exceptLinesL.add("================================================================");
        Assert.assertTrue(Sets.newHashSet(exceptLinesL).containsAll(Sets.newHashSet(actualVerboseMsgLines)));
    }

    @Test
    public void testSucceed() {
        logger.info("testSucceed");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        assertMemoryRestore(currMem);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        assertTimeLegal(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
        //in case hdfs write is not finished yet
        getConditionFactory().untilAsserted(() -> {
            Assertions
                    .assertThat(executableManager.getOutputFromHDFSByJobId(job.getId(), task1.getId()).getVerboseMsg())
                    .contains("succeed");
            Assertions
                    .assertThat(executableManager.getOutputFromHDFSByJobId(job.getId(), task2.getId()).getVerboseMsg())
                    .contains("succeed");
        });
        assertTimeSucceed(createTime, job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    private void assertJobTime(final AbstractExecutable job) {
        assertJobTime(job, 0);
    }

    private void assertJobTime(final AbstractExecutable job, long deltaTime) {

        if (!(job instanceof DefaultChainedExecutable)) {
            return;

        }
        DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;

        long lastExecutableEndTime = chainedExecutable.getOutput().getCreateTime();
        long totalWaitTime = 0L;
        long totalDuration = 0L;
        var lastTaskStatus = chainedExecutable.getStatus();
        for (AbstractExecutable task : chainedExecutable.getTasks()) {

            long taskStartTime = task.getStartTime();
            int stepId = task.getStepId();

            //test final state time legal
            if (task.getStatus().isFinalState()) {
                Assert.assertTrue(taskStartTime > 0L);
                Assert.assertTrue(task.getEndTime() > 0L);
            }

            if (stepId > 0 && (lastExecutableEndTime == 0 || lastTaskStatus != ExecutableState.SUCCEED)) {
                Assert.assertEquals(0, task.getWaitTime());
            } else if (task.getStartTime() == 0) {
                Assert.assertEquals(System.currentTimeMillis() - lastExecutableEndTime, task.getWaitTime(), deltaTime);
            } else {
                Assert.assertEquals(task.getStartTime() - lastExecutableEndTime, task.getWaitTime());
            }

            lastExecutableEndTime = task.getOutput().getEndTime();
            lastTaskStatus = task.getStatus();

            totalWaitTime += task.getWaitTime();
            totalDuration += task.getDuration();
        }

        Assert.assertEquals(chainedExecutable.getWaitTime(), totalWaitTime, deltaTime);
        Assert.assertEquals(chainedExecutable.getDuration(), totalDuration, deltaTime);
        Assert.assertEquals(chainedExecutable.getTotalDurationTime(),
                chainedExecutable.getWaitTime() + chainedExecutable.getDuration(), deltaTime);
    }

    private void assertTimeSucceed(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        assertJobRun(createTime, job);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            Assert.assertTrue(chainedExecutable.getTasks().stream()
                    .allMatch(task -> ExecutableState.SUCCEED == executableManager.getOutput(task.getId()).getState()));
        }

        assertJobTime(job);
    }

    private void assertTimeError(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        assertJobRun(createTime, job);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());

        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            Assert.assertTrue(chainedExecutable.getTasks().stream()
                    .anyMatch(task -> ExecutableState.ERROR == executableManager.getOutput(task.getId()).getState()));
        }

        assertJobTime(job);
    }

    private void assertTimeSuicide(long createTime, String id) {
        assertTimeFinalState(createTime, id, ExecutableState.SUICIDAL);
        assertJobTime(executableManager.getJob(id));
    }

    private void assertTimeDiscard(long createTime, String id) {
        assertTimeFinalState(createTime, id, ExecutableState.DISCARDED);
        assertJobTime(executableManager.getJob(id));
    }

    private void assertTimeFinalState(long createTime, String id, ExecutableState state) {
        AbstractExecutable job = executableManager.getJob(id);
        Assert.assertNotNull(job);
        Assert.assertEquals(state, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertTrue(job.getStartTime() > 0L);
        Assert.assertTrue(job.getEndTime() > 0L);
        Assert.assertTrue(job.getDuration() >= 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
    }

    private void assertTimeRunning(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        Assert.assertNotNull(job);
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
        assertJobTime(job, 100);
    }

    private void assertTimeLegal(String id) {
        AbstractExecutable job = executableManager.getJob(id);
        assertTimeLegal(job);
        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            for (AbstractExecutable task : chainedExecutable.getTasks()) {
                assertTimeLegal(task);
            }
        }
    }

    private void assertTimeLegal(AbstractExecutable job) {
        Assert.assertNotNull(job);
        Assert.assertTrue(job.getCreateTime() > 0L);
        Assert.assertTrue(job.getStartTime() >= 0L);
        Assert.assertTrue(job.getEndTime() >= 0L);
        Assert.assertTrue(job.getDuration() >= 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
    }

    private void assertJobRun(long createTime, AbstractExecutable job) {
        Assert.assertNotNull(job);
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertTrue(job.getStartTime() > 0L);
        Assert.assertTrue(job.getEndTime() > 0L);
        Assert.assertTrue(job.getDuration() > 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
    }

    private void assertMemoryRestore(double currMem) {
        getConditionFactory().untilAsserted(() -> {
            double availableMem = NDefaultScheduler.currentAvailableMem();
            Assert.assertEquals(currMem, availableMem, 0.1);
        });
    }

    @Repeat(3)
    @Test
    public void testSucceedAndFailed() {
        logger.info("testSucceedAndFailed");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new FailedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task2.getId()).getState());
        getConditionFactory().untilAsserted(() -> {
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(job.getId()).getVerboseMsg())
                    .contains("org.apache.kylin.job.execution.MockJobException");
            Assertions
                    .assertThat(executableManager.getOutputFromHDFSByJobId(job.getId(), task1.getId()).getVerboseMsg())
                    .contains("succeed");
            Assertions
                    .assertThat(executableManager.getOutputFromHDFSByJobId(job.getId(), task2.getId()).getVerboseMsg())
                    .contains("org.apache.kylin.job.execution.MockJobException");
        });
        assertTimeError(createTime, job.getId());
        testJobPending(job.getId());
        assertMemoryRestore(currMem);

        executableManager.updateJobOutput(task2.getId(), ExecutableState.READY);
        executableManager.updateJobOutput(task2.getId(), ExecutableState.RUNNING);
        Mockito.doReturn(task2).when(executableManager).getJob(Mockito.anyString());
        ExecutableOutputPO outputPO = new ExecutableOutputPO();
        outputPO.setLogPath("/kylin/null.log");
        Mockito.doReturn(outputPO).when(executableManager).getJobOutputFromHDFS(Mockito.anyString());

        task2.setProject("default");
        Assert.assertEquals("Wait a moment ... ",
                executableManager.getOutputFromHDFSByJobId(task2.getId()).getVerboseMsg());
    }

    @Repeat(3)
    @Test
    public void testSucceedAndError() {
        logger.info("testSucceedAndError");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new ErrorTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        assertMemoryRestore(currMem);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task2.getId()).getState());
        //in case hdfs write is not finished yet
        getConditionFactory().untilAsserted(() -> {
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(job.getId()).getVerboseMsg())
                    .contains("test error");
            Assertions
                    .assertThat(executableManager.getOutputFromHDFSByJobId(job.getId(), task2.getId()).getVerboseMsg())
                    .contains("test error");
        });
        testJobPending(job.getId());
        assertTimeError(createTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testDiscard() {
        logger.info("testDiscard");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        SucceedTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        SucceedTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        // give time to launch job/task1
        getConditionFactory().until(() -> job.getStatus() == ExecutableState.RUNNING);
        discardJobWithLock(job.getId());
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(job.getId()).getState());
        getConditionFactory().until(() -> {
            DefaultChainedExecutable job1 = (DefaultChainedExecutable) getManager().getJob(job.getId());
            return job1.getTasks().get(0).getStatus().isFinalState();
        });
        assertTimeDiscard(createTime, job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(1, killProcessCount.get());
    }

    @Test
    public void testDiscardJobBeforeSchedule() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            modelManager.dropModel(model);
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());

        executableManager.addJob(job);
        getConditionFactory().untilAsserted(() -> {
            AbstractExecutable job1 = executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.SUICIDAL, job1.getStatus());
        });
        assertTimeSuicide(job.getCreateTime(), job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testDiscardErrorJobBeforeSchedule() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new ErrorTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        getConditionFactory().untilAsserted(() -> {
            AbstractExecutable job1 = executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.ERROR, job1.getStatus());
        });
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            modelManager.dropModel(model);
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());

        getConditionFactory().untilAsserted(() -> {
            AbstractExecutable job1 = executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.SUICIDAL, job1.getStatus());
        });
        assertTimeSuicide(job.getCreateTime(), job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testDiscardPausedJobBeforeSchedule() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        val task2 = new FiveSecondSucceedTestExecutable();
        task2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        getConditionFactory().untilAsserted(() -> {
            final AbstractExecutable job1 = executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.RUNNING, job1.getStatus());
        });
        pauseJobWithLock(job.getId());
        getConditionFactory().untilAsserted(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.PAUSED, job1.getTasks().get(0).getStatus());
        });
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            modelManager.dropModel(model);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());

        getConditionFactory().untilAsserted(() -> {
            final AbstractExecutable job1 = executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.SUICIDAL, job1.getStatus());
        });
        assertTimeSuicide(job.getCreateTime(), job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    private void testJobStopped(String jobId) {
        long[] durations = getAllDurations(jobId);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long[] durations2 = getAllDurations(jobId);
        Assert.assertArrayEquals(durations, durations2);
    }

    private long[] getAllDurations(String jobId) {
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        int size = 2 * (stopJob.getTasks().size() + 1);
        long[] durations = new long[size];
        durations[0] = stopJob.getDuration();
        durations[1] = stopJob.getWaitTime();
        int i = 2;
        for (AbstractExecutable task : stopJob.getTasks()) {
            durations[i] = task.getDuration();
            durations[i + 1] = task.getWaitTime();
            i += 2;
        }
        return durations;
    }

    private void testJobPending(String jobId) {
        long[] durations = getDurationByJobId(jobId);
        double[] waitTime = getWaitTimeByJobId(jobId);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long[] durations2 = getDurationByJobId(jobId);
        double[] waitTime2 = getWaitTimeByJobId(jobId);
        Assert.assertArrayEquals(durations, durations2);
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        Assert.assertArrayEquals(waitTime, waitTime2, 50);
    }

    private long[] getDurationByJobId(String jobId) {
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        int size = (stopJob.getTasks().size() + 1);
        long[] durations = new long[size];
        durations[0] = stopJob.getDuration();
        int i = 1;
        for (AbstractExecutable task : stopJob.getTasks()) {
            durations[i] = task.getDuration();
            i += 1;
        }
        return durations;
    }

    private double[] getWaitTimeByJobId(String jobId) {
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        int size = (stopJob.getTasks().size() + 1);
        double[] waitTimes = new double[size];
        waitTimes[0] = stopJob.getWaitTime();
        int i = 1;
        for (AbstractExecutable task : stopJob.getTasks()) {
            waitTimes[i] = task.getWaitTime();
            i += 1;
        }
        return waitTimes;
    }

    @Test
    public void testIllegalState() {
        logger.info("testIllegalState");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateJobOutput(task2.getId(),
                ExecutableState.RUNNING);
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSuicide_RemoveSegment() {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        //in case hdfs write is not finished yet
        getConditionFactory().untilAsserted(() -> {
            DefaultChainedExecutable job2 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            ExecutableState status = job2.getStatus();
            Assert.assertEquals(ExecutableState.SUICIDAL, status);
            Assert.assertEquals(ExecutableState.SUICIDAL, job2.getTasks().get(0).getStatus());
        });
        assertTimeSuicide(createTime, job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    private void changeSchedulerInterval() {
        changeSchedulerInterval(30);
    }

    private void changeSchedulerInterval(int second) {
        NDefaultScheduler.shutdownByProject("default");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", String.valueOf(second));
        startScheduler();
    }

    @Test
    @Ignore
    public void testSuicide_RemoveLayout() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val job = initNoErrorJob(modelId);
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        mgr.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L, 10001L), true, true);
        });

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
    }

    @Test
    public void testSuccess_RemoveSomeLayout() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val job = initNoErrorJob(modelId);
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        mgr.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), true, true);
        });

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, output.getState());
    }

    private AbstractExecutable initNoErrorJob(String modelId) {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(modelId);
        job.setName("NO_ERROR_STATUS_EXECUTABLE");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,10001");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject(modelId);
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        task.setParam(NBatchConstants.P_LAYOUT_IDS, "1,10001");
        job.addTask(task);
        executableManager.addJob(job);
        return job;
    }

    @Test
    public void testSuicide_JobCuttingIn() {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        executableManager.addJob(job);
        assertMemoryRestore(currMem);

        getConditionFactory().until(() -> job.getStatus() == ExecutableState.RUNNING);
        NoErrorStatusExecutableOnModel job2 = new NoErrorStatusExecutableOnModel();
        job2.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job2.setJobType(JobTypeEnum.INC_BUILD);
        job2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task2 = new SucceedTestExecutable();
        task2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));

        job2.addTask(task2);
        executableManager.addJob(job2);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        //in case hdfs write is not finished yet
        getConditionFactory().untilAsserted(() -> {
            final AbstractExecutable job1 = executableManager.getJob(job.getId());
            Assert.assertEquals(ExecutableState.SUCCEED, job1.getStatus());
        });

    }

    @Test
    public void testJobDiscard_AfterSuccess() throws InterruptedException {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        getConditionFactory().until(() -> job.getStatus() == ExecutableState.RUNNING);
        discardJobWithLock(job.getId());

        assertMemoryRestore(currMem);
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, output.getState());
        Assert.assertEquals(ExecutableState.DISCARDED, job.getStatus());

    }

    @Test
    public void testIncBuildJobError_ModelBasedDataFlowOnline() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.MODEL_BASED, JobTypeEnum.INC_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    @Test
    public void testIncBuildJobError_TableOrientedDataFlowLagBehind() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.TABLE_ORIENTED, JobTypeEnum.INC_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, updateDf.getStatus());
    }

    @Test
    public void testIndexBuildJobError_TableOrientedDataFlowOnline() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.TABLE_ORIENTED, JobTypeEnum.INDEX_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    @Test
    public void testIndexBuildJobError_ModelBasedDataFlowOnline() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.MODEL_BASED, JobTypeEnum.INDEX_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    private DefaultChainedExecutable testDataflowStatusWhenJobError(ManagementType tableOriented,
            JobTypeEnum indexBuild) {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), project);
        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(tableOriented);
        });
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setName(indexBuild.toString());
        job.setJobType(indexBuild);
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new ErrorTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        executableManager.addJob(job);
        return job;
    }

    @Repeat(3)
    @Test
    public void testCheckJobStopped_TaskSucceed() throws JobStoppedException {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val df = dfMgr.getDataflow(modelId);
        val targetSegs = df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSegments(targetSegs);
        job.setTargetSubject(modelId);
        val task = new SucceedTestExecutable();
        task.setProject("default");
        task.setTargetSubject(modelId);
        task.setTargetSegments(targetSegs);
        job.addTask(task);

        executableManager.addJob(job);
        getConditionFactory().until(() -> {
            val executeManager = NExecutableManager.getInstance(getTestConfig(), project);
            String runningStatus = executeManager.getOutput(task.getId()).getExtra().get("runningStatus");
            return job.getStatus() == ExecutableState.RUNNING && StringUtils.isNotEmpty(runningStatus)
                    && runningStatus.equals("inRunning");
        });
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        pauseJobWithLock(job.getId());

        getConditionFactory().untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            Assert.assertEquals(ExecutableState.PAUSED, task.getStatus());
        });

        thrown.expect(JobStoppedNonVoluntarilyException.class);
        task.abortIfJobStopped(true);
        assertMemoryRestore(currMem);
    }

    @Test
    public void testCheckJobStopped_TaskError() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val df = dfMgr.getDataflow(modelId);
        val targetSegs = df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSegments(targetSegs);
        job.setTargetSubject(modelId);
        val task = new ErrorTestExecutable();
        task.setProject("default");
        task.setTargetSubject(modelId);
        task.setTargetSegments(targetSegs);
        job.addTask(task);

        executableManager.addJob(job);
        await().pollInterval(50, TimeUnit.MILLISECONDS).atMost(60000, TimeUnit.MILLISECONDS).until(() -> {
            val executeManager = NExecutableManager.getInstance(getTestConfig(), project);
            String runningStatus = executeManager.getOutput(task.getId()).getExtra().get("runningStatus");
            return job.getStatus() == ExecutableState.RUNNING && StringUtils.isNotEmpty(runningStatus)
                    && runningStatus.equals("inRunning");
        });
        pauseJobWithLock(job.getId());

        getConditionFactory().untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            Assert.assertEquals(ExecutableState.PAUSED, task.getStatus());
        });
        assertMemoryRestore(currMem);
        Assert.assertEquals(1, killProcessCount.get());
    }

    @Test
    public void testSchedulerStop() {
        logger.info("testSchedulerStop");

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        // make sure the job is running
        getConditionFactory().until(() -> job.getStatus() == ExecutableState.RUNNING);
        //scheduler failed due to some reason
        scheduler.shutdown();
        Assert.assertFalse(scheduler.hasStarted());

        AbstractExecutable job1 = executableManager.getJob(job.getId());
        ExecutableState status = job1.getStatus();
        Assert.assertEquals(ExecutableState.SUCCEED, status);
    }

    @Test
    public void testSchedulerStopCase2() {
        logger.info("testSchedulerStop case 2");

        thrown.expect(ConditionTimeoutException.class);

        // testSchedulerStopCase2 shutdown first, then the job added will not be scheduled
        scheduler.shutdown();

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        waitForJobFinish(job.getId());
    }

    @Repeat(3)
    @Test
    public void testSchedulerRestart() {
        logger.info("testSchedulerRestart");

        var currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new FiveSecondSucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 2s to make sure SucceedTestExecutable is running
        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(0).getStatus() == ExecutableState.RUNNING;
        });

        assertMemoryRestore(currMem - job.computeStepDriverMemory());

        //scheduler failed due to some reason
        NDefaultScheduler.shutdownByProject("default");
        //make sure the first scheduler has already stopped
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Predicate<ExecutablePO> updater = (po) -> {
            po.getOutput().setStatus(ExecutableState.RUNNING.toString());
            po.getOutput().setEndTime(0);
            po.getTasks().get(0).getOutput().setStatus(ExecutableState.RUNNING.toString());
            po.getTasks().get(0).getOutput().setEndTime(0);
            po.getTasks().get(1).getOutput().setStatus(ExecutableState.READY.toString());
            po.getTasks().get(1).getOutput().setStartTime(0);
            po.getTasks().get(1).getOutput().setWaitTime(0);
            po.getTasks().get(1).getOutput().setEndTime(0);
            return true;
        };
        executableDao.updateJob(stopJob.getId(), updater);
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, executableManager.getOutput(task2.getId()).getState());
        //restart
        startScheduler();
        currMem = NDefaultScheduler.currentAvailableMem();
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(1).getStatus() == ExecutableState.RUNNING;
        });
        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
        assertTimeSucceed(createTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Repeat(3)
    @Test
    public void testJobPauseAndResume() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(0).getStatus() == ExecutableState.RUNNING;
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //pause job due to some reason
        pauseJobWithLock(job.getId());
        //sleep 7s to make sure DefaultChainedExecutable is paused
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        val context1 = new ExecutableDurationContext(project, job.getId());
        assertPausedState(context1, 3000);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertPausedPending(context1, context2, 1000);

        assertMemoryRestore(currMem);

        //resume
        resumeJobWithLock(job.getId());
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(1).getStatus() == ExecutableState.RUNNING;
        });
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();

        Assert.assertTrue(context2.getRecord().getDuration() < totalDuration3);
        val stepType = context1.getStepRecords().get(0).getState();
        if (stepType == ExecutableState.READY) {
            Assert.assertEquals(context2.getStepRecords().get(0).getDuration() + 5000, task1Duration3, 1000);
        } else if (stepType == ExecutableState.SUCCEED) {
            Assert.assertEquals(context2.getStepRecords().get(0).getDuration(), task1Duration3);
        }
        Assert.assertTrue(0 < task2Duration3);
        Assert.assertTrue(context2.getRecord().getWaitTime() <= totalPendingDuration3);
        Assert.assertTrue(context2.getStepRecords().get(0).getWaitTime() <= task1PendingDuration3);

        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        assertTimeSucceed(createTime, job.getId());
        Assert.assertEquals(1, killProcessCount.get());
    }

    @Test
    public void testJobRestart() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(0).getStatus() == ExecutableState.RUNNING;
        });

        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.RUNNING, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.RUNNING, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        long totalDuration = stopJob.getDuration();
        long task1Duration = stopJob.getTasks().get(0).getDuration();
        long task2Duration = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration = stopJob.getWaitTime();
        Assert.assertTrue(totalDuration > 0);
        Assert.assertTrue(task1Duration > 0);
        Assert.assertEquals(0, task2Duration);
        Assert.assertTrue(totalPendingDuration >= 0);

        //restart
        restartJobWithLock(job.getId());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        Assert.assertTrue(newCreateTime > createTime);
        assertTimeLegal(job.getId());

        getConditionFactory().until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.RUNNING);
        assertTimeRunning(newCreateTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeSucceed(newCreateTime, job.getId());
        assertMemoryRestore(currMem);
        Assert.assertFalse(stopJob.isResumable());
    }

    @Test
    public void testJobPauseAndRestart() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new FiveSecondSucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(0).getStatus() == ExecutableState.RUNNING;
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //pause job due to some reason
        pauseJobWithLock(job.getId());
        //sleep 7s to make sure DefaultChainedExecutable is paused
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context1 = new ExecutableDurationContext(project, job.getId());
        assertPausedState(context1, 3000);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertPausedPending(context1, context2, 1000);
        assertMemoryRestore(currMem);

        restartJobWithLock(job.getId());
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        Assert.assertTrue(newCreateTime > createTime);
        assertTimeLegal(job.getId());

        AtomicBoolean ended = new AtomicBoolean(false);
        getConditionFactory().until(() -> {
            if (executableManager.getJob(job.getId()).getStatus() == ExecutableState.SUCCEED) {
                ended.set(true);
                return true;
            }
            return executableManager.getJob(job.getId()).getStatus() == ExecutableState.RUNNING;
        });
        if (!ended.get()) {
            assertTimeRunning(newCreateTime, job.getId());
        }
        waitForJobFinish(job.getId());
        assertTimeSucceed(newCreateTime, job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(1, killProcessCount.get());
    }

    @Test
    @Repeat(3)
    public void testConcurrentJobLimit() {
        String project = "heterogeneous_segment";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val scheduler = NDefaultScheduler.getInstance(project);
        val originExecutableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val executableManager = Mockito.spy(originExecutableManager);
        executableManager.deleteAllJob();
        Mockito.doAnswer(invocation -> {
            String jobId = invocation.getArgument(0);
            originExecutableManager.destroyProcess(jobId);
            return null;
        }).when(executableManager).destroyProcess(Mockito.anyString());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.job.max-concurrent-jobs", "1");
        scheduler.init(new JobEngineConfig(config));

        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        int memory = NDefaultScheduler.getMemoryRemaining().availablePermits();
        val df = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        DefaultChainedExecutable job1 = generateJob(df, project);
        DefaultChainedExecutable job2 = generatePartial(df, project);
        executableManager.addJob(job1);
        executableManager.addJob(job2);
        waitForJobByStatus(job1.getId(), 60000, ExecutableState.RUNNING, executableManager);
        config.setProperty("kylin.job.max-concurrent-jobs", "0");
        Assert.assertNotEquals(memory, NDefaultScheduler.getMemoryRemaining().availablePermits());
        val runningExecutables = executableManager.getRunningExecutables(project, modelId);
        runningExecutables.sort(Comparator.comparing(AbstractExecutable::getCreateTime));
        Assert.assertEquals(ExecutableState.RUNNING, runningExecutables.get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, runningExecutables.get(1).getStatus());

        config.setProperty("kylin.job.max-concurrent-jobs", "1");
        waitForJobByStatus(job1.getId(), 60000, null, executableManager);
        waitForJobByStatus(job2.getId(), 60000, null, executableManager);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job2.getId()).getState());

        scheduler.shutdown();
        Assert.assertEquals(memory, NDefaultScheduler.getMemoryRemaining().availablePermits());
    }

    @Test
    @Repeat(3)
    public void testConcurrentJobWithPriority() {
        String project = "heterogeneous_segment";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        val scheduler = NDefaultScheduler.getInstance(project);
        val originExecutableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val executableManager = Mockito.spy(originExecutableManager);
        Mockito.doAnswer(invocation -> {
            String jobId = invocation.getArgument(0);
            originExecutableManager.destroyProcess(jobId);
            return null;
        }).when(executableManager).destroyProcess(Mockito.anyString());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.job.max-concurrent-jobs", "1");
        val df = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        DefaultChainedExecutable job0 = generateJob(df, project, 4);
        DefaultChainedExecutable job1 = generatePartial(df, project, 3);
        DefaultChainedExecutable job2 = generatePartial(df, project, 2);
        DefaultChainedExecutable job3 = generateJob(df, project, 1);
        DefaultChainedExecutable job4 = generatePartial(df, project, 0);
        DefaultChainedExecutable job5 = generateJob(df, project, 3);
        executableManager.addJob(job0);
        executableManager.addJob(job1);
        executableManager.addJob(job2);
        executableManager.addJob(job3);
        executableManager.addJob(job4);
        executableManager.addJob(job5);
        // start schedule
        scheduler.init(new JobEngineConfig(config));

        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        waitForJobByStatus(job4.getId(), 60000, ExecutableState.SUCCEED, executableManager);
        var runningExecutables = executableManager.getRunningExecutables(project, modelId);
        Assert.assertEquals(5, runningExecutables.size());
        waitForJobByStatus(job2.getId(), 60000, ExecutableState.SUCCEED, executableManager);
        DefaultChainedExecutable job6 = generateJob(df, project, 0);
        executableManager.addJob(job6);
        runningExecutables = executableManager.getRunningExecutables(project, modelId);
        Assert.assertEquals(4, runningExecutables.size());
        waitForJobByStatus(job6.getId(), 60000, ExecutableState.SUCCEED, executableManager);
        runningExecutables = executableManager.getRunningExecutables(project, modelId);
        Assert.assertEquals(3, runningExecutables.size());
        waitForJobByStatus(job0.getId(), 60000, ExecutableState.SUCCEED, executableManager);
        runningExecutables = executableManager.getRunningExecutables(project, modelId);
        Assert.assertEquals(0, runningExecutables.size());
        scheduler.shutdown();
    }

    private DefaultChainedExecutable generateJob(NDataflow df, String project, int priority) {
        DefaultChainedExecutable job = generateJob(df, project);
        job.setPriority(priority);
        return job;
    }

    private DefaultChainedExecutable generateJob(NDataflow df, String project) {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject(project);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        return job;
    }

    private DefaultChainedExecutable generatePartial(NDataflow df, String project, int priority) {
        DefaultChainedExecutable job = generatePartial(df, project);
        job.setPriority(priority);
        return job;
    }

    private DefaultChainedExecutable generatePartial(NDataflow df, String project) {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject(project);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        val targetSubject = df.getModel().getUuid();
        job.setId(job.getId() + "-" + targetSubject);
        job.setTargetSubject(targetSubject);
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        return job;
    }

    private void assertPausedState(ExecutableDurationContext context, long interval) {
        Assert.assertEquals(ExecutableState.PAUSED, context.getRecord().getState());
        val stepType = context.getStepRecords().get(0).getState();
        Assert.assertTrue(stepType == ExecutableState.SUCCEED || stepType == ExecutableState.PAUSED);
        Assert.assertEquals(ExecutableState.READY, context.getStepRecords().get(1).getState());
        Assert.assertTrue(context.getRecord().getDuration() > 0);
        Assert.assertTrue(context.getStepRecords().get(0).getDuration() > 0);
        Assert.assertEquals(0, context.getStepRecords().get(1).getDuration());
        Assert.assertTrue(context.getRecord().getWaitTime() >= 0);
        if (stepType == ExecutableState.SUCCEED) {
            Assert.assertEquals(0, context.getStepRecords().get(0).getWaitTime());
        }
        Assert.assertEquals(0, context.getStepRecords().get(1).getWaitTime());
    }

    private void assertPausedPending(ExecutableDurationContext context1, ExecutableDurationContext context2,
            long interval) {
        assertContextStateEquals(context1, context2);
        val stepType = context2.getStepRecords().get(0).getState();
        Assert.assertEquals(context1.getRecord().getDuration(), context2.getRecord().getDuration());
        Assert.assertEquals(context1.getStepRecords().get(0).getDuration(),
                context2.getStepRecords().get(0).getDuration());
        Assert.assertEquals(0, context2.getStepRecords().get(1).getDuration());
        Assert.assertEquals(context1.getRecord().getWaitTime(), context2.getRecord().getWaitTime(), 100);
        if (stepType == ExecutableState.READY) {
            Assert.assertEquals(context1.getStepRecords().get(0).getWaitTime() + interval,
                    context2.getStepRecords().get(0).getWaitTime(), 100);
        } else if (stepType == ExecutableState.SUCCEED) {
            Assert.assertEquals(0, context2.getStepRecords().get(0).getWaitTime());
        }
        Assert.assertEquals(0, context2.getStepRecords().get(1).getWaitTime());
    }

    private void assertContextStateEquals(ExecutableDurationContext context1, ExecutableDurationContext context2) {
        Assert.assertEquals(context1.getRecord().getState(), context2.getRecord().getState());
        Assert.assertEquals(context1.getStepRecords().size(), context2.getStepRecords().size());
        for (int i = 0; i < context1.getStepRecords().size(); i++) {
            Assert.assertEquals(context1.getStepRecords().get(i).getState(),
                    context2.getStepRecords().get(i).getState());
        }
    }

    private void assertErrorState(ExecutableDurationContext context) {
        Assert.assertEquals(ExecutableState.ERROR, context.getRecord().getState());
        Assert.assertEquals(ExecutableState.SUCCEED, context.getStepRecords().get(0).getState());
        Assert.assertEquals(ExecutableState.ERROR, context.getStepRecords().get(1).getState());
        Assert.assertTrue(context.getRecord().getDuration() > 0);
        Assert.assertTrue(context.getStepRecords().get(0).getDuration() > 0);
        Assert.assertTrue(context.getStepRecords().get(1).getDuration() > 0);
        Assert.assertTrue(context.getRecord().getWaitTime() >= 0);
    }

    private void assertErrorPending(ExecutableDurationContext context1, ExecutableDurationContext context2) {
        assertContextStateEquals(context1, context2);
        Assert.assertEquals(context1.getRecord().getDuration(), context2.getRecord().getDuration());
        Assert.assertEquals(context1.getStepRecords().get(0).getDuration(),
                context2.getStepRecords().get(0).getDuration());
        Assert.assertTrue(context2.getStepRecords().get(1).getDuration() > 0);
        Assert.assertEquals(context1.getRecord().getWaitTime(), context2.getRecord().getWaitTime(), 100);
        Assert.assertEquals(context1.getStepRecords().get(0).getWaitTime(),
                context2.getStepRecords().get(0).getWaitTime(), 100);
        Assert.assertEquals(context2.getStepRecords().get(1).getWaitTime(),
                context2.getStepRecords().get(1).getWaitTime());

    }

    private void restartJobWithLock(String id) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager().restartJob(id);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, id);
    }

    private void resumeJobWithLock(String id) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager().resumeJob(id);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, id);
    }

    private void pauseJobWithLock(String id) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager().pauseJob(id);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, id);
    }

    private void discardJobWithLock(String id) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager().discardJob(id);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, id);
    }

    private NExecutableManager getManager() {
        val originExecutableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val executableManager = Mockito.spy(originExecutableManager);
        Mockito.doAnswer(invocation -> {
            String jobId = invocation.getArgument(0);
            originExecutableManager.destroyProcess(jobId);
            killProcessCount.incrementAndGet();
            return null;
        }).when(executableManager).destroyProcess(Mockito.anyString());
        return executableManager;
    }

    @Test
    @Repeat(3)
    public void testJobErrorAndResume() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new ErrorTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        assertMemoryRestore(currMem - job.computeStepDriverMemory());

        //sleep 3s to make sure SucceedTestExecutable is running
        getConditionFactory().until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.ERROR);

        val context1 = new ExecutableDurationContext(project, job.getId());
        assertErrorState(context1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertErrorPending(context1, context2);
        assertMemoryRestore(currMem);

        //resume
        resumeJobWithLock(job.getId());
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        getConditionFactory().until(() -> {
            final DefaultChainedExecutable job1 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            return job1.getTasks().get(1).getStatus() == ExecutableState.RUNNING;
        });
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration3 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(context2.getRecord().getDuration() < totalDuration3);
        Assert.assertEquals(context2.getStepRecords().get(0).getWaitTime(), task1PendingDuration3);
        Assert.assertTrue(context2.getStepRecords().get(1).getDuration() < task2Duration3);
        Assert.assertTrue(context2.getRecord().getWaitTime() <= totalPendingDuration3);

        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeError(createTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Repeat(3)
    @Test
    public void testJobErrorAndRestart() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new FiveSecondErrorTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 8s to make sure SucceedTestExecutable is running
        getConditionFactory().until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.ERROR);

        val context1 = new ExecutableDurationContext(project, job.getId());
        assertErrorState(context1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertErrorPending(context1, context2);

        assertMemoryRestore(currMem);

        //restart
        restartJobWithLock(job.getId());
        getConditionFactory() //
                .until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.READY
                        && executableManager.getJob(job.getId()).getCreateTime() > createTime);
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        assertTimeLegal(job.getId());
        Assert.assertEquals(ExecutableState.READY, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());

        AtomicBoolean ended = new AtomicBoolean(false);
        getConditionFactory().until(() -> {
            if (executableManager.getJob(job.getId()).getStatus() == ExecutableState.ERROR) {
                ended.set(true);
                return true;
            }
            return executableManager.getJob(job.getId()).getStatus() == ExecutableState.RUNNING;
        });
        if (!ended.get()) {
            assertTimeRunning(newCreateTime, job.getId());
        }
        waitForJobFinish(job.getId());
        assertTimeError(newCreateTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    @Repeat
    public void testRetryableException() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task = new ErrorTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        overwriteSystemProp("kylin.job.retry", "3");

        //don't retry on DefaultChainedExecutable, only retry on subtasks
        Assert.assertFalse(job.needRetry(1, new Exception("")));
        Assert.assertTrue(task.needRetry(1, new Exception("")));
        Assert.assertFalse(task.needRetry(1, null));
        Assert.assertFalse(task.needRetry(4, new Exception("")));

        overwriteSystemProp("kylin.job.retry-exception-classes", "java.io.FileNotFoundException");

        Assert.assertTrue(task.needRetry(1, new FileNotFoundException()));
        Assert.assertFalse(task.needRetry(1, new Exception("")));
    }

    @Test
    public void testJobRunningTimeout() {
        overwriteSystemProp("kylin.scheduler.schedule-job-timeout-minute", "1");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task = new LongRunningTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        waitForJobFinish(job.getId(), 200000);

        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
    }

    @Test
    public void testSubmitParallelTasksSucceed() {
        logger.info("testSubmitParallelTasksSuccessed");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSubmitParallelTasksError() throws InterruptedException {
        logger.info("testSubmitParallelTasksError");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondErrorTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        assertMemoryRestore(currMem - job.computeStepDriverMemory());
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSubmitParallelTasksReachMemoryQuota() throws Exception {
        logger.info("testSubmitParallelTasksByMemoryQuota");
        val manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), project));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = getInstanceByProject();
        managersByPrjCache.get(NExecutableManager.class).put(project, manager);
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfs = Lists.newArrayList(NDataflowManager.getInstance(getTestConfig(), project).listAllDataflows());

        val baseMem = Math.max(Math.round(currMem / dfs.size()), 1024) * 2;
        getTestConfig().setProperty("kylin.engine.driver-memory-base", Long.valueOf(baseMem).toString());
        getTestConfig().setProperty("kylin.engine.driver-memory-maximum", "102400");
        addParallelTasksForJob(dfs, executableManager);

        getConditionFactory().until(() -> (NDefaultScheduler.currentAvailableMem() <= baseMem));
        assertMemoryRestore(currMem);
    }

    @Test
    public void testMarkJobError_AfterUpdateJobStateFailed() {
        changeSchedulerInterval(1);

        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondErrorTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        executableManager.updateJobOutput(job.getId(), ExecutableState.RUNNING);

        getConditionFactory().untilAsserted(() -> {
            DefaultChainedExecutable job2 = (DefaultChainedExecutable) executableManager.getJob(job.getId());
            ExecutableState status = job2.getStatus();
            Assert.assertEquals(ExecutableState.ERROR, status);
            Assert.assertEquals(ExecutableState.ERROR, job2.getTasks().get(0).getStatus());
        });
    }

    private void addParallelTasksForJob(List<NDataflow> dfs, NExecutableManager executableManager) {
        for (NDataflow df : dfs) {
            DefaultChainedExecutable job = new NoErrorStatusExecutableOnModel();
            job.setProject("default");
            job.setJobType(JobTypeEnum.INDEX_BUILD);
            job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2");
            job.setTargetSubject(df.getModel().getUuid());
            job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
            BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable(10);
            task1.setTargetSubject(df.getModel().getUuid());
            task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
            job.addTask(task1);
            executableManager.addJob(job);
        }
    }

    @Test
    public void testSchedulerShutdown() throws Exception {
        overwriteSystemProp("kylin.env", "dev");
        NDefaultScheduler instance = NDefaultScheduler.getInstance(project);
        Assert.assertTrue(instance.hasStarted());
        Thread.sleep(2000);
        EpochManager manager = EpochManager.getInstance();
        manager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        manager.updateEpochWithNotifier(project, true);
        instance.fetchJobsImmediately();
        getConditionFactory().untilAsserted(() -> Assert.assertFalse(instance.hasStarted()));
    }

    @Test
    public void testStorageQuotaLimitReached() {
        try {
            // case READY
            {
                scheduler.getContext().setReachQuotaLimit(true);
                overwriteSystemProp("kylin.storage.quota-in-giga-bytes", "0");
                DefaultChainedExecutable job = new DefaultChainedExecutable();
                job.setProject(project);
                AbstractExecutable task1 = new SucceedTestExecutable();
                task1.setProject(project);
                job.addTask(task1);
                executableManager.addJob(job);
                waitForJobFinish(job.getId());
                Assert.assertEquals(ExecutableState.PAUSED, executableManager.getJob(job.getId()).getStatus());
            }

            // case RUNNING
            {
                scheduler.getContext().setReachQuotaLimit(true);
                overwriteSystemProp("kylin.storage.quota-in-giga-bytes", Integer.toString(Integer.MAX_VALUE));
                DefaultChainedExecutable job = new DefaultChainedExecutable();
                job.setProject(project);
                AbstractExecutable task1 = new LongRunningTestExecutable();
                task1.setProject(project);
                job.addTask(task1);
                executableManager.addJob(job);
                waitForJobByStatus(job.getId(), 60000, ExecutableState.RUNNING, executableManager);
                overwriteSystemProp("kylin.storage.quota-in-giga-bytes", "0");
                waitForJobFinish(job.getId());
                Assert.assertEquals(ExecutableState.PAUSED, executableManager.getJob(job.getId()).getStatus());
            }
        } finally {
            overwriteSystemProp("kylin.storage.quota-in-giga-bytes", Integer.toString(Integer.MAX_VALUE));
            scheduler.getContext().setReachQuotaLimit(false);
        }
    }

    @Test
    public void testDiscardPendingJobDuration() {
        try {
            logger.info("testDiscardPendingJobDuration");
            // add a long running job with max concurrent job = 1 to avoid further jobs added to be running
            overwriteSystemProp("kylin.job.max-concurrent-jobs", "1");
            overwriteSystemProp("kylin.storage.quota-in-giga-bytes", Integer.toString(Integer.MAX_VALUE));
            DefaultChainedExecutable job = new DefaultChainedExecutable();
            job.setProject(project);
            AbstractExecutable task = new LongRunningTestExecutable();
            task.setProject(project);
            job.addTask(task);
            executableManager.addJob(job);
            waitForJobByStatus(job.getId(), 60000, ExecutableState.RUNNING, executableManager);

            // add a pending job
            DefaultChainedExecutable job1 = new DefaultChainedExecutable();
            job1.setProject(project);
            AbstractExecutable task1 = new LongRunningTestExecutable();
            task1.setProject(project);
            job1.addTask(task1);
            executableManager.addJob(job1);
            executableManager.discardJob(job1.getId());
            long sleepStart = System.currentTimeMillis();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // wait time shoud be smaller than the sleep time as job is discard earlier
            Assert.assertTrue(job1.getWaitTime() < System.currentTimeMillis() - sleepStart);
        } finally {
            overwriteSystemProp("kylin.storage.quota-in-giga-bytes", Integer.toString(Integer.MAX_VALUE));
            scheduler.getContext().setReachQuotaLimit(false);
        }
    }
}
