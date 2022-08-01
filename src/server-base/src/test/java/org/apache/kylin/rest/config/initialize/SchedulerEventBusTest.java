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

package org.apache.kylin.rest.config.initialize;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EpochStartedNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.JobFinishedNotifier;
import org.apache.kylin.common.scheduler.JobReadyNotifier;
import org.apache.kylin.common.scheduler.ProjectControlledNotifier;
import org.apache.kylin.common.scheduler.ProjectEscapedNotifier;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.service.task.RecommendationTopNUpdateScheduler;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class SchedulerEventBusTest extends NLocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerEventBusTest.class);

    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    private final AtomicInteger readyCalledCount = new AtomicInteger(0);
    private final AtomicInteger jobFinishedCalledCount = new AtomicInteger(0);

    @Before
    public void setup() {
        logger.info("SchedulerEventBusTest setup");
        createTestMetadata();

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        // init DefaultScheduler
        overwriteSystemProp("kylin.job.max-local-consumption-ratio", "10");
        NDefaultScheduler.getInstance(PROJECT_NEWTEN).init(new JobEngineConfig(getTestConfig()));
        NDefaultScheduler.getInstance(PROJECT).init(new JobEngineConfig(getTestConfig()));

        JobSchedulerListener originJobSchedulerListener = new JobSchedulerListener();
        JobSchedulerListener jobSchedulerListener = Mockito.spy(originJobSchedulerListener);
        JobSyncListener originJobSyncListener = new JobSyncListener();
        JobSyncListener jobSyncListener = Mockito.spy(originJobSyncListener);

        Mockito.doAnswer(invocation -> {
            JobReadyNotifier notifier = invocation.getArgument(0);
            originJobSchedulerListener.onJobIsReady(notifier);
            readyCalledCount.incrementAndGet();
            return null;
        }).when(jobSchedulerListener).onJobIsReady(Mockito.any());

        Mockito.doAnswer(invocation -> {
            JobFinishedNotifier notifier = invocation.getArgument(0);
            originJobSyncListener.onJobFinished(notifier);
            jobFinishedCalledCount.incrementAndGet();
            return null;
        }).when(jobSyncListener).onJobFinished(Mockito.any());

        EventBusFactory.getInstance().register(originJobSchedulerListener, false);
        EventBusFactory.getInstance().register(originJobSyncListener, false);
    }

    @After
    public void cleanup() {
        logger.info("SchedulerEventBusTest cleanup");
        EventBusFactory.getInstance().restart();

        readyCalledCount.set(0);
        jobFinishedCalledCount.set(0);

        cleanupTestMetadata();
    }

    @Ignore
    @Test
    public void testJobSchedulerListener() throws InterruptedException {
        logger.info("SchedulerEventBusTest testJobSchedulerListener");

        overwriteSystemProp("kylin.scheduler.schedule-limit-per-minute", "6000");
        Assert.assertEquals(0, readyCalledCount.get());
        Assert.assertEquals(0, jobFinishedCalledCount.get());

        val df = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(PROJECT);
        job.setTargetSubject(df.getModel().getUuid());
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));

        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        executableManager.addJob(job);

        Thread.sleep(100);

        // job created message got dispatched
        Assert.assertEquals(1, readyCalledCount.get());
        Assert.assertEquals(0, jobFinishedCalledCount.get());

        // wait for job finished
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
            Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task.getId()).getState());
            Assert.assertEquals(1, jobFinishedCalledCount.get());
        });
    }

    @Ignore
    @Test
    public void testResumeJob() {
        logger.info("SchedulerEventBusTest testResumeJob");

        overwriteSystemProp("kylin.scheduler.schedule-limit-per-minute", "6000");
        val df = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(PROJECT);
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        executableManager.addJob(job);

        readyCalledCount.set(0);

        executableManager.updateJobOutput(job.getId(), ExecutableState.PAUSED);

        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(job.getId()), PROJECT, "RESUME", Lists.newArrayList());
            return null;
        }, PROJECT);

        await().atMost(60000, TimeUnit.MILLISECONDS).until(() -> 1 == readyCalledCount.get());
    }

    @Ignore
    @Test
    public void testRestartJob() {
        logger.info("SchedulerEventBusTest testRestartJob");

        overwriteSystemProp("kylin.scheduler.schedule-limit-per-minute", "6000");
        val df = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(PROJECT);
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        executableManager.addJob(job);

        readyCalledCount.set(0);

        executableManager.updateJobOutput(job.getId(), ExecutableState.ERROR);

        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(job.getId()), PROJECT, "RESTART", Lists.newArrayList());
            return null;
        }, PROJECT);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assert.assertEquals(1, readyCalledCount.get()));
    }

    @Test
    public void testEpochChangedListener() throws Exception {
        val prj = "test_epoch";
        val listener = new EpochChangedListener();
        RecommendationTopNUpdateScheduler scheduler = new RecommendationTopNUpdateScheduler();
        ReflectionTestUtils.setField(listener, "recommendationUpdateScheduler", scheduler);
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        prjMgr.createProject("test_epoch", "ADMIN", "", null);
        int oriCount = NDefaultScheduler.listAllSchedulers().size();
        listener.onProjectControlled(new ProjectControlledNotifier(prj));
        Assert.assertEquals(NDefaultScheduler.listAllSchedulers().size(), oriCount + 1);
        listener.onProjectEscaped(new ProjectEscapedNotifier(prj));
        Assert.assertEquals(NDefaultScheduler.listAllSchedulers().size(), oriCount);
        scheduler.close();
    }

    @Test
    public void testEpochChangedListenerOfGlobalPrj() throws Exception {
        var prj = "_global";
        val listener = new EpochChangedListener();
        ReflectionTestUtils.setField(listener, "userService", Mockito.spy(UserService.class));
        ReflectionTestUtils.setField(listener, "env", Mockito.spy(Environment.class));

        val prjMgr = NProjectManager.getInstance(getTestConfig());
        prjMgr.createProject("test_epoch", "ADMIN", "", null);
        int oriCount = NDefaultScheduler.listAllSchedulers().size();
        listener.onEpochStarted(new EpochStartedNotifier());
        listener.onProjectControlled(new ProjectControlledNotifier(prj));
        Assert.assertEquals(NDefaultScheduler.listAllSchedulers().size(), oriCount);
        listener.onProjectEscaped(new ProjectEscapedNotifier(prj));
        Assert.assertEquals(NDefaultScheduler.listAllSchedulers().size(), oriCount);
    }
}
