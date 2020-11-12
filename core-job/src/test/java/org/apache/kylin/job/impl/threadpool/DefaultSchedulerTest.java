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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.BaseTestExecutable;
import org.apache.kylin.job.ErrorTestExecutable;
import org.apache.kylin.job.FailedTestExecutable;
import org.apache.kylin.job.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.RunningTestExecutable;
import org.apache.kylin.job.SelfStopExecutable;
import org.apache.kylin.job.SucceedTestExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DefaultSchedulerTest extends BaseSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulerTest.class);
    private static final int MAX_WAIT_TIME = 20000;

    @Override
    public void after() throws Exception {
        super.after();
        System.clearProperty("kylin.job.retry");
        System.clearProperty("kylin.job.retry-exception-classes");
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSingleTaskJob() throws Exception {
        logger.info("testSingleTaskJob");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        job.addTask(task1);
        execMgr.addJob(job);
        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSucceed() throws Exception {
        logger.info("testSucceed");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        execMgr.addJob(job);
        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndFailed() throws Exception {
        logger.info("testSucceedAndFailed");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new FailedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        execMgr.addJob(job);
        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndError() throws Exception {
        logger.info("testSucceedAndError");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new ErrorTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        execMgr.addJob(job);
        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, execMgr.getOutput(task2.getId()).getState());
    }

    @Test
    public void testDiscard() throws Exception {
        logger.info("testDiscard");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        SelfStopExecutable task1 = new SelfStopExecutable();
        job.addTask(task1);
        execMgr.addJob(job);
        Thread.sleep(1100); // give time to launch job/task1 
        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);
        execMgr.discardJob(job.getId());
        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.DISCARDED, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.DISCARDED, execMgr.getOutput(task1.getId()).getState());
        task1.waitForDoWork();
    }

    @Test
    public void testIllegalState() throws Exception {
        logger.info("testIllegalState");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new RunningTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        execMgr.addJob(job);
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv()).updateJobOutput(null, task2.getId(),
                ExecutableState.RUNNING, null, null, null);
        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(task2.getId()).getState());
    }

    @SuppressWarnings("rawtypes")
    @Ignore("why test JDK feature?")
    @Test
    public void testSchedulerPool() throws InterruptedException {
        logger.info("testSchedulerPool");
        ScheduledExecutorService fetchPool = Executors.newScheduledThreadPool(1);
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        ScheduledFuture future = fetchPool.scheduleAtFixedRate(countDownLatch::countDown, 0, 1, TimeUnit.SECONDS);
        assertTrue("countDownLatch should reach zero in 15 secs", countDownLatch.await(7, TimeUnit.SECONDS));
        assertTrue("future should still running", future.cancel(true));

        final CountDownLatch countDownLatch2 = new CountDownLatch(3);
        ScheduledFuture future2 = fetchPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                countDownLatch2.countDown();
                throw new RuntimeException();
            }
        }, 0, 1, TimeUnit.SECONDS);
        assertFalse("countDownLatch2 should NOT reach zero in 15 secs", countDownLatch2.await(7, TimeUnit.SECONDS));
        assertFalse("future2 should has been stopped", future2.cancel(true));
    }

    @Test
    public void testSchedulerStop() throws Exception {
        logger.info("testSchedulerStop");

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("too long wait time");

        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        job.addTask(task1);
        execMgr.addJob(job);

        //sleep 3s to make sure SucceedTestExecutable is running 
        Thread.sleep(3000);
        //scheduler failed due to some reason
        scheduler.shutdown();

        waitForJobFinish(job.getId(), 6000);
    }

    @Test
    public void testSchedulerRestart() throws Exception {
        logger.info("testSchedulerRestart");

        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        job.addTask(task1);
        execMgr.addJob(job);

        //sleep 3s to make sure SucceedTestExecutable is running 
        Thread.sleep(3000);
        //scheduler failed due to some reason
        scheduler.shutdown();
        //restart
        startScheduler();

        waitForJobFinish(job.getId(), MAX_WAIT_TIME);
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
    }

    @Test
    public void testRetryableException() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task = new ErrorTestExecutable();
        job.addTask(task);

        System.setProperty("kylin.job.retry", "3");

        //don't retry on DefaultChainedExecutable, only retry on subtasks
        Assert.assertFalse(job.needRetry(1, new Exception("")));
        Assert.assertTrue(task.needRetry(1, new Exception("")));
        Assert.assertFalse(task.needRetry(1, null));
        Assert.assertFalse(task.needRetry(4, new Exception("")));

        System.setProperty("kylin.job.retry-exception-classes", "java.io.FileNotFoundException");

        Assert.assertTrue(task.needRetry(1, new FileNotFoundException()));
        Assert.assertFalse(task.needRetry(1, new Exception("")));
    }
}
