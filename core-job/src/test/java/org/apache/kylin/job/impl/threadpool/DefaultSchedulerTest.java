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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.job.BaseTestExecutable;
import org.apache.kylin.job.ErrorTestExecutable;
import org.apache.kylin.job.FailedTestExecutable;
import org.apache.kylin.job.SelfStopExecutable;
import org.apache.kylin.job.SucceedTestExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DefaultSchedulerTest extends BaseSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulerTest.class);

    @Test
    public void testSingleTaskJob() throws Exception {
        logger.info("testSingleTaskJob");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        job.addTask(task1);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSucceed() throws Exception {
        logger.info("testSucceed");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndFailed() throws Exception {
        logger.info("testSucceedAndFailed");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new FailedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, jobService.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, jobService.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndError() throws Exception {
        logger.info("testSucceedAndError");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new ErrorTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, jobService.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, jobService.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, jobService.getOutput(task2.getId()).getState());
    }

    @Test
    public void testDiscard() throws Exception {
        logger.info("testDiscard");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        SelfStopExecutable task1 = new SelfStopExecutable();
        job.addTask(task1);
        jobService.addJob(job);
        Thread.sleep(1100); // give time to launch job/task1 
        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);
        jobService.discardJob(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, jobService.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.DISCARDED, jobService.getOutput(task1.getId()).getState());
        task1.waitForDoWork();
    }

    @SuppressWarnings("rawtypes")
    @Ignore("why test JDK feature?")
    @Test
    public void testSchedulerPool() throws InterruptedException {
        logger.info("testSchedulerPool");
        ScheduledExecutorService fetchPool = Executors.newScheduledThreadPool(1);
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        ScheduledFuture future = fetchPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                countDownLatch.countDown();
            }
        }, 0, 1, TimeUnit.SECONDS);
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
}
