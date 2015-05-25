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

import org.apache.kylin.job.*;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 */
public class ITDefaultSchedulerTest extends BaseSchedulerTest {

    @Test
    public void testSingleTaskJob() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        job.addTask(task1);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job.getId()).getState());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSucceed() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job.getId()).getState());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task1.getId()).getState());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task2.getId()).getState());
    }
    @Test
    public void testSucceedAndFailed() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new FailedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        assertEquals(ExecutableState.ERROR, jobService.getOutput(job.getId()).getState());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(task1.getId()).getState());
        assertEquals(ExecutableState.ERROR, jobService.getOutput(task2.getId()).getState());
    }
    @Test
    public void testSucceedAndError() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new ErrorTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJobFinish(job.getId());
        assertEquals(ExecutableState.ERROR, jobService.getOutput(job.getId()).getState());
        assertEquals(ExecutableState.ERROR, jobService.getOutput(task1.getId()).getState());
        assertEquals(ExecutableState.READY, jobService.getOutput(task2.getId()).getState());
    }

    @Test
    public void testDiscard() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SelfStopExecutable();
        job.addTask(task1);
        jobService.addJob(job);
        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);
        jobService.discardJob(job.getId());
        waitForJobFinish(job.getId());
        assertEquals(ExecutableState.DISCARDED, jobService.getOutput(job.getId()).getState());
        assertEquals(ExecutableState.DISCARDED, jobService.getOutput(task1.getId()).getState());
        Thread.sleep(5000);
        System.out.println(job);
    }
}
