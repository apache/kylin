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

package org.apache.kylin.job;

import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.junit.Assert;
import org.junit.Test;

public class ITDistributedSchedulerBaseTest extends BaseTestDistributedScheduler {
    @Test
    public void testSchedulerLock() throws Exception {
        if (!lock(jobLock, segmentId1, serverName1)) {
            throw new JobException("fail to get the lock");
        }
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setParam(SEGMENT_ID, segmentId1);
        AbstractExecutable task1 = new SucceedTestExecutable();
        task1.setParam(SEGMENT_ID, segmentId1);
        AbstractExecutable task2 = new SucceedTestExecutable();
        task2.setParam(SEGMENT_ID, segmentId1);
        AbstractExecutable task3 = new SucceedTestExecutable();
        task3.setParam(SEGMENT_ID, segmentId1);
        job.addTask(task1);
        job.addTask(task2);
        job.addTask(task3);
        execMgr.addJob(job);

        Assert.assertEquals(serverName1, getServerName(segmentId1));

        waitForJobFinish(job.getId());

        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task2.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task3.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(job.getId()).getState());

        Assert.assertEquals(null, getServerName(segmentId1));
    }

    @Test
    public void testSchedulerConsistent() throws Exception {
        if (!lock(jobLock, segmentId2, serverName1)) {
            throw new JobException("fail to get the lock");
        }
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setParam(SEGMENT_ID, segmentId2);
        ContextTestExecutable task1 = new ContextTestExecutable();
        task1.setParam(SEGMENT_ID, segmentId2);
        job.addTask(task1);
        execMgr.addJob(job);

        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(job.getId()).getState());

        if (!lock(jobLock, segmentId2, serverName2)) {
            throw new JobException("fail to get the lock");
        }

        DefaultChainedExecutable job2 = new DefaultChainedExecutable();
        job2.setParam(SEGMENT_ID, segmentId2);
        ContextTestExecutable task2 = new ContextTestExecutable();
        task2.setParam(SEGMENT_ID, segmentId2);
        job2.addTask(task2);
        execMgr.addJob(job2);

        waitForJobFinish(job2.getId());
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(task2.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, execMgr.getOutput(job2.getId()).getState());
    }
}
