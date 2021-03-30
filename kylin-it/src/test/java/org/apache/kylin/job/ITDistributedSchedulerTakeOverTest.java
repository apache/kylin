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

public class ITDistributedSchedulerTakeOverTest extends BaseTestDistributedScheduler {
    @Test
    public void testSchedulerTakeOver() throws Exception {
        if (!lock(jobLock1, jobId2)) {
            throw new JobException("fail to get the lock");
        }

        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setId(jobId2);
        AbstractExecutable task1 = new SucceedTestExecutable();
        AbstractExecutable task2 = new SucceedTestExecutable();
        AbstractExecutable task3 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        job.addTask(task3);
        execMgr.addJob(job);

        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);

        scheduler1.shutdown();
        scheduler1 = null;

        waitForJobFinish(job.getId());

        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task2.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(task3.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, execMgr.getOutput(job.getId()).getState());
    }
}
