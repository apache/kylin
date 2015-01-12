package com.kylinolap.job.impl.threadpool;

import com.kylinolap.job.*;
import com.kylinolap.job.execution.ExecutableState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by qianzhou on 12/19/14.
 */
public class DefaultSchedulerTest extends BaseSchedulerTest {

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
