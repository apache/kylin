package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.job2.*;
import com.kylinolap.job2.execution.ExecutableState;
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
        assertEquals(ExecutableState.SUCCEED, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableState.SUCCEED, jobService.getJobStatus(task1.getId()));
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
        assertEquals(ExecutableState.SUCCEED, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableState.SUCCEED, jobService.getJobStatus(task1.getId()));
        assertEquals(ExecutableState.SUCCEED, jobService.getJobStatus(task2.getId()));
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
        assertEquals(ExecutableState.ERROR, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableState.SUCCEED, jobService.getJobStatus(task1.getId()));
        assertEquals(ExecutableState.ERROR, jobService.getJobStatus(task2.getId()));
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
        assertEquals(ExecutableState.ERROR, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableState.ERROR, jobService.getJobStatus(task1.getId()));
        assertEquals(ExecutableState.READY, jobService.getJobStatus(task2.getId()));
    }

    @Test
    public void testStop() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SelfStopExecutable();
        job.addTask(task1);
        jobService.addJob(job);
        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);
        jobService.stopJob(job.getId());
        waitForJobFinish(job.getId());
        assertEquals(ExecutableState.STOPPED, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableState.STOPPED, jobService.getJobStatus(task1.getId()));
    }
}
