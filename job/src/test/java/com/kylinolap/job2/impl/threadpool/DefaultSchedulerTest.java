package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.BaseTestExecutable;
import com.kylinolap.job2.ErrorTestExecutable;
import com.kylinolap.job2.FailedTestExecutable;
import com.kylinolap.job2.SucceedTestExecutable;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.service.DefaultJobService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.UUID;

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
        waitForJob(job.getId());
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(task1.getId()));
    }

    @Test
    public void testSucceed() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJob(job.getId());
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(task1.getId()));
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(task2.getId()));
    }
    @Test
    public void testSucceedAndFailed() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new FailedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJob(job.getId());
        assertEquals(ExecutableStatus.ERROR, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(task1.getId()));
        assertEquals(ExecutableStatus.ERROR, jobService.getJobStatus(task2.getId()));
    }
    @Test
    public void testSucceedAndError() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new ErrorTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        jobService.addJob(job);
        waitForJob(job.getId());
        assertEquals(ExecutableStatus.ERROR, jobService.getJobStatus(job.getId()));
        assertEquals(ExecutableStatus.ERROR, jobService.getJobStatus(task1.getId()));
        assertEquals(ExecutableStatus.READY, jobService.getJobStatus(task2.getId()));
    }
}
