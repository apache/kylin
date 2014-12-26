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
public class DefaultSchedulerTest extends LocalFileMetadataTestCase {

    private DefaultScheduler scheduler;

    private DefaultJobService jobService;

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    private void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            System.out.println("job:" + jobId + " status:" + job.getStatus());
            if (job.getStatus() == ExecutableStatus.SUCCEED || job.getStatus() == ExecutableStatus.ERROR) {
                break;
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        setFinalStatic(JobConstants.class.getField("DEFAULT_SCHEDULER_INTERVAL_SECONDS"), 10);
        jobService = DefaultJobService.getInstance(KylinConfig.getInstanceFromEnv());
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
//        scheduler.shutdown();
    }

    @Test
    public void testSucceed() throws Exception {
        assertNotNull(scheduler);
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
        assertNotNull(scheduler);
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
        assertNotNull(scheduler);
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
