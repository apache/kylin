package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.service.ExecutableManager;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Created by qianzhou on 12/26/14.
 */
public abstract class BaseSchedulerTest extends LocalFileMetadataTestCase {

    private DefaultScheduler scheduler;

    protected ExecutableManager jobService;

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    protected void waitForJobFinish(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            final ExecutableState status = job.getStatus();
            if (status == ExecutableState.SUCCEED || status == ExecutableState.ERROR || status == ExecutableState.STOPPED) {
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

    protected void waitForJobStatus(String jobId, ExecutableState state, long interval) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == state) {
                break;
            } else {
                try {
                    Thread.sleep(interval);
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
        jobService = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }
}
