package com.kylinolap.job2.cube;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultScheduler;
import com.kylinolap.job2.service.DefaultJobService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.Assert.*;

public class BuildCubeJobBuilderTest extends HBaseMetadataTestCase {

    private JobEngineConfig jobEngineConfig;

    private CubeManager cubeManager;

    private DefaultScheduler scheduler;

    protected DefaultJobService jobService;

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    protected void waitForJob(String jobId) {
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

    @BeforeClass
    public static void beforeClass() throws Exception {
        ClasspathUtil.addClasspath(new File(SANDBOX_TEST_DATA).getAbsolutePath());
    }

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        setFinalStatic(JobConstants.class.getField("DEFAULT_SCHEDULER_INTERVAL_SECONDS"), 10);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = DefaultJobService.getInstance(kylinConfig);
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(kylinConfig));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);
        jobEngineConfig = new JobEngineConfig(kylinConfig);
        for (AbstractExecutable job: jobService.getAllExecutables()) {
            jobService.deleteJob(job);
        }
        final CubeInstance testCube = cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty");
        testCube.getSegments().clear();
        cubeManager.updateCube(testCube);

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testBuild() throws Exception {
        final CubeInstance cubeInstance = cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty");
        assertNotNull(cubeInstance);
        final List<CubeSegment> cubeSegments = cubeManager.appendSegments(cubeInstance, 0, System.currentTimeMillis());
        final BuildCubeJobBuilder buildCubeJobBuilder = BuildCubeJobBuilder.newBuilder(jobEngineConfig, cubeSegments.get(0));
        final BuildCubeJob job = buildCubeJobBuilder.build();
        jobService.addJob(job);
        waitForJob(job.getId());
        assertEquals(ExecutableStatus.SUCCEED, jobService.getJobStatus(job.getId()));
    }
}