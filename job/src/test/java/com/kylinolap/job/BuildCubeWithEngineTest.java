package com.kylinolap.job;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.cube.StorageCleanupJob;
import com.kylinolap.job2.cube.CubingJob;
import com.kylinolap.job2.cube.CubingJobBuilder;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultScheduler;
import com.kylinolap.job2.service.ExecutableManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BuildCubeWithEngineTest {

    private JobEngineConfig jobEngineConfig;

    private CubeManager cubeManager;

    private DefaultScheduler scheduler;

    protected ExecutableManager jobService;

    protected void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
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
        ClasspathUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);

        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();
        DeployUtil.overrideJobConf(HBaseMetadataTestCase.SANDBOX_TEST_DATA);

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(kylinConfig));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);
        jobEngineConfig = new JobEngineConfig(kylinConfig);
        for (String jobId: jobService.getAllJobIds()) {
            jobService.deleteJob(jobId);
        }

    }

    @After
    public void after() throws Exception {
        int exitCode = cleanupOldCubes();
        if (exitCode == 0) {
            exportHBaseData();
        }

        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void testBuild() throws Exception {
        final String testCubeName = "test_kylin_cube_without_slr_left_join_empty";
        final CubeInstance testCube = cubeManager.getCube(testCubeName);
        testCube.getSegments().clear();
        cubeManager.updateCube(testCube);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date1 = dateFormat.parse("2013-01-01");
        final CubeSegment cubeSegment1 = cubeManager.appendSegments(testCube, 0, date1.getTime());
        final CubingJobBuilder cubingJobBuilder = CubingJobBuilder.newBuilder().setJobEnginConfig(jobEngineConfig).setSegment(cubeSegment1);
        final CubingJob job = cubingJobBuilder.buildJob();
        jobService.addJob(job);
        waitForJob(job.getId());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job.getId()).getState());

        Date date2 = dateFormat.parse("2013-04-01");
        final CubeSegment cubeSegment2 = cubeManager.appendSegments(testCube, date1.getTime(), date2.getTime());
        final CubingJob job2 = CubingJobBuilder.newBuilder().setJobEnginConfig(jobEngineConfig).setSegment(cubeSegment2).buildJob();
        jobService.addJob(job2);
        waitForJob(job2.getId());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job2.getId()).getState());

        final CubeSegment cubeSegment3 = cubeManager.mergeSegments(testCube, 0, date2.getTime());
        final CubingJob job3 = CubingJobBuilder.newBuilder().setJobEnginConfig(jobEngineConfig).setSegment(cubeSegment3).mergeJob();
        jobService.addJob(job3);
        waitForJob(job3.getId());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job3.getId()).getState());

    }

    private int cleanupOldCubes() throws Exception {
        String[] args = { "--delete", "true" };

        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        return exitCode;
    }

    private void exportHBaseData() throws IOException {
        ExportHBaseData export = new ExportHBaseData();
        export.exportTables();
    }
}