package com.kylinolap.job2.cube;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.DeployUtil;
import com.kylinolap.job.ExportHBaseData;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.cube.StorageCleanupJob;
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
import java.util.List;

import static org.junit.Assert.*;

@Ignore
public class BuildCubeJobBuilderTest {

    private JobEngineConfig jobEngineConfig;

    private CubeManager cubeManager;

    private DefaultScheduler scheduler;

    protected ExecutableManager jobService;

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

        setFinalStatic(JobConstants.class.getField("DEFAULT_SCHEDULER_INTERVAL_SECONDS"), 10);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
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
//        int exitCode = cleanupOldCubes();
//        if (exitCode == 0) {
//            exportHBaseData();
//        }
//
//        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void testBuild() throws Exception {
        final CubeInstance cubeInstance = cubeManager.getCube("test_kylin_cube_without_slr_left_join_empty");
        assertNotNull(cubeInstance);
        final CubeSegment cubeSegment = cubeManager.appendSegments(cubeInstance, 0, System.currentTimeMillis());
        final BuildCubeJobBuilder buildCubeJobBuilder = BuildCubeJobBuilder.newBuilder(jobEngineConfig, cubeSegment);
        final BuildCubeJob job = buildCubeJobBuilder.build();
        jobService.addJob(job);
        waitForJob(job.getId());
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(job.getId()).getState());
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