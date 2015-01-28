package com.kylinolap.job;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.cube.CubingJob;
import com.kylinolap.job.cube.CubingJobBuilder;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.execution.AbstractExecutable;
import com.kylinolap.job.execution.ExecutableState;
import com.kylinolap.job.hadoop.cube.StorageCleanupJob;
import com.kylinolap.job.impl.threadpool.DefaultScheduler;
import com.kylinolap.job.manager.ExecutableManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

public class BuildCubeWithEngineTest {

    private JobEngineConfig jobEngineConfig;

    private CubeManager cubeManager;

    private DefaultScheduler scheduler;

    protected ExecutableManager jobService;

    private static final Log logger = LogFactory.getLog(BuildCubeWithEngineTest.class);

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
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClasspathUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.0.0-2041"); // mapred-site.xml ref this
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);

        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();
        

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

    private void backup() throws Exception {
        int exitCode = cleanupOldCubes();
        if (exitCode == 0) {
            exportHBaseData();
        }

        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        testInner();
        testLeft();
        backup();
    }

    private void testInner() throws Exception {
        DeployUtil.prepareTestData("inner", "test_kylin_cube_with_slr_empty");
        String[] testCase = new String[]{
                "testInnerJoinCube",
                "testInnerJoinCube2",
        };
        runTestAndAssertSucceed(testCase);
    }

    private void testLeft() throws Exception {
        DeployUtil.prepareTestData("left", "test_kylin_cube_with_slr_left_join_empty");
        String[] testCase = new String[]{
                "testLeftJoinCube",
                "testLeftJoinCube2",
        };
        runTestAndAssertSucceed(testCase);
    }

    private void runTestAndAssertSucceed(String[] testCase) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(testCase.length);
        final CountDownLatch countDownLatch = new CountDownLatch(testCase.length);
        List<Future<List<String>>> tasks = Lists.newArrayListWithExpectedSize(testCase.length);
        for (int i = 0; i < testCase.length; i++) {
            tasks.add(executorService.submit(new TestCallable(testCase[i], countDownLatch)));
        }
        countDownLatch.await();
        try {
            for (int i = 0; i < tasks.size(); ++i) {
                Future<List<String>> task = tasks.get(i);
                final List<String> jobIds = task.get();
                for (String jobId : jobIds) {
                    assertJobSucceed(jobId);
                }
            }
        } catch (Exception ex) {
            logger.error(ex);
            throw ex;
        }
    }

    private void assertJobSucceed(String jobId) {
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(jobId).getState());
    }

    private class TestCallable implements Callable<List<String>> {

        private final String methodName;
        private final CountDownLatch countDownLatch;

        public TestCallable(String methodName, CountDownLatch countDownLatch) {
            this.methodName = methodName;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public List<String> call() throws Exception {
            try {
                final Method method = BuildCubeWithEngineTest.class.getDeclaredMethod(methodName);
                method.setAccessible(true);
                return (List<String>) method.invoke(BuildCubeWithEngineTest.this);
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    private List<String> testInnerJoinCube2() throws Exception {
        clearSegment("test_kylin_cube_with_slr_empty");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2022-01-01").getTime();
        List<String> result = Lists.newArrayList();
        result.add(buildSegment("test_kylin_cube_with_slr_empty", date1, date2));
        result.add(buildSegment("test_kylin_cube_with_slr_empty", date2, date3));
        return result;
    }

    private List<String> testInnerJoinCube() throws Exception {
        clearSegment("test_kylin_cube_without_slr_empty");


        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        // this cube's start date is 0, end date is 20501112000000
        long date1 = 0;
        long date2 = f.parse("2013-01-01").getTime();


        // this cube doesn't support incremental build, always do full build

        List<String> result = Lists.newArrayList();
        result.add(buildSegment("test_kylin_cube_without_slr_empty", date1, date2));
        return result;
    }

    private List<String> testLeftJoinCube2() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        List<String> result = Lists.newArrayList();
        final String cubeName = "test_kylin_cube_without_slr_left_join_empty";
        // this cube's start date is 0, end date is 20120601000000
        long dateStart = cubeManager.getCube(cubeName).getDescriptor().getModel().getPartitionDesc().getPartitionDateStart();
        long dateEnd = f.parse("2012-06-01").getTime();

        clearSegment(cubeName);
        result.add(buildSegment(cubeName, dateStart, dateEnd));

        // then submit an append job, start date is 20120601000000, end
        // date is 20220101000000
        dateStart = f.parse("2012-06-01").getTime();
        dateEnd = f.parse("2022-01-01").getTime();
        result.add(buildSegment(cubeName, dateStart, dateEnd));
        return result;

    }

    private List<String> testLeftJoinCube() throws Exception {
        String cubeName = "test_kylin_cube_with_slr_left_join_empty";
        clearSegment(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long dateStart = cubeManager.getCube(cubeName).getDescriptor().getModel().getPartitionDesc().getPartitionDateStart();
        long dateEnd = f.parse("2050-11-12").getTime();

        // this cube's start date is 0, end date is 20501112000000
        List<String> result = Lists.newArrayList();
        result.add(buildSegment(cubeName, dateStart, dateEnd));
        return result;

    }

    private void clearSegment(String cubeName) throws Exception{
        CubeInstance cube = cubeManager.getCube(cubeName);
        cube.getSegments().clear();
        cubeManager.updateCube(cube);
    }


    private String buildSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeSegment segment = cubeManager.appendSegments(cubeManager.getCube(cubeName), endDate);
        CubingJobBuilder cubingJobBuilder = new CubingJobBuilder(jobEngineConfig);
        CubingJob job = cubingJobBuilder.buildJob(segment);
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
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