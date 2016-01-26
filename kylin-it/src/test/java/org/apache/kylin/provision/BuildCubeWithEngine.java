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

package org.apache.kylin.provision;

import java.io.File;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.storage.hbase.util.StorageCleanupJob;
import org.apache.kylin.storage.hbase.util.ZookeeperJobLock;

import com.google.common.collect.Lists;

public class BuildCubeWithEngine {

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    private static boolean fastBuildMode = false;

    private static final Log logger = LogFactory.getLog(BuildCubeWithEngine.class);

    public static void main(String[] args) throws Exception {
        beforeClass();
        BuildCubeWithEngine buildCubeWithEngine = new BuildCubeWithEngine();
        buildCubeWithEngine.before();
        buildCubeWithEngine.build();
        logger.info("Build is done");
        afterClass();
        logger.info("Going to exit");
        System.exit(0);
    }

    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());

        String fastModeStr = System.getProperty("fastBuildMode");
        if (fastModeStr != null && fastModeStr.equalsIgnoreCase("true")) {
            fastBuildMode = true;
            logger.info("Will use fast build mode");
        } else {
            logger.info("Will not use fast build mode");
        }

        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/sandbox");
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }

        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);
    }

    public void before() throws Exception {

        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);
        for (String jobId : jobService.getAllJobIds()) {
            if (jobService.getJob(jobId) instanceof CubingJob) {
                jobService.deleteJob(jobId);
            }
        }

    }

    public static void afterClass() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void build() throws Exception {
        DeployUtil.prepareTestDataForNormalCubes("test_kylin_cube_with_slr_left_join_empty");
        testInner();
        testLeft();
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

    private void testInner() throws Exception {
        String[] testCase = new String[] { "testInnerJoinCubeWithoutSlr", "testInnerJoinCubeWithSlr" };
        runTestAndAssertSucceed(testCase);
    }

    private void testLeft() throws Exception {
        String[] testCase = new String[] { "testLeftJoinCubeWithSlr", "testLeftJoinCubeWithoutSlr" };
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
        if (jobService.getOutput(jobId).getState() != ExecutableState.SUCCEED) {
            throw new RuntimeException("The job '" + jobId + "' is failed.");
        }
    }

    private class TestCallable implements Callable<List<String>> {

        private final String methodName;
        private final CountDownLatch countDownLatch;

        public TestCallable(String methodName, CountDownLatch countDownLatch) {
            this.methodName = methodName;
            this.countDownLatch = countDownLatch;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<String> call() throws Exception {
            try {
                final Method method = BuildCubeWithEngine.class.getDeclaredMethod(methodName);
                method.setAccessible(true);
                return (List<String>) method.invoke(BuildCubeWithEngine.this);
            } catch (Exception e) {
                logger.error(e.getMessage());
                throw e;
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    @SuppressWarnings("unused")
    // called by reflection
    private List<String> testInnerJoinCubeWithSlr() throws Exception {
        clearSegment("test_kylin_cube_with_slr_empty");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2015-01-01").getTime();
        long date3 = f.parse("2022-01-01").getTime();
        List<String> result = Lists.newArrayList();

        if (fastBuildMode) {
            result.add(buildSegment("test_kylin_cube_with_slr_empty", date1, date3));
        } else {
            result.add(buildSegment("test_kylin_cube_with_slr_empty", date1, date2));
            result.add(buildSegment("test_kylin_cube_with_slr_empty", date2, date3));//empty segment
        }
        return result;
    }

    @SuppressWarnings("unused")
    // called by reflection
    private List<String> testInnerJoinCubeWithoutSlr() throws Exception {

        clearSegment("test_kylin_cube_without_slr_empty");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();
        long date4 = f.parse("2022-01-01").getTime();
        List<String> result = Lists.newArrayList();

        if (fastBuildMode) {
            result.add(buildSegment("test_kylin_cube_without_slr_empty", date1, date4));
        } else {
            result.add(buildSegment("test_kylin_cube_without_slr_empty", date1, date2));
            result.add(buildSegment("test_kylin_cube_without_slr_empty", date2, date3));
            result.add(buildSegment("test_kylin_cube_without_slr_empty", date3, date4));
            result.add(mergeSegment("test_kylin_cube_without_slr_empty", date1, date3));//don't merge all segments
        }
        return result;

    }

    @SuppressWarnings("unused")
    // called by reflection
    private List<String> testLeftJoinCubeWithoutSlr() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        List<String> result = Lists.newArrayList();
        final String cubeName = "test_kylin_cube_without_slr_left_join_empty";
        clearSegment(cubeName);

        long date1 = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long date2 = f.parse("2012-06-01").getTime();
        long date3 = f.parse("2022-01-01").getTime();
        long date4 = f.parse("2023-01-01").getTime();

        if (fastBuildMode) {
            result.add(buildSegment("test_kylin_cube_without_slr_left_join_empty", date1, date4));
        } else {
            result.add(buildSegment("test_kylin_cube_without_slr_left_join_empty", date1, date2));
            result.add(buildSegment("test_kylin_cube_without_slr_left_join_empty", date2, date3));
            result.add(buildSegment("test_kylin_cube_without_slr_left_join_empty", date3, date4));//empty segment
            result.add(mergeSegment("test_kylin_cube_without_slr_left_join_empty", date1, date3));//don't merge all segments
        }

        return result;

    }

    @SuppressWarnings("unused")
    // called by reflection
    private List<String> testLeftJoinCubeWithSlr() throws Exception {
        String cubeName = "test_kylin_cube_with_slr_left_join_empty";
        clearSegment(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();
        long date4 = f.parse("2022-01-01").getTime();

        List<String> result = Lists.newArrayList();
        if (fastBuildMode) {
            result.add(buildSegment(cubeName, date1, date4));
        } else {
            result.add(buildSegment(cubeName, date1, date2));
            result.add(buildSegment(cubeName, date2, date3));
            result.add(buildSegment(cubeName, date3, date4));
            result.add(mergeSegment(cubeName, date1, date3));//don't merge all segments
        }
        return result;

    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        // remove all existing segments
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        cubeManager.updateCube(cubeBuilder);
    }

    private String mergeSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), startDate, endDate, true);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    private String buildSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeSegment segment = cubeManager.appendSegments(cubeManager.getCube(cubeName), endDate);
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    private int cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };

        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        return exitCode;
    }

}