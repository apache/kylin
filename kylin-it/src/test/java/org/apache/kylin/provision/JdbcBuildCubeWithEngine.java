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

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.common.KylinConfig;
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
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.SourcePartition;
import org.apache.kylin.storage.hbase.util.ZookeeperJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class JdbcBuildCubeWithEngine {

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    private static boolean fastBuildMode = false;

    private static final Logger logger = LoggerFactory.getLogger(JdbcBuildCubeWithEngine.class);
    public static final String JDBC_TEST_DATA="../examples/jdbc_case_data/sandbox";
    public static final String JDBC_LOCALMETA_TEST_DATA = "../examples/jdbc_case_data/localmeta";

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int exitCode = 0;
        try {
            BuildCubeWithEngine.beforeClass("../examples/jdbc_case_data/sandbox");

            JdbcBuildCubeWithEngine buildCubeWithEngine = new JdbcBuildCubeWithEngine();
            buildCubeWithEngine.before();
            buildCubeWithEngine.build();
            buildCubeWithEngine.after();
            logger.info("Build is done");
            afterClass();
            logger.info("Going to exit");
        } catch (Throwable e) {
            logger.error("error", e);
            exitCode = 1;
        }

        long millis = System.currentTimeMillis() - start;
        System.out.println("Time elapsed: " + (millis / 1000) + " sec - in " + JdbcBuildCubeWithEngine.class.getName());

        System.exit(exitCode);
    }

    protected void deployEnv() throws IOException {
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata(JDBC_LOCALMETA_TEST_DATA);
        DeployUtil.overrideJobJarLocations();
    }

    public void before() throws Exception {
        deployEnv();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.createInstance();
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

    public void after() {
        DefaultScheduler.destroyInstance();
    }

    public static void afterClass() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void build() throws Exception {
        //DeployUtil.prepareTestDataForNormalCubes("ci_left_join_model");
        System.setProperty("kylin.storage.hbase.hfile-size-gb", "1.0f");
        //testInner();
        testLeft();
        System.setProperty("kylin.storage.hbase.hfile-size-gb", "0.0f");
    }

    protected ExecutableState waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                return job.getStatus();
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private void testLeft() throws Exception {
        String[] testCase = new String[] { "testLeftJoinCube" };
        runTestAndAssertSucceed(testCase);
    }

    private void runTestAndAssertSucceed(String[] testCase) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(testCase.length);
        final CountDownLatch countDownLatch = new CountDownLatch(testCase.length);
        List<Future<Boolean>> tasks = Lists.newArrayListWithExpectedSize(testCase.length);
        for (int i = 0; i < testCase.length; i++) {
            tasks.add(executorService.submit(new TestCallable(testCase[i], countDownLatch)));
        }
        countDownLatch.await();
        try {
            for (int i = 0; i < tasks.size(); ++i) {
                Future<Boolean> task = tasks.get(i);
                final Boolean result = task.get();
                if (result == false) {
                    throw new RuntimeException("The test '" + testCase[i] + "' is failed.");
                }
            }
        } catch (Exception ex) {
            logger.error("error", ex);
            throw ex;
        }
    }

    private class TestCallable implements Callable<Boolean> {

        private final String methodName;
        private final CountDownLatch countDownLatch;

        public TestCallable(String methodName, CountDownLatch countDownLatch) {
            this.methodName = methodName;
            this.countDownLatch = countDownLatch;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Boolean call() throws Exception {
            try {
                final Method method = JdbcBuildCubeWithEngine.class.getDeclaredMethod(methodName);
                method.setAccessible(true);
                return (Boolean) method.invoke(JdbcBuildCubeWithEngine.this);
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
    private boolean testLeftJoinCube() throws Exception {
        String cubeName = "ci_left_join_cube";
        clearSegment(cubeName);

        // ci_left_join_cube has percentile which isn't supported by Spark engine now
        // updateCubeEngineType(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();
        long date4 = f.parse("2022-01-01").getTime();

        if (fastBuildMode) {
            return buildSegment(cubeName, date1, date4);
        } else {
            if (buildSegment(cubeName, date1, date2) == true) {
                if (buildSegment(cubeName, date2, date3) == true) {
                    if (buildSegment(cubeName, date3, date4) == true) {
                        return mergeSegment(cubeName, date1, date3); // don't merge all segments
                    }
                }
            }
        }
        return false;
    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        // remove all existing segments
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        cubeManager.updateCube(cubeBuilder);
    }

    private Boolean mergeSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), startDate, endDate, 0, 0, true);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    private Boolean buildSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        ISource source = SourceFactory.getSource(cubeInstance);
        SourcePartition partition = source.enrichSourcePartitionBeforeBuild(cubeInstance, new SourcePartition(0, endDate, 0, 0, null, null));
        CubeSegment segment = cubeManager.appendSegment(cubeInstance, partition.getStartDate(), partition.getEndDate());
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }
}
