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
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.BatchOptimizeJobCheckpointBuilder;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.zookeeper.ZookeeperJobLock;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.job.StorageCleanupJob;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.util.HBaseRegionSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class BuildCubeWithEngine {

    private CubeManager cubeManager;
    private CubeDescManager cubeDescManager;
    private DefaultScheduler scheduler;
    private ExecutableManager jobService;
    private static boolean fastBuildMode = false;
    private static boolean simpleBuildMode = false;
    private static int engineType;

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithEngine.class);

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int exitCode = 0;
        try {
            beforeClass();

            BuildCubeWithEngine buildCubeWithEngine = new BuildCubeWithEngine();
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
        System.out.println("Time elapsed: " + (millis / 1000) + " sec - in " + BuildCubeWithEngine.class.getName());

        System.exit(exitCode);
    }

    public static void beforeClass() throws Exception {
        beforeClass(HBaseMetadataTestCase.SANDBOX_TEST_DATA);
    }

    public static void beforeClass(String confDir) throws Exception {
        logger.info("Adding to classpath: " + new File(confDir).getAbsolutePath());
        ClassUtil.addClasspath(new File(confDir).getAbsolutePath());

        fastBuildMode = isFastBuildMode();
        simpleBuildMode = isSimpleBuildMode();
        if (fastBuildMode) {
            logger.info("Will use fast build mode");
        }
        if (simpleBuildMode) {
            logger.info("Will use simple build mode");
        }

        String specifiedEngineType = System.getProperty("engineType");
        if (StringUtils.isNotEmpty(specifiedEngineType)) {
            engineType = Integer.parseInt(specifiedEngineType);
        }

        System.setProperty(KylinConfig.KYLIN_CONF, confDir);
        System.setProperty("SPARK_HOME", "/usr/local/spark"); // need manually create and put spark to this folder on Jenkins
        System.setProperty("kylin.hadoop.conf.dir", confDir);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException(
                    "No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.4.0.0-169");
        }

        HBaseMetadataTestCase.staticCreateTestMetadata(confDir);

        try {
            //check hdfs permission
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            String hdfsWorkingDirectory = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
            Path coprocessorDir = new Path(hdfsWorkingDirectory);
            boolean success = fileSystem.mkdirs(coprocessorDir);
            if (!success) {
                throw new IOException("mkdir fails");
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "failed to create kylin.env.hdfs-working-dir, Please make sure the user has right to access "
                            + KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(),
                    e);
        }
    }

    private static boolean isFastBuildMode() {
        String fastModeStr = System.getProperty("fastBuildMode");
        if (fastModeStr == null)
            fastModeStr = System.getenv("KYLIN_CI_FASTBUILD");

        return "true".equalsIgnoreCase(fastModeStr);
    }

    private static boolean isSimpleBuildMode() {
        String simpleModeStr = System.getProperty("simpleBuildMode");
        if (simpleModeStr == null)
            simpleModeStr = System.getenv("KYLIN_CI_SIMPLEBUILD");

        return "true".equalsIgnoreCase(simpleModeStr);
    }

    protected void deployEnv() throws IOException {
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
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
            AbstractExecutable executable = jobService.getJob(jobId);
            if (executable instanceof CubingJob || executable instanceof CheckpointExecutable) {
                jobService.deleteJob(jobId);
            }
        }

        cubeDescManager = CubeDescManager.getInstance(kylinConfig);

        // update enginType
        updateCubeEngineType(Lists.newArrayList("ci_inner_join_cube", "ci_left_join_cube"));
    }

    public void after() {
        DefaultScheduler.destroyInstance();
    }

    public static void afterClass() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void build() throws Exception {
        DeployUtil.prepareTestDataForNormalCubes("ci_left_join_model");
        System.setProperty("kylin.storage.hbase.hfile-size-gb", "1.0f");
        testCase("testInnerJoinCube");
        testCase("testLeftJoinCube");
        testCase("testTableExt");
        testCase("testModel");
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

    private void testCase(String... testCase) throws Exception {
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
                if (!result) {
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

        @Override
        public Boolean call() throws Exception {
            try {
                final Method method = BuildCubeWithEngine.class.getDeclaredMethod(methodName);
                method.setAccessible(true);
                return (Boolean) method.invoke(BuildCubeWithEngine.this);
            } catch (Exception e) {
                logger.error(e.getMessage());
                throw e;
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    protected boolean testTableExt() throws Exception {
        return true;
    }

    protected boolean testModel() throws Exception {
        return true;
    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testLeftJoinCube() throws Exception {
        String cubeName = "ci_left_join_cube";
        clearSegment(cubeName);

        // NOTE: ci_left_join_cube has percentile which isn't supported by Spark engine now

        return doBuildAndMergeOnCube(cubeName);
    }

    private boolean doBuildAndMergeOnCube(String cubeName) throws ParseException, Exception {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2012-06-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();
        long date4 = f.parse("2022-01-01").getTime();
        long date5 = f.parse("2023-01-01").getTime();
        long date6 = f.parse("2024-01-01").getTime();
        CubeInstance cube = cubeManager.getCube(cubeName);
        if (simpleBuildMode) {
            if (!buildSegment(cubeName, date1, date4))
                return false;
        } else {
            if (!buildSegment(cubeName, date1, date2))
                return false;
            if (!fastBuildMode) {
                checkNormalSegRangeInfo(cubeManager.getCube(cubeName));
            }
            if (!buildSegment(cubeName, date2, date3))
                return false;
            if (!fastBuildMode) {
                checkNormalSegRangeInfo(cubeManager.getCube(cubeName));
            }
            if (!buildSegment(cubeName, date3, date4))
                return false;
            if (!fastBuildMode) {
                checkNormalSegRangeInfo(cubeManager.getCube(cubeName));
            }
            if (!buildSegment(cubeName, date4, date5, true)) // one empty segment
                return false;
            if (!fastBuildMode) {
                checkEmptySegRangeInfo(cubeManager.getCube(cubeName));
            }
            if (!buildSegment(cubeName, date5, date6, true)) // another empty segment
                return false;
            if (!fastBuildMode) {
                checkEmptySegRangeInfo(cubeManager.getCube(cubeName));
            }
            if (fastBuildMode && !checkJobState()) {
                return false;
            }
            logger.info("all of build jobs complete!");
            if (!mergeSegment(cubeName, date2, date4)) // merge 2 normal segments
                return false;
            checkNormalSegRangeInfo(cubeManager.getCube(cubeName));
            if (!mergeSegment(cubeName, date2, date5)) // merge normal and empty
                return false;
            checkNormalSegRangeInfo(cubeManager.getCube(cubeName));
        }
        if (fastBuildMode && !checkJobState()) {
            return false;
        }
        if (!simpleBuildMode && !optimizeCube(cubeName))
            return false;
        logger.info("all of jobs complete!");
        return true;
    }

    @SuppressWarnings("unused")
    // called by reflection
    private boolean testInnerJoinCube() throws Exception {

        String cubeName = "ci_inner_join_cube";
        clearSegment(cubeName);

        return doBuildAndMergeOnCube(cubeName);
    }

    private void updateCubeEngineType(List<String> cubeNames) throws IOException {
        if (engineType != 0) {
            for (String cubeName : cubeNames) {
                CubeDesc cubeDesc = cubeDescManager.getCubeDesc(cubeName);
                if (cubeDesc.getEngineType() != engineType) {
                    cubeDesc.setEngineType(engineType);
                    cubeDescManager.updateCubeDesc(cubeDesc);
                }
            }
        }
    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        cubeManager.updateCubeDropSegments(cube, cube.getSegments());
    }

    private Boolean optimizeCube(String cubeName) throws Exception {
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        Set<Long> cuboidsRecommend = mockRecommendCuboids(cubeInstance, 0.05, 255);
        CubeSegment[] optimizeSegments = cubeManager.optimizeSegments(cubeInstance, cuboidsRecommend);
        List<AbstractExecutable> optimizeJobList = Lists.newArrayListWithExpectedSize(optimizeSegments.length);
        for (CubeSegment optimizeSegment : optimizeSegments) {
            DefaultChainedExecutable optimizeJob = EngineFactory.createBatchOptimizeJob(optimizeSegment, "TEST");
            jobService.addJob(optimizeJob);
            optimizeJobList.add(optimizeJob);
            optimizeSegment.setLastBuildJobID(optimizeJob.getId());
        }
        CheckpointExecutable checkpointJob = new BatchOptimizeJobCheckpointBuilder(cubeInstance, "TEST").build();
        checkpointJob.addTaskListForCheck(optimizeJobList);
        jobService.addJob(checkpointJob);
        if (fastBuildMode) {
            return true;
        }
        ExecutableState state = waitForJob(checkpointJob.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    private static Map<String, String> jobCheckActionMap = new HashMap<>();
    private static Map<String, CubeSegment> jobSegmentMap = new HashMap<>();

    private Boolean mergeSegment(String cubeName, long startDate, long endDate) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), new TSRange(startDate, endDate),
                null, true);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    private Boolean buildSegment(String cubeName, long startDate, long endDate, boolean isEmpty) throws Exception {
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        CubeSegment segment = cubeManager.appendSegment(cubeInstance, new TSRange(0L, endDate));
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        if (fastBuildMode) {
            jobSegmentMap.put(job.getId(), segment);
            jobCheckActionMap.put(job.getId(), isEmpty ? "checkEmptySegRangeInfo" : "checkNormalSegRangeInfo");
            return true;
        }
        ExecutableState state = waitForJob(job.getId());
        return Boolean.valueOf(ExecutableState.SUCCEED == state);
    }

    private Boolean buildSegment(String cubeName, long startDate, long endDate) throws Exception {
        return buildSegment(cubeName, startDate, endDate, false);
    }

    private boolean checkJobState() throws Exception {
        List<String> jobIds = jobService.getAllJobIdsInCache();
        while (true) {
            if (jobIds.size() == 0) {
                return true;
            }
            for (int i = jobIds.size() - 1; i >= 0; i--) {
                String jobId = jobIds.get(i);
                Output job = jobService.getOutputDigest(jobId);
                if (job.getState() == ExecutableState.ERROR) {
                    return false;
                } else if (job.getState() == ExecutableState.SUCCEED) {
                    String checkActionName = jobCheckActionMap.get(jobId);
                    CubeSegment jobSegment = jobSegmentMap.get(jobId);
                    if (checkActionName != null) {
                        Method checkAction = this.getClass().getDeclaredMethod(checkActionName, CubeSegment.class);
                        checkAction.invoke(this, jobSegment);
                        jobCheckActionMap.remove(jobId);
                        jobSegmentMap.remove(jobId);
                    }
                    jobIds.remove(i);
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Set<Long> mockRecommendCuboids(CubeInstance cubeInstance, double maxRatio, int maxNumber) {
        Preconditions.checkArgument(maxRatio > 0.0 && maxRatio < 1.0);
        Preconditions.checkArgument(maxNumber > 0);
        Set<Long> cuboidsRecommend;
        Random rnd = new Random();

        // add some mandatory cuboids which are for other unit test
        // - org.apache.kylin.query.ITCombinationTest.testLimitEnabled
        // - org.apache.kylin.query.ITFailfastQueryTest.testPartitionNotExceedMaxScanBytes
        // - org.apache.kylin.query.ITFailfastQueryTest.testQueryNotExceedMaxScanBytes
        List<Set<String>> mandatoryDimensionSetList = Lists.newLinkedList();
        mandatoryDimensionSetList.add(Sets.newHashSet("CAL_DT"));
        mandatoryDimensionSetList.add(Sets.newHashSet("seller_id", "CAL_DT"));
        mandatoryDimensionSetList.add(Sets.newHashSet("LSTG_FORMAT_NAME", "slr_segment_cd"));
        Set<Long> mandatoryCuboids = cubeInstance.getDescriptor().generateMandatoryCuboids(mandatoryDimensionSetList);

        CuboidScheduler cuboidScheduler = cubeInstance.getCuboidScheduler();
        Set<Long> cuboidsCurrent = cuboidScheduler.getAllCuboidIds();
        long baseCuboid = cuboidScheduler.getBaseCuboidId();
        do {
            cuboidsRecommend = Sets.newHashSet();
            cuboidsRecommend.add(baseCuboid);
            cuboidsRecommend.addAll(mandatoryCuboids);
            for (long i = 1; i < baseCuboid; i++) {
                if (rnd.nextDouble() < maxRatio) { // add 5% cuboids
                    cuboidsRecommend.add(i);
                }
                if (cuboidsRecommend.size() > maxNumber) {
                    break;
                }
            }
        } while (cuboidsRecommend.equals(cuboidsCurrent));

        return cuboidsRecommend;
    }

    @SuppressWarnings("unused")
    private int cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };

        StorageCleanupJob cli = new StorageCleanupJob();
        cli.execute(args);
        return 0;
    }

    @SuppressWarnings("unused")
    private void checkHFilesInHBase(CubeSegment segment) throws IOException {
        try (Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl())) {
            String tableName = segment.getStorageLocationIdentifier();

            HBaseRegionSizeCalculator cal = new HBaseRegionSizeCalculator(tableName, conn);
            Map<byte[], Long> sizeMap = cal.getRegionSizeMap();
            long totalSize = 0;
            for (Long size : sizeMap.values()) {
                totalSize += size;
            }
            if (totalSize == 0) {
                return;
            }
            Map<byte[], Pair<Integer, Integer>> countMap = cal.getRegionHFileCountMap();
            // check if there's region contains more than one hfile, which means the hfile config take effects
            boolean hasMultiHFileRegions = false;
            for (Pair<Integer, Integer> count : countMap.values()) {
                // check if hfile count is greater than store count
                if (count.getSecond() > count.getFirst()) {
                    hasMultiHFileRegions = true;
                    break;
                }
            }
            if (KylinConfig.getInstanceFromEnv().getHBaseHFileSizeGB() == 0 && hasMultiHFileRegions) {
                throw new IOException("hfile size set to 0, but found region contains more than one hfiles");
            } else if (KylinConfig.getInstanceFromEnv().getHBaseHFileSizeGB() > 0 && !hasMultiHFileRegions) {
                throw new IOException("hfile size set greater than 0, but all regions still has only one hfile");
            }
        }
    }

    private void checkEmptySegRangeInfo(CubeSegment segment) {
        if (segment != null) {
            segment = cubeManager.getCube(segment.getCubeDesc().getName()).getSegmentById(segment.getUuid());
            for (String colId : segment.getDimensionRangeInfoMap().keySet()) {
                DimensionRangeInfo range = segment.getDimensionRangeInfoMap().get(colId);
                if (!(range.getMax() == null && range.getMin() == null)) {
                    throw new RuntimeException("Empty segment must have null info.");
                }
            }
        }
    }

    private void checkEmptySegRangeInfo(CubeInstance cube) {
        CubeSegment segment = getLastModifiedSegment(cube);
        checkEmptySegRangeInfo(segment);
    }

    private void checkNormalSegRangeInfo(CubeSegment segment) throws IOException {
        if (segment != null && segment.getModel().getPartitionDesc().isPartitioned()) {
            segment = cubeManager.getCube(segment.getCubeDesc().getName()).getSegmentById(segment.getUuid());
            TblColRef colRef = segment.getModel().getPartitionDesc().getPartitionDateColumnRef();
            DimensionRangeInfo dmRangeInfo = segment.getDimensionRangeInfoMap().get(colRef.getIdentity());
            if (dmRangeInfo != null) {
                long min_v = DateFormat.stringToMillis(dmRangeInfo.getMin());
                long max_v = DateFormat.stringToMillis(dmRangeInfo.getMax());
                long ts_range_start = segment.getTSRange().start.v;
                long ts_range_end = segment.getTSRange().end.v;
                if (!(ts_range_start <= min_v && max_v <= ts_range_end - 1)) {
                    throw new RuntimeException(String.format(Locale.ROOT,
                            "Build cube failed, wrong partition column min/max value."
                                    + " Segment: %s, min value: %s, TsRange.start: %s, max value: %s, TsRange.end: %s",
                            segment, min_v, ts_range_start, max_v, ts_range_end));
                }
            }
        }
    }

    private void checkNormalSegRangeInfo(CubeInstance cube) throws IOException {
        CubeSegment segment = getLastModifiedSegment(cube);
        checkNormalSegRangeInfo(segment);
    }

    private CubeSegment getLastModifiedSegment(CubeInstance cube) {
        Segments segments = cube.getSegments();
        if (segments.size() > 0) {
            return Collections.max(segments, new Comparator<CubeSegment>() {
                @Override
                public int compare(CubeSegment o1, CubeSegment o2) {
                    return Long.compare(o1.getLastBuildTime(), o2.getLastBuildTime());
                }
            });
        }
        return null;
    }
}
