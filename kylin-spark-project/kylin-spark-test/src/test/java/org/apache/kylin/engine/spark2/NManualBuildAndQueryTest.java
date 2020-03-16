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
package org.apache.kylin.engine.spark2;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.kylin.engine.spark2.NExecAndComp.CompareLevel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Ignore("see io.kyligence.kap.ut.TestQueryAndBuild")
@SuppressWarnings("serial")
public class NManualBuildAndQueryTest extends LocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryTest.class);

    private boolean succeed = true;

    @Before
    public void setup() throws Exception {
        super.init();
        System.setProperty("kylin.env", "UT");
        System.setProperty("kylin.metadata.distributed-lock-impl", "org.apache.kylin.engine.spark.utils.MockedDistributedLock$MockedFactory");
        System.setProperty("isDeveloperMode", "true");
    }

    @After
    public void after() {
        DefaultScheduler.destroyInstance();
        //super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("noBuild");
        System.clearProperty("isDeveloperMode");
    }

    @Test
    @Ignore("for developing")
    public void testTmp() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        System.setProperty("noBuild", "true");
        System.setProperty("isDeveloperMode", "true");
        buildCubes();
        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());
        List<Pair<String, Throwable>> results = execAndGetResults(
                Lists.newArrayList(new QueryCallable(CompareLevel.SAME, "left", "temp"))); //
        report(results);
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();

        buildCubes();

        // build is done, start to test query
        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());
        List<QueryCallable> tasks = prepareAndGenQueryTasks(config);
        List<Pair<String, Throwable>> results = execAndGetResults(tasks);
        Assert.assertEquals(results.size(), tasks.size());
        report(results);
    }

    private List<Pair<String, Throwable>> execAndGetResults(List<QueryCallable> tasks)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(9//
                , 9 //
                , 1 //
                , TimeUnit.DAYS //
                , new LinkedBlockingQueue<Runnable>(100));
        CompletionService<Pair<String, Throwable>> service = new ExecutorCompletionService<>(executor);
        for (QueryCallable task : tasks) {
            service.submit(task);
        }

        List<Pair<String, Throwable>> results = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            Pair<String, Throwable> r = service.take().get();
            failFastIfNeeded(r);
            results.add(r);
        }
        executor.shutdown();
        return results;
    }

    private void report(List<Pair<String, Throwable>> results) {
        for (Pair<String, Throwable> result : results) {
            if (result.getSecond() != null) {
                succeed = false;
                logger.error("CI failed on:" + result.getFirst(), result.getSecond());
            }
        }
        if (!succeed) {
            Assert.fail();
        }
    }

    private void failFastIfNeeded(Pair<String, Throwable> result) {
        if (Boolean.valueOf(System.getProperty("failFast", "false")) && result.getSecond() != null) {
            logger.error("CI failed on:" + result.getFirst());
            Assert.fail();
        }
    }

    private List<QueryCallable> prepareAndGenQueryTasks(KylinConfig config) throws Exception {
        String[] joinTypes = new String[] { "left", "inner" };
        List<QueryCallable> tasks = new ArrayList<>();
        for (String joinType : joinTypes) {
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_lookup"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_casewhen"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_like"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_cache"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_derived"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_datetime"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_subquery"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct_dim"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_timestamp"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_orderby"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_snowflake"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_topn"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_join"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_union"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_hive"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct_precisely"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_powerbi"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_raw"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_value"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_magine"));
            //            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_cross_join"));

            // same row count
            tasks.add(new QueryCallable(CompareLevel.SAME_ROWCOUNT, joinType, "sql_tableau"));

            // none
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_window"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_h2_uncapable"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_grouping"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_intersect_count"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_percentile"));
            tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_distinct"));

            //execLimitAndValidate
            //            tasks.add(new QueryCallable(CompareLevel.SUBSET, joinType, "sql"));
        }

        // cc tests
        tasks.add(new QueryCallable(CompareLevel.SAME_SQL_COMPARE, "default", "sql_computedcolumn_common"));
        tasks.add(new QueryCallable(CompareLevel.SAME_SQL_COMPARE, "default", "sql_computedcolumn_leftjoin"));

        tasks.add(new QueryCallable(CompareLevel.SAME, "inner", "sql_magine_inner"));
        tasks.add(new QueryCallable(CompareLevel.SAME, "inner", "sql_magine_window"));
        tasks.add(new QueryCallable(CompareLevel.SAME, "default", "sql_rawtable"));
        tasks.add(new QueryCallable(CompareLevel.SAME, "default", "sql_multi_model"));
        logger.info("Total {} tasks.", tasks.size());
        return tasks;
    }

    public void buildCubes() throws Exception {
        if (Boolean.valueOf(System.getProperty("noBuild", "false"))) {
            System.out.println("Direct query");
        } else if (Boolean.valueOf(System.getProperty("isDeveloperMode", "false"))) {
            fullBuildCube("ci_inner_join_cube");
//            fullBuildCube("ci_left_join_cube");
        } else {
            buildAndMergeCube("ci_inner_join_cube");
            buildAndMergeCube("ci_left_join_cube");
        }
    }

    private void buildAndMergeCube(String cubeName) throws Exception {
        if (cubeName.equals("ci_inner_join_cube")) {
            buildFourSegementAndMerge(cubeName);
        }
        if (cubeName.equals("ci_left_join_cube")) {
            buildTwoSegementAndMerge(cubeName);
        }
    }

    private void buildTwoSegementAndMerge(String cubeName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        ExecutableManager execMgr = ExecutableManager.getInstance(config);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        cleanupSegments(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        /**
         * Round1. Build 2 segment
         */
        long start = f.parse("2010-01-01").getTime();
        long end = f.parse("2013-01-01").getTime();
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));
        start = f.parse("2013-01-01").getTime();
        end = f.parse("2015-01-01").getTime();
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));

        /**
         * Round2. Merge two segments
         */

        CubeInstance cube = cubeMgr.reloadCube(cubeName);
        CubeSegment firstMergeSeg = cubeMgr.mergeSegments(cube,
                new SegmentRange.TSRange(f.parse("2010-01-01").getTime(), f.parse("2015-01-01").getTime()), null,
                false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, "ADMIN");
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));
        AfterMergeOrRefreshResourceMerger merger = new AfterMergeOrRefreshResourceMerger(config);
        merger.merge(firstMergeJob.getSparkMergingStep());

        /**
         * validate cube segment info
         */
        CubeSegment firstSegment = cubeMgr.getCube(cubeName).getSegments().get(0);

        /*if (getProject().equals("default") && cubeName.equals("ci_left_join_cube")) {
            Map<Long, NDataLayout> cuboidsMap1 = firstSegment.();
            Map<Long, Long[]> compareTuples1 = Maps.newHashMap();
            compareTuples1.put(1L, new Long[] { 9896L, 9896L });
            compareTuples1.put(10001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(10002L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(30001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(1000001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(1010001L, new Long[] { 731L, 9163L });
            compareTuples1.put(1020001L, new Long[] { 302L, 6649L });
            compareTuples1.put(1030001L, new Long[] { 44L, 210L });
            compareTuples1.put(1040001L, new Long[] { 9163L, 9884L });
            compareTuples1.put(1050001L, new Long[] { 105L, 276L });
            compareTuples1.put(1060001L, new Long[] { 138L, 286L });
            compareTuples1.put(1070001L, new Long[] { 9880L, 9896L });
            compareTuples1.put(1080001L, new Long[] { 9833L, 9896L });
            compareTuples1.put(1090001L, new Long[] { 9421L, 9884L });
            compareTuples1.put(1100001L, new Long[] { 143L, 6649L });
            compareTuples1.put(1110001L, new Long[] { 4714L, 9884L });
            compareTuples1.put(1120001L, new Long[] { 9884L, 9896L });
            compareTuples1.put(20000000001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20000010001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20000020001L, new Long[] { 9896L, 9896L });
            verifyCuboidMetrics(cuboidsMap1, compareTuples1);
        }*/

        Assert.assertEquals(new SegmentRange.TSRange(f.parse("2010-01-01").getTime(), f.parse("2015-01-01").getTime()),
                firstSegment.getSegRange());
        //Assert.assertEquals(27, firstSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
    }

    private void buildFourSegementAndMerge(String cubeName) throws Exception {
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        cleanupSegments(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        /**
         * Round1. Build 4 segment
         */
        long start = f.parse("2010-01-01").getTime();
        long end = f.parse("2012-06-01").getTime();
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));

        start = f.parse("2012-06-01").getTime();
        end = f.parse("2013-01-01").getTime();
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));

        start = f.parse("2013-01-01").getTime();
        end = f.parse("2013-06-01").getTime();
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));

        start = f.parse("2013-06-01").getTime();
        end = f.parse("2015-01-01").getTime();
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));

        /**
         * Round2. Merge two segments
         */
        CubeInstance cube = cubeMgr.reloadCube(cubeName);
        CubeSegment firstMergeSeg = cubeMgr.mergeSegments(cube,
                new SegmentRange.TSRange(f.parse("2010-01-01").getTime(), f.parse("2013-01-01").getTime()), null,
                false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, "ADMIN");
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        cube = cubeMgr.reloadCube(cubeName);

        CubeSegment secondMergeSeg = cubeMgr.mergeSegments(cube, new SegmentRange.TSRange(
                f.parse("2013-01-01").getTime(), f.parse("2015-01-01").getTime()), null, false);
        NSparkMergingJob secondMergeJob = NSparkMergingJob.merge(secondMergeSeg, "ADMIN");
        execMgr.addJob(secondMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(secondMergeJob));

        /**
         * validate cube segment info
         */
        CubeSegment firstSegment = cubeMgr.reloadCube(cubeName).getSegments().get(0);
        CubeSegment secondSegment = cubeMgr.reloadCube(cubeName).getSegments().get(1);

        /*if (getProject().equals("default") && cubeName.equals("ci_inner_join_cube")) {
            Map<Long, NDataLayout> cuboidsMap1 = firstSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples1 = Maps.newHashMap();
            compareTuples1.put(1L, new Long[] { 4903L, 4903L });
            compareTuples1.put(10001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(10002L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(30001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(1000001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000000001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000010001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000020001L, new Long[] { 4903L, 4903L });
            verifyCuboidMetrics(cuboidsMap1, compareTuples1);

            Map<Long, NDataLayout> cuboidsMap2 = secondSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples2 = Maps.newHashMap();
            compareTuples2.put(1L, new Long[] { 5097L, 5097L });
            compareTuples2.put(10001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(10002L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(30001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(1000001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000000001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000010001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000020001L, new Long[] { 5097L, 5097L });
            verifyCuboidMetrics(cuboidsMap2, compareTuples2);
        }*/

        Assert.assertEquals(new SegmentRange.TSRange(f.parse("2010-01-01").getTime(), f.parse("2013-01-01").getTime()),
                firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TSRange(f.parse("2013-01-01").getTime(), f.parse("2015-01-01").getTime()),
                secondSegment.getSegRange());
        //Assert.assertEquals(31, firstSegment.getDictionaries().size());
        //Assert.assertEquals(31, secondSegment.getDictionaries().size());
    }

    class QueryCallable implements Callable<Pair<String, Throwable>> {

        private NExecAndComp.CompareLevel compareLevel;
        private String joinType;
        private String sqlFolder;

        QueryCallable(NExecAndComp.CompareLevel compareLevel, String joinType, String sqlFolder) {
            this.compareLevel = compareLevel;
            this.joinType = joinType;
            this.sqlFolder = sqlFolder;
        }

        @Override
        public Pair<String, Throwable> call() {
            String identity = "sqlFolder:" + sqlFolder + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
            try {
                if (NExecAndComp.CompareLevel.SUBSET.equals(compareLevel)) {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql");
                    NExecAndComp.execLimitAndValidate(queries, getProject(), joinType);
                } else if (NExecAndComp.CompareLevel.SAME_SQL_COMPARE.equals(compareLevel)) {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execAndCompare(queries, getProject(), NExecAndComp.CompareLevel.SAME_SQL_COMPARE, joinType);
                } else {
                    List<Pair<String, String>> queries = NExecAndComp
                            .fetchQueries(KYLIN_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
                }
            } catch (Throwable th) {
                logger.error("Query fail on:", identity);
                return Pair.newPair(identity, th);
            }
            logger.info("Query succeed on:", identity);
            return Pair.newPair(identity, null);
        }
    }
}
