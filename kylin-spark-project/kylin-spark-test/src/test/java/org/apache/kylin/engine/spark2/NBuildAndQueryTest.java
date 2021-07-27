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
import org.apache.kylin.common.util.Quadruple;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.kylin.engine.spark2.NExecAndComp.CompareLevel;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class NBuildAndQueryTest extends LocalWithSparkSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(NBuildAndQueryTest.class);

    private boolean succeed = true;
    protected KylinConfig config;
    protected CubeManager cubeMgr;
    protected ExecutableManager execMgr;

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        overwriteSystemProp("kylin.env", "UT");
        overwriteSystemProp("isDeveloperMode", "true");
        overwriteSystemProp("kylin.query.enable-dynamic-column", "false");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        Candidate.setPriorities(priorities);
        config = KylinConfig.getInstanceFromEnv();
        cubeMgr = CubeManager.getInstance(config);
        execMgr = ExecutableManager.getInstance(config);
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    @Ignore("Manually verify for developer if `examples/test_metadata` exists.")
    public void manualVerifyForDeveloper() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());
        List<Pair<String, Throwable>> results = execAndGetResults(
                Lists.newArrayList(new QueryCallable(CompareLevel.SAME, "left", "temp"))); //
        report(results);
    }

    @Test
    public void verifySqlStandard() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        // 1. Kylin side
        buildCubes();

        // 2. Spark side
        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());

        // 3. Compare Kylin with Spark
        List<QueryCallable> tasks = prepareAndGenQueryTasks();
        List<Pair<String, Throwable>> results = execAndGetResults(tasks);
        Assert.assertEquals(results.size(), tasks.size());
        report(results);
    }

    @Test
    public void exactlyMatchCuboidMultiSegmentTest() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        buildSegments("ci_left_join_cube", new SegmentRange.TSRange(dateToLong("2012-01-01"), dateToLong("2013-01-01")),
                new SegmentRange.TSRange(dateToLong("2013-01-01"), dateToLong("2015-01-01")));

        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());

        List<QueryCallable> tasks = new ArrayList<>();
        tasks.add(new QueryCallable(CompareLevel.SAME, "left", "sql_exactly_agg_multi_segment"));
        List<Pair<String, Throwable>> results = execAndGetResults(tasks);
        Assert.assertEquals(results.size(), tasks.size());
        report(results);
    }

    private List<Pair<String, Throwable>> execAndGetResults(List<QueryCallable> tasks)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(9//
                , 9
                , 1
                , TimeUnit.DAYS
                , new LinkedBlockingQueue<>(100));
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
        if (Boolean.parseBoolean(System.getProperty("failFast", "false")) && result.getSecond() != null) {
            logger.error("CI failed on:" + result.getFirst());
            Assert.fail();
        }
    }

    private List<QueryCallable> prepareAndGenQueryTasks() throws Exception {
        String[] joinTypes = new String[] {"left"};
        List<QueryCallable> tasks = new ArrayList<>();
        for (String joinType : joinTypes) {
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_cache"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_casewhen"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_castprunesegs"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_datetime"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_derived"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_dict_enumerator"));
            // HLL is not precise
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct_dim"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_distinct_precisely"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType,
                    "sql_distinct_precisely_rollup"));
            // Supports to use dynamic parameters,
            // but now only supports string type for querying from SparkSQL
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_dynamic"));

            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_exactly_agg"));

            // Not support yet
            //tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_expression"));

            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_function"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_grouping"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_h2"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_hive"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_intersect_count"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_intersect_value"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_join"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_like"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_lookup"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_multi_model"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_orderby"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_ordinal"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_percentile"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_plan"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_snowflake"));

            // Not support yet
            //tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_streaming"));
            //tasks.add(new QueryCallable(CompareLevel.NONE, joinType, "sql_streaming_v2"));

            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_subquery"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_tableau"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_timeout"));

            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_timestamp"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_topn"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_union"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_unionall"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_values"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_window"));

            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_limit"));
            tasks.add(new QueryCallable(CompareLevel.SAME, joinType, "sql_prune_segment"));
        }
        logger.info("Total {} tasks.", tasks.size());
        return tasks;
    }

    public void buildCubes() throws Exception {
        logger.debug("Prepare Kylin data.");
        if (Boolean.parseBoolean(System.getProperty("noBuild", "false"))) {
            logger.debug("Query prebuilt cube.");
        } else if (Boolean.parseBoolean(System.getProperty("isDeveloperMode", "false"))) {
            //fullBuildCube("ci_inner_join_cube");
            fullBuildCube("ci_left_join_cube");
            buildSegments("ssb", new SegmentRange.TSRange(dateToLong("1992-09-04"), dateToLong("1992-09-05")),
                    new SegmentRange.TSRange(dateToLong("1992-09-05"), dateToLong("1992-09-06")),
                    new SegmentRange.TSRange(dateToLong("1992-09-06"), dateToLong("1992-09-07")),
                    new SegmentRange.TSRange(dateToLong("1992-09-07"), dateToLong("1992-09-08")));
        } else {
            //buildAndMergeCube("ci_inner_join_cube");
            buildAndMergeCube("ci_left_join_cube");
        }
    }

    private void buildAndMergeCube(String cubeName) throws Exception {
        if (cubeName.equals("ci_inner_join_cube")) {
            buildFourSegmentAndMerge(cubeName);
        }
        if (cubeName.equals("ssb")) {
            buildSegments(cubeName, new SegmentRange.TSRange(dateToLong("1992-0-01"), dateToLong("2015-01-01")));
        }
        if (cubeName.equals("ci_left_join_cube")) {
            buildTwoSegmentAndMerge(cubeName);
        }
    }

    private void buildTwoSegmentAndMerge(String cubeName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        cleanupSegments(cubeName);

        // Round 1: Build 2 segment
        ExecutableState state;
        state = buildCuboid(cubeName, new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2012-01-01")));
        Assert.assertEquals(ExecutableState.SUCCEED, state);
        if (cubeName.equals("ci_left_join_cube")) {
            CubeSegment segment1 = cubeMgr.reloadCube(cubeName).getSegments().get(0);

            Assert.assertEquals(0, segment1.getInputRecords());
            Assert.assertEquals(0, segment1.getInputRecordsSize());
            Assert.assertEquals(0, segment1.getSizeKB());
            Assert.assertEquals(17, segment1.getCuboidShardNums().size());
        }

        state = buildCuboid(cubeName, new SegmentRange.TSRange(dateToLong("2012-01-01"), dateToLong("2015-01-01")));
        Assert.assertEquals(ExecutableState.SUCCEED, state);
        if (cubeName.equals("ci_left_join_cube")) {
            CubeSegment segment2 = cubeMgr.reloadCube(cubeName).getSegments().get(1);
            Assert.assertEquals(10000, segment2.getInputRecords());
            Assert.assertEquals(2103495, segment2.getInputRecordsSize());
            Assert.assertTrue(segment2.getSizeKB() > 0);
            Assert.assertEquals(17, segment2.getCuboidShardNums().size());
            Assert.assertEquals(leftJoinCubeCuboidShardNums(), segment2.getCuboidShardNums());
        }


        // Round 2: Merge two segments
        state = mergeSegments(cubeName, dateToLong("2010-01-01"), dateToLong("2015-01-01"), true);
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        // validate cube segment info
        CubeSegment firstSegment = cubeMgr.reloadCube(cubeName).getSegments().get(0);
        if (cubeName.equals("ci_left_join_cube")) {
            Assert.assertEquals(10000, firstSegment.getInputRecords());
            Assert.assertEquals(2103495, firstSegment.getInputRecordsSize());
            Assert.assertTrue(firstSegment.getSizeKB() > 0);
            Assert.assertEquals(17, firstSegment.getCuboidShardNums().size());
            Assert.assertEquals(leftJoinCubeCuboidShardNums(), firstSegment.getCuboidShardNums());
        }

        Assert.assertEquals(new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2015-01-01")),
                firstSegment.getSegRange());
    }

    private void buildFourSegmentAndMerge(String cubeName) throws Exception {
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        cleanupSegments(cubeName);

        // Round 1: Build 4 segment
        ExecutableState state;
        buildSegments(cubeName, new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2012-06-01")),
                new SegmentRange.TSRange(dateToLong("2012-06-01"), dateToLong("2013-01-01")),
                new SegmentRange.TSRange(dateToLong("2013-01-01"), dateToLong("2013-06-01")),
                new SegmentRange.TSRange(dateToLong("2013-06-01"), dateToLong("2015-01-01")));

        // Round 2: Merge two segments
        state = mergeSegments(cubeName, dateToLong("2010-01-01"), dateToLong("2013-01-01"), false);
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        state = mergeSegments(cubeName, dateToLong("2013-01-01"), dateToLong("2015-01-01"), false);
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        // validate cube segment info
        CubeSegment firstSegment = cubeMgr.reloadCube(cubeName).getSegments().get(0);
        CubeSegment secondSegment = cubeMgr.reloadCube(cubeName).getSegments().get(1);

        Assert.assertEquals(new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2013-01-01")),
                firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TSRange(dateToLong("2013-01-01"), dateToLong("2015-01-01")),
                secondSegment.getSegRange());
    }

    public void buildSegments(String cubeName, SegmentRange.TSRange ... toBuildRanges) throws Exception{
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        cleanupSegments(cubeName);

        ExecutableState state;
        for (SegmentRange.TSRange toBuildRange : toBuildRanges) {
            state = buildCuboid(cubeName, toBuildRange);
            Assert.assertEquals(ExecutableState.SUCCEED, state);
        }
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
                } else {
                    List<Quadruple<String, String, NExecAndComp.ITQueryMetrics, List<String>>> queries =
                            NExecAndComp.fetchQueries2(KYLIN_SQL_BASE_DIR + File.separator + sqlFolder);
                    NExecAndComp.execAndCompareNew2(queries, getProject(), compareLevel, joinType,
                            null, sqlFolder);
                }
            } catch (Throwable th) {
                logger.error("Query fail on: {}", identity);
                return Pair.newPair(identity, th);
            }
            logger.info("Query succeed on: {}", identity);
            return Pair.newPair(identity, null);
        }
    }
}
