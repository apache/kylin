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

package org.apache.kylin.engine.spark2.file_pruning;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.UdfManager;
import org.apache.kylin.engine.spark2.NExecAndComp;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.runtime.AbstractFunction1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NFilePruningTest extends LocalWithSparkSessionTest {

    private String SQL_BASE = "SELECT COUNT(*)  FROM TEST_KYLIN_FACT LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";
    private final static String CUBE_SHARD_BY_SELLER_ID = "file_pruning_cube";
    private final static String CUBE_PRUNER_BY_PARTITION = "file_pruning_cube2";
    protected KylinConfig config;
    protected CubeManager cubeMgr;
    protected ExecutableManager execMgr;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        KylinSparkEnv.setSparkSession(ss);
        UdfManager.create(ss);

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }

    @Before
    public void setup() throws SchedulerException {
        this.createTestMetadata("../../examples/test_case_data/file_prunning");
        System.setProperty("kylin.env", "UT");
        System.setProperty("kylin.query.enable-dynamic-column", "false");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        Candidate.setPriorities(priorities);
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("calcite.keep-in-clause", "true");
        overwriteSystemProp("kylin.metadata.distributed-lock-impl", "org.apache.kylin.engine.spark.utils.MockedDistributedLock$MockedFactory");
        DefaultScheduler scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        config = KylinConfig.getInstanceFromEnv();
        cubeMgr = CubeManager.getInstance(config);
        execMgr = ExecutableManager.getInstance(config);
    }

    @Override
    public void after() {
        System.clearProperty("kylin.env");
        System.clearProperty("kylin.query.enable-dynamic-column");
        super.after();
    }

    @Test
    public void testNonExistTimeRange() throws Exception {
        Long start = DateFormat.stringToMillis("2023-01-01 00:00:00");
        Long end = DateFormat.stringToMillis("2025-01-01 00:00:00");
        cleanupSegments(CUBE_PRUNER_BY_PARTITION);
        buildCuboid(CUBE_PRUNER_BY_PARTITION, new SegmentRange.TSRange(start, end));

        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());
        assertResultsAndScanFiles(SQL_BASE, 1);
    }

    @Test
    public void testXPartitionPruning() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildMultiSegs(CUBE_PRUNER_BY_PARTITION);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderContext.getSparkSession());
        testSegPruningWithStringDate();
        testSegPruningWithStringTimeStamp();
    }

    private void testSegPruningWithStringDate() throws Exception {
        String no_pruning1 = "select count(*) from TEST_KYLIN_FACT";
        String no_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > '2010-01-01' and CAL_DT < '2015-01-01'";

        String seg_pruning1 = "select count(*) from TEST_KYLIN_FACT where CAL_DT < '2013-01-01'";
        String seg_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > '2013-01-01'";

        assertResultsAndScanFiles(no_pruning1, 3);
        assertResultsAndScanFiles(no_pruning2, 3);
        assertResultsAndScanFiles(seg_pruning1, 2);
        assertResultsAndScanFiles(seg_pruning2, 1);
    }

    public void testSegPruningWithStringTimeStamp() throws Exception {
        String and_pruning0 = SQL_BASE
                + "where CAL_DT > '2011-01-01 00:00:00' and CAL_DT < '2013-01-01 00:00:00'";
        String and_pruning1 = SQL_BASE
                + "where CAL_DT > '2011-01-01 00:00:00' and CAL_DT = '2016-01-01 00:00:00'";

        String or_pruning0 = SQL_BASE
                + "where CAL_DT > '2011-01-01 00:00:00' or CAL_DT = '2016-01-01 00:00:00'";
        String or_pruning1 = SQL_BASE
                + "where CAL_DT < '2009-01-01 00:00:00' or CAL_DT > '2015-01-01 00:00:00'";

        String pruning0 = SQL_BASE + "where CAL_DT < '2009-01-01 00:00:00'";
        String pruning1 = SQL_BASE + "where CAL_DT <= '2009-01-01 00:00:00'";
        String pruning2 = SQL_BASE + "where CAL_DT >= '2015-01-01 00:00:00'";

        String not0 = SQL_BASE + "where CAL_DT <> '2012-01-01 00:00:00'";

        String in_pruning0 = SQL_BASE
                + "where CAL_DT in ('2009-01-01 00:00:00', '2008-01-01 00:00:00', '2016-01-01 00:00:00')";
        String in_pruning1 = SQL_BASE
                + "where CAL_DT in ('2008-01-01 00:00:00', '2016-01-01 00:00:00')";

        assertResultsAndScanFiles(SQL_BASE, 3);

        assertResultsAndScanFiles(and_pruning0, 1);
        assertResultsAndScanFiles(and_pruning1, 0);

        assertResultsAndScanFiles(or_pruning0, 2);
        assertResultsAndScanFiles(or_pruning1, 0);

        assertResultsAndScanFiles(pruning0, 0);
        assertResultsAndScanFiles(pruning1, 1);
        assertResultsAndScanFiles(pruning2, 0);

        // pruning with "not" is not supported
        assertResultsAndScanFiles(not0, 3);

        assertResultsAndScanFiles(in_pruning0, 1);
        assertResultsAndScanFiles(in_pruning1, 0);
    }

    @Override
    public String getProject() {
        return "default";
    }

    private long assertResultsAndScanFiles(String sql, long numScanFiles) throws Exception {
        Dataset<Row> dataset = queryCubeAndSkipCompute(getProject(), sql);
        dataset.collect();
        long actualNum = findFileSourceScanExec(dataset.queryExecution().executedPlan())
                .metrics().get("numFiles").get().value();
        Assert.assertEquals(numScanFiles, actualNum);
        return actualNum;
    }

    private FileSourceScanExec findFileSourceScanExec(SparkPlan plan) {
        return (FileSourceScanExec) plan.find(new AbstractFunction1<SparkPlan, Object>() {
            @Override
            public Object apply(SparkPlan p) {
                return p instanceof FileSourceScanExec;
            }
        }).get();
    }


    private static Dataset<Row> queryCubeAndSkipCompute(String prj, String sql) throws Exception {
        SparderContext.skipCompute();
        Dataset<Row> df = queryCube(prj, sql, null);
        return df;
    }

    private static Dataset<Row> queryCube(String prj, String sql, List<String> parameters) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = QueryConnection.getConnection(prj);
            stmt = conn.prepareStatement(sql);
            for (int i = 1; parameters != null && i <= parameters.size(); i++) {
                stmt.setString(i, parameters.get(i - 1).trim());
            }
            rs = stmt.executeQuery();
        } finally {
            DBUtils.closeQuietly(rs);
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
            SparderContext.cleanCompute();
        }
        return SparderContext.getDF();
    }

    @Test
    public void testSegShardPruning() throws Exception {
        System.setProperty("kylin.storage.columnar.shard-rowcount", "100");
        try {
            buildMultiSegs(CUBE_SHARD_BY_SELLER_ID);

            populateSSWithCSVData(getTestConfig(), getProject(), KylinSparkEnv.getSparkSession());

            basicPruningScenario();

            //TODO: For now not support multi shard columns in one cube
            //pruningWithVariousTypesScenario();

        } finally {
            System.clearProperty("kylin.storage.columnar.shard-rowcount");
        }
    }

    @Test
    public void testPruningWithChineseCharacter() throws Exception {
        System.setProperty("kylin.storage.columnar.shard-rowcount", "1");
        try {
            fullBuildCube("file_pruning_cube_measure");
            populateSSWithCSVData(getTestConfig(), getProject(), KylinSparkEnv.getSparkSession());

            String chinese0 = "select count(*) from TEST_MEASURE where name1 = '中国'";
            String chinese1 = "select count(*) from TEST_MEASURE where name1 <> '中国'";

            assertResultsAndScanFiles(chinese0, 1);
            assertResultsAndScanFiles(chinese1, 3);

            List<Pair<String, String>> query = new ArrayList<>();
            query.add(Pair.newPair("", chinese0));
            query.add(Pair.newPair("", chinese1));
            NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");

        } finally {
            System.clearProperty("kylin.storage.columnar.shard-rowcount");
        }
    }

    private void pruningWithVariousTypesScenario() throws Exception {
        // int type is tested #basicPruningScenario

        String decimal0 = SQL_BASE + "where PRICE = 290.48";
        String decimal1 = SQL_BASE + "where PRICE > 290.48";

        String short0 = SQL_BASE + "where SLR_SEGMENT_CD = 16";
        String short1 = SQL_BASE + "where SLR_SEGMENT_CD > 16";

        String string0 = SQL_BASE + "where LSTG_FORMAT_NAME = 'Auction'";
        String string1 = SQL_BASE + "where LSTG_FORMAT_NAME <> 'Auction'";

        String long0 = SQL_BASE + "where TEST_ORDER.ORDER_ID = 2662";
        String long1 = SQL_BASE + "where TEST_ORDER.ORDER_ID <> 2662";

        String date0 = SQL_BASE + "where TEST_DATE_ENC = DATE '2011-07-10'";
        String date1 = SQL_BASE + "where TEST_DATE_ENC <> DATE '2011-07-10'";

        String ts0 = SQL_BASE + "where TEST_TIME_ENC = TIMESTAMP '2013-06-18 07:07:10'";

        String ts1 = SQL_BASE + "where TEST_TIME_ENC > TIMESTAMP '2013-01-01 00:00:00' "
                + "and TEST_TIME_ENC < TIMESTAMP '2015-01-01 00:00:00' "
                + "and TEST_TIME_ENC <> TIMESTAMP '2013-06-18 07:07:10'";

        assertResultsAndScanFiles(decimal0, 3);
        assertResultsAndScanFiles(decimal1, 52);

        // calcite will treat short as int. So pruning will not work.
        assertResultsAndScanFiles(short0, 25);
        assertResultsAndScanFiles(short1, 25);

        assertResultsAndScanFiles(string0, 3);
        assertResultsAndScanFiles(string1, 12);

        assertResultsAndScanFiles(long0, 3);
        assertResultsAndScanFiles(long1, 28);

        assertResultsAndScanFiles(date0, 3);
        assertResultsAndScanFiles(date1, 19);

        // segment pruning first, then shard pruning
        // so the scanned files is 1 not 3(each segment per shard)
        assertResultsAndScanFiles(ts0, 1);
        assertResultsAndScanFiles(ts1, 11);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", decimal0));
        query.add(Pair.newPair("", decimal1));
        query.add(Pair.newPair("", short0));
        query.add(Pair.newPair("", short1));
        query.add(Pair.newPair("", string0));
        query.add(Pair.newPair("", string1));
        query.add(Pair.newPair("", long0));
        query.add(Pair.newPair("", long1));
        query.add(Pair.newPair("", date0));
        query.add(Pair.newPair("", date1));

        // see #11598
        query.add(Pair.newPair("", ts0));
        query.add(Pair.newPair("", ts1));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

    private void basicPruningScenario() throws Exception {
        // shard pruning supports: Equality/In/IsNull/And/Or
        // other expression(gt/lt/like/cast/substr, etc.) will select all files.

        String equality = SQL_BASE + "where SELLER_ID = 10000233";
        String in = SQL_BASE + "where SELLER_ID in (10000233,10000234,10000235)";
        String isNull = SQL_BASE + "where SELLER_ID is NULL";
        String and = SQL_BASE + "where SELLER_ID in (10000233,10000234,10000235) and SELLER_ID = 10000233 ";
        String or = SQL_BASE + "where SELLER_ID = 10000233 or SELLER_ID = 2 ";
        String notSupported0 = SQL_BASE + "where SELLER_ID <> 10000233";
        String notSupported1 = SQL_BASE + "where SELLER_ID > 10000233";

        assertResultsAndScanFiles(equality, 3);
        assertResultsAndScanFiles(in, 7);
        assertResultsAndScanFiles(isNull, 3);
        assertResultsAndScanFiles(and, 3);
        assertResultsAndScanFiles(or, 5);
        assertResultsAndScanFiles(notSupported0, 13);
        assertResultsAndScanFiles(notSupported1, 13);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", equality));
        query.add(Pair.newPair("", in));
        query.add(Pair.newPair("", isNull));
        query.add(Pair.newPair("", and));
        query.add(Pair.newPair("", or));
        query.add(Pair.newPair("", notSupported0));
        query.add(Pair.newPair("", notSupported1));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");
    }

}
