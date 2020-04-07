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

import com.google.common.collect.Maps;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderContext;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.runtime.AbstractFunction1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NFilePruningTest extends LocalWithSparkSessionTest {

    private String base = "SELECT COUNT(*)  FROM TEST_KYLIN_FACT LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

    @Before
    public void setup() throws Exception {
        super.init();
        System.setProperty("kylin.env", "UT");
        System.setProperty("kylin.query.enable-dynamic-column", "false");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        Candidate.setPriorities(priorities);
    }

    @After
    public void after() {
        DefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testNonExistTimeRange() throws Exception {
        Long start = DateFormat.stringToMillis("2023-01-01 00:00:00");
        Long end = DateFormat.stringToMillis("2025-01-01 00:00:00");
        String cubeName = "ci_left_join_cube";
        cleanupSegments(cubeName);
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));

        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());
        assertResultsAndScanFiles(base, 1);
    }

    @Test
    public void testSegPruningWithStringDate() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildMultiSegs("ci_left_join_cube");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderContext.getSparkSession());
        String no_pruning1 = "select count(*) from TEST_KYLIN_FACT";
        String no_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > '2010-01-01' and CAL_DT < '2015-01-01'";

        String seg_pruning1 = "select count(*) from TEST_KYLIN_FACT where CAL_DT < '2013-01-01'";
        String seg_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > '2013-01-01'";
        assertResultsAndScanFiles(no_pruning1, 3);
        assertResultsAndScanFiles(no_pruning2, 3);
        assertResultsAndScanFiles(seg_pruning1, 2);
        assertResultsAndScanFiles(seg_pruning2, 1);
    }

    @Test
    @Ignore("Ignore with the introduce of Parquet storage")
    public void testSegPruningWithStringTimeStamp() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildMultiSegs("ci_left_join_cube");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderContext.getSparkSession());
        String base = "select count(*)  FROM TEST_KYLIN_FACT LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String and_pruning0 = base
                + "where CAL_DT > '2011-01-01 00:00:00' and CAL_DT < '2013-01-01 00:00:00'";
        String and_pruning1 = base
                + "where CAL_DT > '2011-01-01 00:00:00' and CAL_DT = '2016-01-01 00:00:00'";

        String or_pruning0 = base
                + "where CAL_DT > '2011-01-01 00:00:00' or CAL_DT = '2016-01-01 00:00:00'";
        String or_pruning1 = base
                + "where CAL_DT < '2009-01-01 00:00:00' or CAL_DT > '2015-01-01 00:00:00'";

        String pruning0 = base + "where CAL_DT < '2009-01-01 00:00:00'";
        String pruning1 = base + "where CAL_DT <= '2009-01-01 00:00:00'";
        String pruning2 = base + "where CAL_DT >= '2015-01-01 00:00:00'";

        String not0 = base + "where CAL_DT <> '2012-01-01 00:00:00'";

        String in_pruning0 = base
                + "where CAL_DT in ('2009-01-01 00:00:00', '2008-01-01 00:00:00', '2016-01-01 00:00:00')";
        String in_pruning1 = base
                + "where CAL_DT in ('2008-01-01 00:00:00', '2016-01-01 00:00:00')";

        assertResultsAndScanFiles(base, 3);

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

    //TODO:Pruning shards not supported right now
    @Test
    @Ignore("not supported right now")
    public void testShardPruning() throws Exception {
        System.setProperty("kap.storage.columnar.shard-rowcount", "100");
        try {
            buildMultiSegs("ci_left_join_cube");

            populateSSWithCSVData(getTestConfig(), getProject(), KylinSparkEnv.getSparkSession());

            basicPruningScenario();
            pruningWithVariousTypesScenario();

        } finally {
            System.clearProperty("kap.storage.columnar.shard-rowcount");
        }
    }

    //TODO:Pruning shards not supported right now
    @Test
    @Ignore("not supported right now")
    public void testPruningWithChineseCharacter() throws Exception {
        System.setProperty("kap.storage.columnar.shard-rowcount", "1");
        try {
            fullBuildCube("ci_left_join_cube");
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
            System.clearProperty("kap.storage.columnar.shard-rowcount");
        }
    }

    private void pruningWithVariousTypesScenario() throws Exception {
        // int type is tested #basicPruningScenario

        // xx0 means can pruning, while xx1 can not.
        String bool0 = base + "where IS_EFFECTUAL = true";
        String bool1 = base + "where IS_EFFECTUAL <> true";

        String decimal0 = base + "where PRICE = 290.48";
        String decimal1 = base + "where PRICE > 290.48";

        String short0 = base + "where SLR_SEGMENT_CD = 16";
        String short1 = base + "where SLR_SEGMENT_CD > 16";

        String string0 = base + "where LSTG_FORMAT_NAME = 'Auction'";
        String string1 = base + "where LSTG_FORMAT_NAME <> 'Auction'";

        String long0 = base + "where TEST_ORDER.ORDER_ID = 2662";
        String long1 = base + "where TEST_ORDER.ORDER_ID <> 2662";

        String date0 = base + "where TEST_DATE_ENC = DATE '2011-07-10'";
        String date1 = base + "where TEST_DATE_ENC <> DATE '2011-07-10'";

        String ts0 = base + "where TEST_TIME_ENC = TIMESTAMP '2013-06-18 07:07:10'";

        String ts1 = base + "where TEST_TIME_ENC > TIMESTAMP '2013-01-01 00:00:00' "
                + "and TEST_TIME_ENC < TIMESTAMP '2015-01-01 00:00:00' "
                + "and TEST_TIME_ENC <> TIMESTAMP '2013-06-18 07:07:10'";

        assertResultsAndScanFiles(bool0, 3);
        assertResultsAndScanFiles(bool1, 11);

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
        query.add(Pair.newPair("", bool0));
        query.add(Pair.newPair("", bool1));
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

        String equality = base + "where SELLER_ID = 10000233";
        String in = base + "where SELLER_ID in (10000233,10000234,10000235)";
        String isNull = base + "where SELLER_ID is NULL";
        String and = base + "where SELLER_ID in (10000233,10000234,10000235) and SELLER_ID = 10000233 ";
        String or = base + "where SELLER_ID = 10000233 or SELLER_ID = 1 ";
        String notSupported0 = base + "where SELLER_ID <> 10000233";
        String notSupported1 = base + "where SELLER_ID > 10000233";

        assertResultsAndScanFiles(equality, 3);
        assertResultsAndScanFiles(in, 9);
        assertResultsAndScanFiles(isNull, 3);
        assertResultsAndScanFiles(and, 3);
        assertResultsAndScanFiles(or, 4);
        assertResultsAndScanFiles(notSupported0, 17);
        assertResultsAndScanFiles(notSupported1, 17);

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

    @Override
    public String getProject() {
        return "default";
    }

    private long assertResultsAndScanFiles(String sql, long numScanFiles) throws Exception {
        Dataset<Row> dataset = queryCubeAndSkipCompute(getProject(), sql);
        dataset.collect();
        long actualNum = findFileSourceScanExec(dataset.queryExecution().sparkPlan()).metrics().get("numFiles").get().value();
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
}
