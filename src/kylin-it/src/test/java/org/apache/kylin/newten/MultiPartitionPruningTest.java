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


package org.apache.kylin.newten;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.KylinFileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;
import scala.runtime.AbstractFunction1;

@RunWith(TimeZoneTestRunner.class)
public class MultiPartitionPruningTest extends NLocalWithSparkSessionTest implements AdaptiveSparkPlanHelper {
    private final String sql = "select count(*) from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id ";

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "true");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition_pruning");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "multi_partition_pruning";
    }

    @Test
    public void testPartitionPruningVarchar() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c78";

        // segment1 [2009-01-01, 2011-01-01] partition value Others, ABIN, FP-non GTC
        // segment2 [2011-01-01, 2013-01-01] partition value Others, ABIN
        // segment3 [2013-01-01, 2015-01-01] partition value Others, ABIN, FP-GTC
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 2L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 3L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");
        val expectedPartitions = Lists.<List<Long>> newArrayList();

        val noPartitionFilterSql = sql + "where cal_dt between '2008-01-01' and '2012-01-01' ";
        val andSql = sql + "where cal_dt between '2009-01-01' and '2012-01-01' and lstg_format_name = 'ABIN' ";
        val notInSql = sql
                + "where cal_dt > '2009-01-01' and cal_dt < '2012-01-01' and lstg_format_name not in ('ABIN', 'FP-non GTC', 'FP-GTC', 'Auction')";
        val emptyResultSql = sql
                + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_format_name = 'NOT-EXIST-VALUE' ";
        val pushdownSql = sql
                + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_format_name = 'FP-GTC' ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 3L));
        assertResultsAndScanFiles(dfName, sql, 5, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, andSql, 1, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(0L));
        expectedPartitions.add(Lists.newArrayList(0L));
        assertResultsAndScanFiles(dfName, notInSql, 1, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", notInSql));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningInteger() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c76";

        // segment1 [2009-01-01, 2011-01-01] partition value 0, 2, 3
        // segment2 [2011-01-01, 2013-01-01] partition value 0, 2
        // segment3 [2013-01-01, 2015-01-01] partition value 0, 2, 15
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 2L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L, 3L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");
        val expectedPartitions = Lists.<List<Long>> newArrayList();

        val noPartitionFilterSql = sql + "where cal_dt between '2008-01-01' and '2012-01-01' ";
        val andSql = sql + "where cal_dt between '2009-01-01' and '2012-01-01' and lstg_site_id = 2 ";
        val notInSql = sql
                + "where cal_dt > '2009-01-01' and cal_dt < '2012-01-01' and lstg_site_id not in (2, 3, 15, 23)";
        val emptyResultSql = sql + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_site_id = 10000 ";
        val pushdownSql = sql + "where cal_dt > '2012-01-01' and cal_dt < '2014-01-01' and lstg_site_id = 15 ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 3L));
        assertResultsAndScanFiles(dfName, sql, 5, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, andSql, 1, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(0L));
        expectedPartitions.add(Lists.newArrayList(0L));
        assertResultsAndScanFiles(dfName, notInSql, 1, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", notInSql));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningDate() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";

        // segment1 [2009-01-01, 2011-01-01] partition value 2010-01-01, 2011-01-01
        // segment2 [2011-01-01, 2013-01-01] partition value 2011-01-01, 2012-01-01, 2013-01-01
        // segment3 [2013-01-01, 2015-01-01] partition value 2012-01-01, 2013-01-01, 2014-01-01
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(1L, 2L, 3L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(2L, 3L, 4L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01 00:00:00", "2011-01-01 00:00:00");
        val segmentRange2 = Pair.newPair("2011-01-01 00:00:00", "2013-01-01 00:00:00");
        val segmentRange3 = Pair.newPair("2013-01-01 00:00:00", "2015-01-01 00:00:00");
        val expectedPartitions = Lists.<List<Long>> newArrayList();

        val baseSql = "select count(*) from test_order left join test_kylin_fact on test_order.order_id = test_kylin_fact.order_id ";
        val noPartitionFilterSql = baseSql
                + "where test_time_enc between '2010-01-01 00:00:00' and '2012-01-01 00:00:00' ";
        val andSql = baseSql
                + "where test_time_enc > '2012-01-01 00:00:00' and test_time_enc < '2014-01-01 00:00:00' and test_date_enc = '2013-01-01' ";
        val inSql = baseSql
                + "where test_time_enc > '2012-01-01 00:00:00' and test_time_enc < '2014-01-01 00:00:00' and test_date_enc in ('2012-01-01', '2013-01-01') ";
        val notInSql = baseSql
                + "where test_time_enc between '2009-01-01 00:00:00' and '2011-01-01 00:00:00' and test_date_enc not in ('2010-01-01', '2012-01-01', '2013-01-01', '2014-01-01') ";
        val emptyResultSql = baseSql
                + "where test_time_enc between '2009-01-01 00:00:00' and '2011-01-01 00:00:00' and test_date_enc = '2020-01-01' ";
        val pushdownSql = baseSql
                + "where test_time_enc between '2011-01-01 00:00:00' and '2015-01-01 00:00:00' and test_date_enc = '2011-01-01' ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L, 4L));
        assertResultsAndScanFiles(dfName, baseSql, 8, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 5, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(3L));
        expectedPartitions.add(Lists.newArrayList(3L));
        assertResultsAndScanFiles(dfName, andSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        assertResultsAndScanFiles(dfName, inSql, 4, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, notInSql, 2, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", inSql));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningTimestamp() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c79";

        // segment1 [2009-01-01, 2011-01-01] partition value 2010-01-01 00:56:38, 2010-01-01 04:03:59
        // segment2 [2011-01-01, 2013-01-01] partition value 2010-01-01 04:03:59, 2010-01-01 08:16:36, 2010-01-02 14:24:50
        // segment3 [2013-01-01, 2015-01-01] partition value 2010-01-01 08:16:36, 2010-01-02 14:24:50, 2010-01-03 05:15:09
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(0L, 1L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(1L, 2L, 3L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2015-01-01 00:00:00",
                Lists.newArrayList(10001L), Lists.newArrayList(2L, 3L, 4L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");
        val expectedPartitions = Lists.<List<Long>> newArrayList();

        val baseSql = "select count(*) from test_order left join test_kylin_fact on test_order.order_id = test_kylin_fact.order_id ";
        val noPartitionFilterSql = baseSql + "where test_date_enc between '2010-01-01' and '2012-01-01' ";
        val andSql = baseSql
                + "where test_date_enc > '2012-01-01' and test_date_enc < '2014-01-01' and test_time_enc = '2010-01-02 14:24:50' ";
        val inSql = baseSql
                + "where test_date_enc > '2012-01-01' and test_date_enc < '2014-01-01' and test_time_enc in ('2010-01-01 08:16:36', '2010-01-02 14:24:50') ";
        val notInSql = baseSql
                + "where test_date_enc between '2009-01-01' and '2011-01-01' and test_time_enc not in ('2010-01-01 00:56:38', '2010-01-01 08:16:36', '2010-01-02 14:24:50', '2010-01-03 05:15:09') ";
        val emptyResultSql = baseSql
                + "where test_date_enc between '2009-01-01' and '2011-01-01' and test_time_enc = '2020-01-01 00:00:00' ";
        val pushdownSql = baseSql
                + "where test_date_enc between '2011-01-01' and '2015-01-01' and test_time_enc = '2010-01-01 04:03:59' ";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L, 4L));
        assertResultsAndScanFiles(dfName, baseSql, 8, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L, 3L));
        assertResultsAndScanFiles(dfName, noPartitionFilterSql, 5, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        expectedPartitions.add(Lists.newArrayList(3L));
        expectedPartitions.add(Lists.newArrayList(3L));
        assertResultsAndScanFiles(dfName, andSql, 2, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        expectedPartitions.add(Lists.newArrayList(2L, 3L));
        assertResultsAndScanFiles(dfName, inSql, 4, false, expectedRanges, expectedPartitions);

        expectedRanges.clear();
        expectedPartitions.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(1L));
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, notInSql, 2, false, expectedRanges, expectedPartitions);

        assertResultsAndScanFiles(dfName, emptyResultSql, 0, true, null, null);

        try {
            assertResultsAndScanFiles(dfName, pushdownSql, 0, false, null, null);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NoRealizationFoundException);
        }

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", andSql));
        query.add(Pair.newPair("", inSql));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testPartitionPruningChinese() throws Exception {
        val dfName = "9cde9d25-9334-4b92-b229-a00f49453757";

        // segment1 [2012-01-01, 2013-01-01] partition value FT, 中国
        // segment2 [2013-01-01, 2014-01-01] partition value 中国
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2012-01-01 00:00:00", "2013-01-01 00:00:00",
                Lists.newArrayList(100001L), Lists.newArrayList(0L, 1L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2013-01-01 00:00:00", "2014-01-01 00:00:00",
                Lists.newArrayList(100001L), Lists.newArrayList(0L, 1L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2012-01-01", "2013-01-01");
        val segmentRange2 = Pair.newPair("2013-01-01", "2014-01-01");
        val expectedPartitions = Lists.<List<Long>> newArrayList();

        val chineseSql = "select count(*), time1 from test_measure where time1 > '2012-01-01' and time1 < '2013-01-01' and name1 = '中国' group by time1";

        expectedRanges.add(segmentRange1);
        expectedPartitions.add(Lists.newArrayList(1L));
        assertResultsAndScanFiles(dfName, chineseSql, 0, false, expectedRanges, expectedPartitions);

        val queries = Lists.<Pair<String, String>> newArrayList();
        queries.add(Pair.newPair("", chineseSql));
        ExecAndComp.execAndCompare(queries, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testExactlyMatch() throws Exception {
        val dfName = "8c670664-8d05-466a-802f-83c023b56c80";

        // segment1 [2009-01-01, 2011-01-01] build all partitions
        // segment2 [2011-01-01, 2013-01-01] build all partitions
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2009-01-01 00:00:00", "2011-01-01 00:00:00",
                Lists.newArrayList(10001L, 11001L), Lists.newArrayList(0L, 1L, 2L, 3L, 4L));
        indexDataConstructor.buildMultiSegmentPartitions(dfName, "2011-01-01 00:00:00", "2013-01-01 00:00:00",
                Lists.newArrayList(10001L, 11001L), Lists.newArrayList(0L, 1L, 2L, 3L, 4L));

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val expectedPartitions = Lists.<List<Long>> newArrayList();

        val sql1 = "select\n" + "  count(*), cal_dt\n" + "from\n" + "  test_kylin_fact\n"
                + "  left join test_order on test_kylin_fact.order_id = test_order.order_id\n" + "where\n"
                + "  cal_dt between '2009-01-01'\n" + "  and '2012-01-01'\n" + "group by\n" + "  cal_dt\n"
                + "order by\n" + "  cal_dt\n";
        val sql2 = "select\n" + "  count(*), cal_dt\n" + "from\n" + "  test_kylin_fact\n"
                + "  left join test_order on test_kylin_fact.order_id = test_order.order_id\n" + "where\n"
                + "  cal_dt between '2009-01-01'\n"
                + "  and '2012-01-01' and lstg_format_name in ('ABIN', 'FP-non GTC') \n" + "group by\n"
                + "  cal_dt, lstg_format_name\n" + "order by\n" + "  cal_dt\n";

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L, 3L, 4L));
        expectedPartitions.add(Lists.newArrayList(0L, 1L, 2L, 3L, 4L));
        assertResultsAndScanFiles(dfName, sql1, 5, false, expectedRanges, expectedPartitions);

        expectedPartitions.clear();
        expectedPartitions.add(Lists.newArrayList(1L, 2L));
        expectedPartitions.add(Lists.newArrayList(1L, 2L));
        assertResultsAndScanFiles(dfName, sql2, 2, false, expectedRanges, expectedPartitions);

        val queries = Lists.<Pair<String, String>> newArrayList();
        queries.add(Pair.newPair("", sql1));
        queries.add(Pair.newPair("", sql2));
        ExecAndComp.execAndCompare(queries, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    private long assertResultsAndScanFiles(String modelId, String sql, long numScanFiles, boolean emptyLayout,
            List<Pair<String, String>> expectedRanges, List<List<Long>> expectedPartitions) throws Exception {
        val df = ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        val context = ContextUtil.listContexts().get(0);
        if (emptyLayout) {
            Assert.assertTrue(context.storageContext.isEmptyLayout());
            Assert.assertEquals(Long.valueOf(-1), context.storageContext.getLayoutId());
            return numScanFiles;
        }
        df.collect();
        val actualNum = findFileSourceScanExec(df.queryExecution().executedPlan()).metrics().get("numFiles").get()
                .value();
        Assert.assertEquals(numScanFiles, actualNum);
        val segmentIds = context.storageContext.getPrunedSegments();
        val partitions = context.storageContext.getPrunedPartitions();
        assertPrunedSegmentRange(modelId, segmentIds, partitions, expectedRanges, expectedPartitions);
        return actualNum;
    }

    private KylinFileSourceScanExec findFileSourceScanExec(SparkPlan plan) {
        return (KylinFileSourceScanExec) find(plan, new AbstractFunction1<SparkPlan, Object>() {
            @Override
            public Object apply(SparkPlan v1) {
                return v1 instanceof KylinFileSourceScanExec;
            }
        }).get();
    }

    private void assertPrunedSegmentRange(String dfId, List<NDataSegment> prunedSegments,
            Map<String, List<Long>> prunedPartitions, List<Pair<String, String>> expectedRanges,
            List<List<Long>> expectedPartitions) {
        val model = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();

        if (CollectionUtils.isEmpty(expectedRanges)) {
            return;
        }
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        Assert.assertEquals(expectedPartitions.size(), prunedSegments.size());
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);

            val actualPartitions = prunedPartitions.get(segment.getId());
            Assert.assertEquals(expectedPartitions.get(i), actualPartitions);
        }
    }
}
