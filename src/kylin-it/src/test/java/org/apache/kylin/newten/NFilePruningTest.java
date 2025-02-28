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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataSegmentManager;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.KylinFileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sparkproject.guava.collect.Sets;

import lombok.val;
import scala.runtime.AbstractFunction1;

@RunWith(TimeZoneTestRunner.class)
public class NFilePruningTest extends NLocalWithSparkSessionTest implements AdaptiveSparkPlanHelper {

    private final String base = "select count(*)  FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

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
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/ut_meta/file_pruning");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    public String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/file_pruning" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testNonExistTimeRangeExcludeEmpty() throws Exception {
        val start = SegmentRange.dateToLong("2023-01-01 00:00:00");
        val end = SegmentRange.dateToLong("2025-01-01 00:00:00");
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
        assertResultsAndScanFiles(dfName, base, 0, false, Lists.newArrayList());
    }

    @Test
    public void testNonExistTimeRangeIncludeEmpty() throws Exception {
        overwriteSystemProp("kylin.query.skip-empty-segments", "false");
        val start = SegmentRange.dateToLong("2023-01-01 00:00:00");
        val end = SegmentRange.dateToLong("2025-01-01 00:00:00");
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
        assertResultsAndScanFiles(dfName, base, 1, false, Lists.newArrayList());
    }

    @Test
    public void testExistTimeRangeExcludeEmpty() throws Exception {
        val start = SegmentRange.dateToLong("2013-01-01 00:00:00");
        val end = SegmentRange.dateToLong("2025-01-01 00:00:00");
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
        assertResultsAndScanFiles(dfName, base, 1, false, Lists.newArrayList());
    }

    @Test
    public void testSegPruningWithTimeStamp() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        val dfId = "8c670664-8d05-466a-802f-83c023b56c77";
        buildMultiSegs(dfId, 10001);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        String and_pruning0 = base
                + "where TEST_TIME_ENC > TIMESTAMP '2011-01-01 00:00:00' and TEST_TIME_ENC < TIMESTAMP '2013-01-01 00:00:00'";
        String and_pruning1 = base
                + "where TEST_TIME_ENC > TIMESTAMP '2011-01-01 00:00:00' and TEST_TIME_ENC = TIMESTAMP '2016-01-01 00:00:00'";

        String or_pruning0 = base
                + "where TEST_TIME_ENC > TIMESTAMP '2011-01-01 00:00:00' or TEST_TIME_ENC = TIMESTAMP '2016-01-01 00:00:00'";
        String or_pruning1 = base
                + "where TEST_TIME_ENC < TIMESTAMP '2009-01-01 00:00:00' or TEST_TIME_ENC > TIMESTAMP '2015-01-01 00:00:00'";

        String pruning0 = base + "where TEST_TIME_ENC < TIMESTAMP '2009-01-01 00:00:00'";
        String pruning1 = base + "where TEST_TIME_ENC <= TIMESTAMP '2009-01-01 00:00:00'";
        String pruning2 = base + "where TEST_TIME_ENC >= TIMESTAMP '2015-01-01 00:00:00'";

        String not_equal0 = base + "where TEST_TIME_ENC <> TIMESTAMP '2012-01-01 00:00:00'";

        String not0 = base
                + "where not (TEST_TIME_ENC < TIMESTAMP '2011-01-01 00:00:00' or TEST_TIME_ENC >= TIMESTAMP '2013-01-01 00:00:00')";

        String in_pruning0 = base
                + "where TEST_TIME_ENC in (TIMESTAMP '2009-01-01 00:00:00',TIMESTAMP '2008-01-01 00:00:00',TIMESTAMP '2016-01-01 00:00:00')";
        String in_pruning1 = base
                + "where TEST_TIME_ENC in (TIMESTAMP '2008-01-01 00:00:00',TIMESTAMP '2016-01-01 00:00:00')";

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01 00:00:00", "2011-01-01 00:00:00");
        val segmentRange2 = Pair.newPair("2011-01-01 00:00:00", "2013-01-01 00:00:00");
        val segmentRange3 = Pair.newPair("2013-01-01 00:00:00", "2015-01-01 00:00:00");

        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(dfId, base, 3, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(dfId, and_pruning0, 1, false, expectedRanges);
        expectedRanges.clear();
        assertResultsAndScanFiles(dfId, and_pruning1, 0, true, expectedRanges);

        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(dfId, or_pruning0, 2, false, expectedRanges);
        expectedRanges.clear();
        assertResultsAndScanFiles(dfId, or_pruning1, 0, true, expectedRanges);

        assertResultsAndScanFiles(dfId, pruning0, 0, true, expectedRanges);
        expectedRanges.add(segmentRange1);
        assertResultsAndScanFiles(dfId, pruning1, 1, false, expectedRanges);
        expectedRanges.clear();
        assertResultsAndScanFiles(dfId, pruning2, 0, true, expectedRanges);

        // pruning with "not equal" is not supported
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(dfId, not_equal0, 3, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(dfId, not0, 1, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        assertResultsAndScanFiles(dfId, in_pruning0, 1, false, expectedRanges);
        assertResultsAndScanFiles(dfId, in_pruning1, 0, true, expectedRanges);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("base", base));
        query.add(Pair.newPair("and_pruning0", and_pruning0));
        query.add(Pair.newPair("or_pruning0", or_pruning0));
        query.add(Pair.newPair("pruning1", pruning1));
        query.add(Pair.newPair("not_equal0", not_equal0));
        query.add(Pair.newPair("not0", not0));
        query.add(Pair.newPair("in_pruning0", in_pruning0));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    @Test
    public void testShardPruning() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "100");

        val dfId = "8c670664-8d05-466a-802f-83c023b56c77";
        buildMultiSegs(dfId);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        basicPruningScenario(dfId);
        pruningWithVariousTypesScenario(dfId);
    }

    @Test
    public void testPruningWithChineseCharacter() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "1");
        val dfId = "9cde9d25-9334-4b92-b229-a00f49453757";
        fullBuild(dfId);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val chinese0 = "select count(*) from TEST_MEASURE where name1 = '中国'";
        val chinese1 = "select count(*) from TEST_MEASURE where name1 <> '中国'";

        assertResultsAndScanFiles(dfId, chinese0, 1, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, chinese1, 4, false, Lists.newArrayList());

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", chinese0));
        query.add(Pair.newPair("", chinese1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    private void pruningWithVariousTypesScenario(String dfId) throws Exception {
        // int type is tested #basicPruningScenario

        // xx0 means can pruning, while xx1 can not.
        val bool0 = base + "where IS_EFFECTUAL = true";
        val bool1 = base + "where IS_EFFECTUAL <> true";

        val decimal0 = base + "where PRICE = 290.48";
        val decimal1 = base + "where PRICE > 290.48";

        val short0 = base + "where SLR_SEGMENT_CD = 16";
        val short1 = base + "where SLR_SEGMENT_CD > 16";

        val string0 = base + "where LSTG_FORMAT_NAME = 'Auction'";
        val string1 = base + "where LSTG_FORMAT_NAME <> 'Auction'";

        val long0 = base + "where TEST_ORDER.ORDER_ID = 2662";
        val long1 = base + "where TEST_ORDER.ORDER_ID <> 2662";

        val date0 = base + "where TEST_DATE_ENC = DATE '2011-07-10'";
        val date1 = base + "where TEST_DATE_ENC <> DATE '2011-07-10'";

        val ts0 = base + "where TEST_TIME_ENC = TIMESTAMP '2013-06-18 07:07:10'";

        val ts1 = base + "where TEST_TIME_ENC > TIMESTAMP '2013-01-01 00:00:00' "
                + "and TEST_TIME_ENC < TIMESTAMP '2015-01-01 00:00:00' "
                + "and TEST_TIME_ENC <> TIMESTAMP '2013-06-18 07:07:10'";

        assertResultsAndScanFiles(dfId, bool0, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, bool1, 11, false, Lists.newArrayList());

        assertResultsAndScanFiles(dfId, decimal0, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, decimal1, 52, false, Lists.newArrayList());

        // calcite will treat short as int. So pruning will not work.
        assertResultsAndScanFiles(dfId, short0, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, short1, 25, false, Lists.newArrayList());

        assertResultsAndScanFiles(dfId, string0, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, string1, 12, false, Lists.newArrayList());

        assertResultsAndScanFiles(dfId, long0, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, long1, 28, false, Lists.newArrayList());

        assertResultsAndScanFiles(dfId, date0, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, date1, 19, false, Lists.newArrayList());

        // segment pruning first, then shard pruning
        // so the scanned files is 1 not 3(each segment per shard)
        assertResultsAndScanFiles(dfId, ts0, 1, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, ts1, 11, false, Lists.newArrayList());

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
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    @Ignore("TODO: remove or adapt")
    public void testSegmentPruningDate() throws Exception {
        val modelId = "8c670664-8d05-466a-802f-83c023b56c80";
        buildMultiSegs(modelId, 10005);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        val sql = "select test_date_enc, count(*) FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        val and_pruning0 = sql
                + "where test_date_enc > (Date '2011-01-01') and test_date_enc < (Date '2012-01-01') group by test_date_enc";
        val and_pruning1 = sql
                + "where test_date_enc > '2011-01-01' and test_date_enc < '2012-01-01' group by test_date_enc";

        val or_pruning0 = sql
                + "where test_date_enc > '2012-01-01' or test_date_enc = '2008-01-01' group by test_date_enc";
        val or_pruning1 = sql
                + "where test_date_enc < '2011-01-01' or test_date_enc > '2013-01-01' group by test_date_enc";

        val pruning0 = sql + "where test_date_enc > '2020-01-01' group by test_date_enc";
        val pruning1 = sql + "where test_date_enc < '2008-01-01' group by test_date_enc";
        val pruning2 = sql + "where test_date_enc = '2012-01-01' group by test_date_enc";

        val not_pruning0 = sql
                + "where not (test_date_enc < '2011-01-01' or test_date_enc >= '2013-01-01') group by test_date_enc";
        val not_pruning1 = sql + "where not test_date_enc = '2012-01-01' group by test_date_enc";

        val nested_query0 = "with test_order as (select * from \"default\".test_order where test_date_enc > '2012-01-01' and test_date_enc < '2013-01-01')"
                + sql + "group by test_date_enc";
        val nested_query1 = "select * from (select * from (" + sql
                + "where test_date_enc > '2011-01-01' group by test_date_enc) where test_date_enc < '2012-01-01')";

        // date functions are not supported yet
        val date_function_query0 = "select * from (select year(test_date_enc) as test_date_enc_year from (" + sql
                + "where test_date_enc > '2011-01-01' and test_date_enc < '2013-01-01' group by test_date_enc)) where test_date_enc_year = '2014'";

        val between_query0 = sql + "where test_date_enc between '2011-01-01' and '2012-12-31' group by test_date_enc";

        val in_query0 = sql
                + "where test_date_enc in (Date '2011-06-01', Date '2012-06-01', Date '2012-12-31') group by test_date_enc";
        val in_query1 = sql
                + "where test_date_enc in ('2011-06-01', '2012-06-01', '2012-12-31') group by test_date_enc";
        val not_in_query0 = sql
                + "where test_date_enc not in (Date '2011-06-01', Date '2012-06-01', Date '2013-06-01') group by test_date_enc";
        val not_in_query1 = sql
                + "where test_date_enc not in ('2011-06-01', '2012-06-01', '2013-06-01') group by test_date_enc";

        val complex_query0 = sql
                + "where test_date_enc in ('2011-01-01', '2012-01-01', '2013-01-01', '2014-01-01') and test_date_enc > '2013-01-01' group by test_date_enc";
        val complex_query1 = sql
                + "where test_date_enc in (Date '2011-01-01', Date '2012-01-01', Date '2013-01-01', Date '2014-01-01') and test_date_enc > Date '2013-01-01' group by test_date_enc";

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01", "2011-01-01");
        val segmentRange2 = Pair.newPair("2011-01-01", "2013-01-01");
        val segmentRange3 = Pair.newPair("2013-01-01", "2015-01-01");

        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(modelId, and_pruning0, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, and_pruning1, 1, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(modelId, or_pruning0, 2, false, expectedRanges);
        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(modelId, or_pruning1, 2, false, expectedRanges);

        expectedRanges.clear();
        assertResultsAndScanFiles(modelId, pruning0, 0, true, expectedRanges);
        assertResultsAndScanFiles(modelId, pruning1, 0, true, expectedRanges);
        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(modelId, pruning2, 1, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(modelId, not_pruning0, 1, false, expectedRanges);
        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(modelId, not_pruning1, 3, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(modelId, nested_query0, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, nested_query1, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, between_query0, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, in_query0, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, in_query1, 1, false, expectedRanges);
        expectedRanges.clear();
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(modelId, not_in_query0, 3, false, expectedRanges);
        assertResultsAndScanFiles(modelId, not_in_query1, 3, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange2);
        assertResultsAndScanFiles(modelId, date_function_query0, 1, false, expectedRanges);

        expectedRanges.clear();
        expectedRanges.add(segmentRange3);
        assertResultsAndScanFiles(modelId, complex_query0, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, complex_query1, 1, false, expectedRanges);

        List<Pair<String, String>> query = Lists.newArrayList(//
                Pair.newPair("", and_pruning0), Pair.newPair("", and_pruning1), //
                Pair.newPair("", or_pruning0), Pair.newPair("", or_pruning1), //
                Pair.newPair("", pruning2), //
                Pair.newPair("", not_pruning0), Pair.newPair("", not_pruning1), //
                Pair.newPair("", nested_query0), Pair.newPair("", nested_query1), //
                Pair.newPair("", in_query0), Pair.newPair("", in_query1), //
                Pair.newPair("", date_function_query0), //
                Pair.newPair("", complex_query0), Pair.newPair("", complex_query1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");

        // kylin.query.heterogeneous-segment-enabled is turned off
        val projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(getProject(), copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.heterogeneous-segment-enabled", "false");
        });

        expectedRanges.clear();
        val sqls = Lists.<String> newArrayList();
        Collections.addAll(sqls, and_pruning0, and_pruning1, or_pruning0, or_pruning1, pruning0, pruning1, pruning2,
                not_pruning0, not_pruning1, nested_query0, nested_query1, between_query0, in_query0, in_query1,
                date_function_query0, complex_query0, complex_query1);
        assertResultAndScanFilesForPruningDate(modelId, sqls, expectedRanges);
    }

    private void assertResultAndScanFilesForPruningDate(String modelId, List<String> sqls,
            List<Pair<String, String>> expectedRanges) throws Exception {
        assertResultsAndScanFiles(modelId, sqls.get(0), 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(1), 1, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(2), 2, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(3), 2, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(4), 0, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(5), 0, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(6), 1, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(7), 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(8), 3, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(9), 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(10), 1, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(11), 1, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(12), 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(13), 3, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(14), 1, false, expectedRanges);

        assertResultsAndScanFiles(modelId, sqls.get(15), 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, sqls.get(16), 1, false, expectedRanges);
    }

    @Test
    public void testDimRangePruningAfterMerge() throws Exception {
        String modelId = "3f152495-44de-406c-9abf-b11d4132aaed";
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "true");
        buildMultiSegAndMerge("3f152495-44de-406c-9abf-b11d4132aaed");
        val ss = SparderEnv.getSparkSession();
        populateSSWithCSVData(getTestConfig(), getProject(), ss);

        val lessThanEquality = base + "where TEST_KYLIN_FACT.ORDER_ID <= 10";
        val castIn = base + "where test_date_enc in ('2014-12-20', '2014-12-21')";

        Instant startDate = DateFormat.stringToDate("2014-12-20").toInstant();
        String[] dates = new String[ss.sqlContext().conf().optimizerInSetConversionThreshold() + 1];
        for (int i = 0; i < dates.length; i++) {
            dates[i] = "'" + DateFormat.formatToDateStr(startDate.plus(i, ChronoUnit.DAYS).toEpochMilli()) + "'";
        }
        val largeCastIn = base + "where test_date_enc in (" + String.join(",", dates) + ")";

        val in = base + "where TEST_KYLIN_FACT.ORDER_ID in (4998, 4999)";
        val lessThan = base + "where TEST_KYLIN_FACT.ORDER_ID < 10";
        val and = base + "where PRICE < -99 AND TEST_KYLIN_FACT.ORDER_ID = 1";
        val or = base + "where TEST_KYLIN_FACT.ORDER_ID = 1 or TEST_KYLIN_FACT.ORDER_ID = 10";
        val notSupported0 = base + "where SELLER_ID <> 10000233";
        val notSupported1 = base + "where SELLER_ID > 10000233";

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01 00:00:00", "2011-01-01 00:00:00");
        val segmentRange2 = Pair.newPair("2011-01-01 00:00:00", "2015-01-01 00:00:00");
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);

        assertResultsAndScanFiles(modelId, largeCastIn, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, lessThanEquality, 2, false, expectedRanges);
        assertResultsAndScanFiles(modelId, castIn, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, in, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, lessThan, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, and, 1, false, expectedRanges);
        assertResultsAndScanFiles(modelId, or, 2, false, expectedRanges);
        assertResultsAndScanFiles(modelId, notSupported0, 2, false, expectedRanges);
        assertResultsAndScanFiles(modelId, notSupported1, 2, false, expectedRanges);
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", lessThanEquality));
        query.add(Pair.newPair("", in));
        query.add(Pair.newPair("", lessThan));
        query.add(Pair.newPair("", and));
        query.add(Pair.newPair("", or));
        query.add(Pair.newPair("", notSupported0));
        query.add(Pair.newPair("", notSupported1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testMergeDimRange() throws Exception {
        String dataflowId = "3f152495-44de-406c-9abf-b11d4132aaed";
        String modelId = dataflowId;
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        buildMultiSegAndMerge(dataflowId);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        Segments<NDataSegment> segments = dataflow.getSegments();
        Assert.assertEquals(2, segments.size());
        NDataSegment mergedSegment = segments.get(1);
        Assert.assertEquals(14, mergedSegment.getDimensionRangeInfoMap().size());

        val priceTest = base + "where PRICE <= -99.7900";

        val expectedRanges = Lists.<Pair<String, String>> newArrayList();
        val segmentRange1 = Pair.newPair("2009-01-01 00:00:00", "2011-01-01 00:00:00");
        val segmentRange2 = Pair.newPair("2011-01-01 00:00:00", "2015-01-01 00:00:00");
        expectedRanges.add(segmentRange1);
        expectedRanges.add(segmentRange2);

        assertResultsAndScanFiles(modelId, priceTest, 1, false, expectedRanges);
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", priceTest));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Test
    public void testMergeDimRangeFalse() throws Exception {
        String dataflowId = "3f152495-44de-406c-9abf-b11d4132aaed";
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        buildMultiSegs(dataflowId);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        Segments<NDataSegment> segments = dataflow.getSegments();
        Assert.assertEquals(3, segments.size());
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataSegment nDataSegment = segments.get(1);
            NDataSegmentManager.getInstance(getTestConfig(), getProject()).update(nDataSegment.getUuid(), copy -> {
                copy.getDimensionRangeInfoMap().clear();
            });
            return null;
        }, getProject());
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dataflowId);
        IndexPlan indexPlan = df.getIndexPlan();
        List<LayoutEntity> layouts = indexPlan.getAllLayouts();
        mergeSegments(dataflowId, Sets.newLinkedHashSet(layouts));
        Segments<NDataSegment> segments2 = dataflowManager.getDataflow(dataflowId).getSegments();
        Assert.assertEquals(2, segments2.size());
        NDataSegment segment = segments2.get(1);
        Assert.assertTrue(segment.getDimensionRangeInfoMap().isEmpty());
    }

    private void basicPruningScenario(String dfId) throws Exception {
        // shard pruning supports: Equality/In/IsNull/And/Or
        // other expression(gt/lt/like/cast/substr, etc.) will select all files.

        val equality = base + "where SELLER_ID = 10000233";
        val in = base + "where SELLER_ID in (10000233,10000234,10000235)";
        val isNull = base + "where SELLER_ID is NULL";
        val and = base + "where SELLER_ID in (10000233,10000234,10000235) and SELLER_ID = 10000233 ";
        val or = base + "where SELLER_ID = 10000233 or SELLER_ID = 1 ";
        val notSupported0 = base + "where SELLER_ID <> 10000233";
        val notSupported1 = base + "where SELLER_ID > 10000233";

        assertResultsAndScanFiles(dfId, equality, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, in, 9, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, isNull, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, and, 3, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, or, 4, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, notSupported0, 17, false, Lists.newArrayList());
        assertResultsAndScanFiles(dfId, notSupported1, 17, false, Lists.newArrayList());

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", equality));
        query.add(Pair.newPair("", in));
        query.add(Pair.newPair("", isNull));
        query.add(Pair.newPair("", and));
        query.add(Pair.newPair("", or));
        query.add(Pair.newPair("", notSupported0));
        query.add(Pair.newPair("", notSupported1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    @Override
    public String getProject() {
        return "file_pruning";
    }

    private long assertResultsAndScanFiles(String modelId, String sql, long numScanFiles, boolean emptyLayout,
            List<Pair<String, String>> expectedRanges) {
        val df = ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        val context = ContextUtil.listContexts().get(0);
        if (emptyLayout) {
            Assert.assertTrue(context.getStorageContext().isDataSkipped());
            Assert.assertEquals(-1L, context.getStorageContext().getBatchCandidate().getLayoutId());
            return numScanFiles;
        }
        df.collect();

        val actualNum = findFileSourceScanExec(df.queryExecution().executedPlan()).metrics().get("numFiles").get()
                .value();
        Assert.assertEquals(numScanFiles, actualNum);
        val segmentIds = context.getStorageContext().getBatchCandidate().getPrunedSegments();
        assertPrunedSegmentRange(modelId, segmentIds, expectedRanges);
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
            List<Pair<String, String>> expectedRanges) {
        val model = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();

        if (CollectionUtils.isEmpty(expectedRanges)) {
            return;
        }
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);
        }
    }
}
