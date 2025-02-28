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

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.LayoutFileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
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
@Ignore
public class NFilePruningV2Test extends NLocalWithSparkSessionTest {

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
        sparkConf.set("spark.sql.sources.bucketing.enabled", "false");
        sparkConf.set("spark.sql.adaptive.shuffle.maxTargetPostShuffleInputSize", "1");
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

        NDataModelManager instance = NDataModelManager.getInstance(getTestConfig(), getProject());
        instance.updateDataModel("8c670664-8d05-466a-802f-83c023b56c77", write -> write.setStorageType(2));
        instance.updateDataModel("8c670664-8d05-466a-802f-83c023b56c78", write -> write.setStorageType(2));
        instance.updateDataModel("8c670664-8d05-466a-802f-83c023b56c79", write -> write.setStorageType(2));
        instance.updateDataModel("9cde9d25-9334-4b92-b229-a00f49453757", write -> write.setStorageType(2));
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/file_pruning" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testNonExistTimeRange() throws Exception {
        val start = SegmentRange.dateToLong("2023-01-01 00:00:00");
        val end = SegmentRange.dateToLong("2025-01-01 00:00:00");
        val dfName = "8c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
        assertResultsAndScanFiles(base, 0);
    }

    @Test
    public void testSegPruningWithStringDate() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c78", 10001);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        val no_pruning1 = "select count(*) from TEST_KYLIN_FACT";
        val no_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > '2010-01-01' and CAL_DT < '2015-01-01'";
        val seg_pruning1 = "select count(*) from TEST_KYLIN_FACT where CAL_DT < '2013-01-01'";
        val seg_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > '2013-01-01'";
        val seg_pruning3 = "select count(*) from TEST_KYLIN_FACT where CAL_DT = '2013-05-16'";
        val seg_pruning4 = "select count(*) from TEST_KYLIN_FACT where CAL_DT in ('2013-05-16', '2013-03-22')";
        val seg_pruning5 = "select count(*) from TEST_KYLIN_FACT where CAL_DT not in ('2013-05-16', '2013-03-22')";
        val seg_pruning6 = "select count(*) from TEST_KYLIN_FACT where CAL_DT <> '2013-05-16'";
        assertResultsAndScanFiles(no_pruning1, 731);
        assertResultsAndScanFiles(no_pruning2, 731);
        assertResultsAndScanFiles(seg_pruning1, 365);
        assertResultsAndScanFiles(seg_pruning2, 365);
        assertResultsAndScanFiles(seg_pruning3, 1);
        assertResultsAndScanFiles(seg_pruning4, 2);
        assertResultsAndScanFiles(seg_pruning5, 729);
        assertResultsAndScanFiles(seg_pruning6, 730);
    }

    @Ignore("Unsupport timestamp")
    public void testSegPruningWithStringTimeStamp() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c79", 10001);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        String base = "select count(*)  FROM TEST_ORDER_STRING_TS LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER_STRING_TS.ORDER_ID ";

        String and_pruning0 = base
                + "where TEST_TIME_ENC > '2011-01-01 00:00:00' and TEST_TIME_ENC < '2013-01-01 00:00:00'";
        String and_pruning1 = base
                + "where TEST_TIME_ENC > '2011-01-01 00:00:00' and TEST_TIME_ENC = '2016-01-01 00:00:00'";

        String or_pruning0 = base
                + "where TEST_TIME_ENC > '2011-01-01 00:00:00' or TEST_TIME_ENC = '2016-01-01 00:00:00'";
        String or_pruning1 = base
                + "where TEST_TIME_ENC < '2009-01-01 00:00:00' or TEST_TIME_ENC > '2015-01-01 00:00:00'";

        String pruning0 = base + "where TEST_TIME_ENC < '2009-01-01 00:00:00'";
        String pruning1 = base + "where TEST_TIME_ENC <= '2009-01-01 00:00:00'";
        String pruning2 = base + "where TEST_TIME_ENC >= '2015-01-01 00:00:00'";

        String not0 = base + "where TEST_TIME_ENC <> '2012-01-01 00:00:00'";

        String in_pruning0 = base
                + "where TEST_TIME_ENC in ('2009-01-01 00:00:00', '2008-01-01 00:00:00', '2016-01-01 00:00:00')";
        String in_pruning1 = base + "where TEST_TIME_ENC in ('2008-01-01 00:00:00', '2016-01-01 00:00:00')";

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

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("base", base));
        query.add(Pair.newPair("and_pruning0", and_pruning0));
        query.add(Pair.newPair("and_pruning1", and_pruning1));
        query.add(Pair.newPair("or_pruning0", or_pruning0));
        query.add(Pair.newPair("or_pruning1", or_pruning1));
        query.add(Pair.newPair("pruning0", pruning0));
        query.add(Pair.newPair("pruning1", pruning1));
        query.add(Pair.newPair("pruning2", pruning2));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    @Ignore("Unsupport timestamp")
    public void testSegPruningWithTimeStamp() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77", 10001);
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

        String not0 = base + "where TEST_TIME_ENC <> TIMESTAMP '2012-01-01 00:00:00'";

        String in_pruning0 = base
                + "where TEST_TIME_ENC in (TIMESTAMP '2009-01-01 00:00:00',TIMESTAMP '2008-01-01 00:00:00',TIMESTAMP '2016-01-01 00:00:00')";
        String in_pruning1 = base
                + "where TEST_TIME_ENC in (TIMESTAMP '2008-01-01 00:00:00',TIMESTAMP '2016-01-01 00:00:00')";

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

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("base", base));
        query.add(Pair.newPair("and_pruning0", and_pruning0));
        query.add(Pair.newPair("and_pruning1", and_pruning1));
        query.add(Pair.newPair("or_pruning0", or_pruning0));
        query.add(Pair.newPair("or_pruning1", or_pruning1));
        query.add(Pair.newPair("pruning0", pruning0));
        query.add(Pair.newPair("pruning1", pruning1));
        query.add(Pair.newPair("pruning2", pruning2));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    @Test
    public void testShardPruning() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "100");
        overwriteSystemProp("kylin.storage.columnar.bucket-num", "10");
        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77");

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        basicPruningScenario();
        pruningWithVariousTypesScenario();
    }

    @Test
    public void testDimRangePruning() throws Exception {
        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77");

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val lessThanEquality = base + "where TEST_ORDER.ORDER_ID <= 2";
        val in = base + "where TEST_ORDER.ORDER_ID in (4998, 4999)";
        val lessThan = base + "where TEST_ORDER.ORDER_ID < 2";
        val and = base + "where PRICE < -99 AND TEST_ORDER.ORDER_ID = 1";
        val or = base + "where TEST_ORDER.ORDER_ID = 1 or TEST_ORDER.ORDER_ID = 2 ";
        val notSupported0 = base + "where SELLER_ID <> 10000233";
        val notSupported1 = base + "where SELLER_ID > 10000233";

        assertResultsAndScanFiles(lessThanEquality, 3);
        assertResultsAndScanFiles(in, 3);
        assertResultsAndScanFiles(lessThan, 3);
        assertResultsAndScanFiles(and, 3);
        assertResultsAndScanFiles(or, 3);
        assertResultsAndScanFiles(notSupported0, 3);
        assertResultsAndScanFiles(notSupported1, 3);

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
    public void testPruningWithChineseCharacter() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "1");
        fullBuild("9cde9d25-9334-4b92-b229-a00f49453757");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val chinese0 = "select count(*) from TEST_MEASURE where name1 = '中国'";
        val chinese1 = "select count(*) from TEST_MEASURE where name1 <> '中国'";

        assertResultsAndScanFiles(chinese0, 1);
        assertResultsAndScanFiles(chinese1, 4);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", chinese0));
        query.add(Pair.newPair("", chinese1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");
    }

    private void pruningWithVariousTypesScenario() throws Exception {
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

        assertResultsAndScanFiles(bool0, 3);
        assertResultsAndScanFiles(bool1, 9);

        assertResultsAndScanFiles(decimal0, 3);
        assertResultsAndScanFiles(decimal1, 33);

        // calcite will treat short as int. So pruning will not work.
        assertResultsAndScanFiles(short0, 24);
        assertResultsAndScanFiles(short1, 24);

        assertResultsAndScanFiles(string0, 3);
        assertResultsAndScanFiles(string1, 15);

        assertResultsAndScanFiles(long0, 3);
        assertResultsAndScanFiles(long1, 30);

        assertResultsAndScanFiles(date0, 3);
        assertResultsAndScanFiles(date1, 30);

        // segment pruning first, then shard pruning
        // so the scanned files is 1 not 3(each segment per shard)
        assertResultsAndScanFiles(ts0, 3);
        assertResultsAndScanFiles(ts1, 30);

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

    private void basicPruningScenario() throws Exception {
        // shard pruning supports: Equality/In/IsNull/And/Or
        // other expression(gt/lt/like/cast/substr, etc.) will select all files.

        val equality = base + "where SELLER_ID = 10000233";
        val in = base + "where SELLER_ID in (10000233,10000234,10000235)";
        val isNull = base + "where SELLER_ID is NULL";
        val and = base + "where SELLER_ID in (10000233,10000234,10000235) and SELLER_ID = 10000233 ";
        val or = base + "where SELLER_ID = 10000233 or SELLER_ID = 1 ";
        val notSupported0 = base + "where SELLER_ID <> 10000233";
        val notSupported1 = base + "where SELLER_ID > 10000233";

        assertResultsAndScanFiles(equality, 3);
        assertResultsAndScanFiles(in, 9);
        assertResultsAndScanFiles(isNull, 3);
        assertResultsAndScanFiles(and, 3);
        assertResultsAndScanFiles(or, 6);
        assertResultsAndScanFiles(notSupported0, 30);
        assertResultsAndScanFiles(notSupported1, 30);

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

    private long assertResultsAndScanFiles(String sql, long numScanFiles) throws Exception {
        val df = ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        df.collect();
        val actualNum = findFileSourceScanExec(df.queryExecution().sparkPlan()).metrics().get("numFiles").get().value();
        Assert.assertEquals(numScanFiles, actualNum);
        return actualNum;
    }

    private LayoutFileSourceScanExec findFileSourceScanExec(SparkPlan plan) {
        return (LayoutFileSourceScanExec) plan.find(new AbstractFunction1<SparkPlan, Object>() {
            @Override
            public Object apply(SparkPlan p) {
                return p instanceof LayoutFileSourceScanExec;
            }
        }).get();
    }
}
