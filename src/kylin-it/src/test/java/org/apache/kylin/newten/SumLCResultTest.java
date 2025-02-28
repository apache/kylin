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

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SumLCResultTest extends NLocalWithSparkSessionTest {

    private NDataflowManager dfMgr = null;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/sum_lc");
        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        getTestConfig().setProperty("kylin.query.heterogeneous-segment-enabled", "false");
        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/sum_lc" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "sum_lc";
    }

    @Test
    public void testSumLCWithDifferentDataType() throws Exception {
        String dfID = "f35f2937-9e4d-347a-7465-d64df939e7d6";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(30001L);
        Assert.assertNotNull(layout);

        indexDataConstructor.buildIndex(dfID,
                new SegmentRange.TimePartitionedSegmentRange(1661961600000L, 1664553600000L), Sets.newHashSet(layout),
                true);
        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select TX_DATE, sum_lc(TINYINT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_tinyint_data_query", sql1));

        String sql2 = "select TX_DATE, sum_lc(SMALLINT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_smallint_data_query", sql2));

        String sql3 = "select TX_DATE, sum_lc(INT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_int_data_query", sql3));

        String sql4 = "select TX_DATE, sum_lc(BIGINT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_bigint_data_query", sql4));

        String sql5 = "select TX_DATE, sum_lc(FLOAT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_float_data_query", sql5));

        String sql6 = "select TX_DATE, sum_lc(DOUBLE_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_double_data_query", sql6));

        String sql7 = "select TX_DATE, sum_lc(DECIMAL_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by TX_DATE";
        query.add(Pair.newPair("sum_lc_decimal_data_query", sql7));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "left");
    }

    @Test
    public void testPostAggregate() throws Exception {
        String dfID = "f35f2937-9e4d-347a-7465-d64df939e7d6";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(30001L);
        Assert.assertNotNull(layout);

        indexDataConstructor.buildIndex(dfID,
                new SegmentRange.TimePartitionedSegmentRange(1661961600000L, 1664553600000L), Sets.newHashSet(layout),
                true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();

        String sql = "select sum_lc(TINYINT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X";
        query.add(Pair.newPair("sum_lc_post_aggregate_query", sql));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "left");
    }

    @Test
    public void testEmptySegment() throws Exception {
        String dfID = "f35f2937-9e4d-347a-7465-d64df939e7d6";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(50001L);
        Assert.assertNotNull(layout);

        indexDataConstructor.buildIndex(dfID,
                new SegmentRange.TimePartitionedSegmentRange(1667232000000L, 1669824000000L), Sets.newHashSet(layout),
                true);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();

        String sql1 = "select account, sum_lc(TINYINT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X group by account";
        query.add(Pair.newPair("sum_lc_empty_seg_exactly_match_query", sql1));

        String sql2 = "select sum_lc(TINYINT_DATA, TX_DATE) from SSB.SUMLC_EXTEND_4X";
        query.add(Pair.newPair("sum_lc_empty_seg_post_agg_query", sql2));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "left");
    }

    @Test
    public void testCCQuery() throws Exception {
        String dfID = "c2f81b79-2c10-dce2-4206-588cab0e68ec";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(190001L);
        Assert.assertNotNull(layout);

        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(layout), true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();

        String sql1 = "select account, sum_lc(INT_DATA, TO_TIME_COMPOSE_CC) from SSB.SUMLC_CC_TEST group by account";
        query.add(Pair.newPair("sum_lc_cc_time", sql1));

        String sql2 = "select account, sum_lc(INT_DATA * 2, TO_TIME_COMPOSE_CC) from SSB.SUMLC_CC_TEST group by account";
        query.add(Pair.newPair("sum_lc_cc_int", sql2));

        String sql3 = "select account, sum_lc(INT_DATA_CC, TO_TIME_COMPOSE_CC) from SSB.SUMLC_CC_TEST group by account";
        query.add(Pair.newPair("sum_lc_cc_int_cc_time", sql3));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "left");
    }

    @Test
    public void testTimestampTimeCol() throws Exception {
        String dfID = "648098d6-3009-5b26-3e20-82e494cfdb0c";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(1L);
        Assert.assertNotNull(layout);

        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(layout), true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();

        String sql = "select sum_lc(BALANCE, SUM_DATE) from SSB.SUM_LC_TB";
        query.add(Pair.newPair("query", sql));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "left");
    }

    @Test
    public void testNullVal() throws Exception {
        String dfID = "4120b88e-6a3b-aba2-f86e-c692f6588f22";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(1L);
        Assert.assertNotNull(layout);

        indexDataConstructor.buildIndex(dfID,
                new SegmentRange.TimePartitionedSegmentRange(1667750400000L, 1667836800000L), Sets.newHashSet(layout),
                true);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();

        String sql1 = "select account1, account2, part_col, sum_lc(data_null, sum_date1) from ssb.sum_lc_null_tbl group by account1, account2, part_col";
        query.add(Pair.newPair("exact_match_null_query", sql1));

        String sql2 = "select account1, sum_lc(data_null, sum_date1) from ssb.sum_lc_null_tbl group by account1";
        query.add(Pair.newPair("double_null_query", sql2));

        String sql3 = "select account1, sum_lc(data_decimal, sum_date1) from ssb.sum_lc_null_tbl group by account1";
        query.add(Pair.newPair("decimal_null_query", sql3));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "left");
    }

}
