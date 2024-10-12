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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange.TimePartitionedSegmentRange;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.OlapContextTestUtil;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class NTopNTest extends NLocalWithSparkSessionTest {

    private NDataflowManager dfMgr = null;

    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");

        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        JobContextUtil.getJobContext(getTestConfig());

        getTestConfig().setProperty("kylin.query.heterogeneous-segment-enabled", "false");
    }

    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Override
    public String getProject() {
        return "top_n";
    }

    @Test
    public void testTopNWithMultiDims() throws Exception {
        String dfID = "79547ec2-350e-4ba4-88f9-099048962ceb";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dataflow.getIndexPlan().getLayoutEntity(101001L)), true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,TRANS_ID order by sum(PRICE) desc limit 1";
        String sql2 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) desc limit 1";
        query.add(Pair.newPair("topn_with_multi_dim", sql1));
        query.add(Pair.newPair("topn_with_one_dim", sql2));
        // TopN will answer TopN style query.
        verifyTopnResult(query, dfMgr.getDataflow(dfID));
        ExecAndCompExt.execAndCompare(query, getProject(), CompareLevel.NONE, "left");
    }

    @Test
    public void testTopNCannotAnswerAscendingTopnQuery() throws Exception {
        String dfID = "79547ec2-350e-4ba4-88f9-099048962ceb";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dataflow.getIndexPlan().getLayoutEntity(101001L)), true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,TRANS_ID order by sum(PRICE) limit 1";
        String sql2 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) limit 1";
        query.add(Pair.newPair("topn_with_multi_dim", sql1));
        query.add(Pair.newPair("topn_with_one_dim", sql2));

        try {
            ExecAndCompExt.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage()
                    .contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testTopNCanNotAnswerNonTopNStyleQuery() throws Exception {
        dfMgr.updateDataflowStatus("fb6ce800-43ee-4ef9-b100-39d523f36304", RealizationStatusEnum.OFFLINE);
        dfMgr.updateDataflowStatus("da101c43-6d22-48ce-88d2-bf0ce0594022", RealizationStatusEnum.OFFLINE);
        String dfID = "79547ec2-350e-4ba4-88f9-099048962ceb";
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dfMgr.getDataflow(dfID).getIndexPlan().getLayoutEntity(100001L),
                        dfMgr.getDataflow(dfID).getIndexPlan().getLayoutEntity(100003L)),
                true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_answer_single_dim",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) desc limit 1"));
        query.add(Pair.newPair("can_answer_multi_dim",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,TRANS_ID order by sum(PRICE) desc limit 1"));
        // TopN will answer TopN style query.
        verifyTopnResult(query, dfMgr.getDataflow(dfID));
        ExecAndCompExt.execAndCompare(query, getProject(), CompareLevel.NONE, "left");
        try {
            query.clear();
            query.add(Pair.newPair("can_not_answer", "select sum(PRICE) from TEST_TOP_N group by SELLER_ID"));
            // TopN will not answer sum.
            ExecAndCompExt.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage()
                    .contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testSingleDimLayoutCannotAnswerMultiTopnQuery() throws Exception {
        dfMgr.updateDataflowStatus("79547ec2-350e-4ba4-88f9-099048962ceb", RealizationStatusEnum.OFFLINE);
        dfMgr.updateDataflowStatus("da101c43-6d22-48ce-88d2-bf0ce0594022", RealizationStatusEnum.OFFLINE);
        String dfID = "fb6ce800-43ee-4ef9-b100-39d523f36304";
        //  layout[ID, count(*), sum(price), Topn(price, SELLER_ID)]
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dfMgr.getDataflow(dfID).getIndexPlan().getLayoutEntity(1L)), true);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("cannot_answer_multi_dim_in_single_dim_index",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,ID order by sum(PRICE) desc limit 1"));
        try {
            ExecAndCompExt.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage()
                    .contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testSameTableNameInDifferentDatabase() throws Exception {
        TopNCounter.EXTRA_SPACE_RATE = 1;

        fullBuild("da101c43-6d22-48ce-88d2-bf0ce0594022");

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val sql = "select A.SELLER_ID,sum(A.PRICE) from ISSUES.TEST_TOP_N A "
                + " join TEST_TOP_N B on A.ID=B.ID group by A.SELLER_ID order by sum(B.PRICE) desc limit 100";
        try {
            ExecAndCompExt.queryModelWithoutCompute(getProject(), sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage()
                    .contains("No realization found for OlapContext"));
        }

    }

    private void verifyTopnResult(List<Pair<String, String>> queries, NDataflow dataflow) throws SqlParseException {
        //verify topN measure will answer the multi-Dimension query
        for (Pair<String, String> nameAndQueryPair : queries) {
            OlapContext context = OlapContextTestUtil
                    .getOlapContexts(dataflow.getProject(), nameAndQueryPair.getSecond(), true).get(0);
            Map<String, String> aliasMapping = OlapContextTestUtil.matchJoins(dataflow.getModel(), context);
            context.fixModel(dataflow.getModel(), aliasMapping);
            CapabilityResult capabilityResult = context.getStorageContext().getBatchCandidate().getCapabilityResult();
            Assert.assertEquals(1, capabilityResult.influences.size());
            CapabilityResult.CapabilityInfluence capabilityInfluence = capabilityResult.influences.get(0);
            Assert.assertFalse(capabilityInfluence instanceof CapabilityResult.DimensionAsMeasure);
            Assert.assertEquals(context.getAllColumns(),
                    Sets.newHashSet(capabilityInfluence.getInvolvedMeasure().getFunction().getColRefs()));
        }
    }
}
