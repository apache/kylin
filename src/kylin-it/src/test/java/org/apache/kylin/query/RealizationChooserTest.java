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

package org.apache.kylin.query;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.metadata.realization.QueryableSeg;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.DataflowCapabilityChecker;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.SmartContext;
import org.apache.kylin.rec.SmartMaster;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class RealizationChooserTest extends NLocalWithSparkSessionTest {

    @Test
    public void testUseSelectStarAnswerNonRawQueryNotAffectRecommendation() throws SqlParseException {
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        overwriteSystemProp("kylin.query.implicit-computed-column-convert", "false");

        // propose tableIndex
        final String project = "ssb";
        String sql = "select * from ssb.customer";
        val proposeContext = AccelerationUtil.runWithSmartContext(getTestConfig(), project, new String[] { sql }, true);

        // mock tableIndex has been built
        String modelId = proposeContext.getProposedModels().get(0).getId();
        int rowcount = 100;
        NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataflow dataflow = dfMgr.getDataflow(modelId);
        IndexPlan indexPlan = indexMgr.getIndexPlan(modelId);
        Set<Long> allLayoutIds = indexPlan.getAllLayoutIds(false);
        Assert.assertEquals(1, allLayoutIds.size());
        Long layoutId = allLayoutIds.iterator().next();
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout layout = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), layoutId);
        layout.setRows(rowcount);
        dataflowUpdate.setToAddOrUpdateLayouts(layout);
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(dataflowUpdate);

        // doesn't propose cc, but propose agg indexes and measures
        String newSql = "select if(COUNT(DISTINCT  case when C_NAME <> 0.0 and C_CITY = '正常销售' "
                + "and C_ADDRESS = '线下' then C_CUSTKEY else null end) = 0,\n" //
                + "          null,\n"
                + "          SUM(case when C_MKTSEGMENT = '线下' and C_CITY = '正常销售' then C_CUSTKEY else 0 end)\n"
                + "              / COUNT(DISTINCT case when C_ADDRESS <> 0.0 and C_CITY = '正常销售' "
                + "and C_NAME = '线下' then C_CUSTKEY else null end))\n" + "from SSB.CUSTOMER";
        SmartContext smartContext2 = new SmartContext(getTestConfig(), project, new String[] { newSql });
        SmartMaster smartMaster2 = new SmartMaster(smartContext2);
        smartMaster2.runUtWithContext(null);
        AbstractContext.ModelContext modelContext = smartMaster2.getContext().getModelContexts().get(0);
        NDataModel model = modelContext.getTargetModel();
        Assert.assertTrue(model.getComputedColumnDescs().isEmpty());
        Assert.assertEquals(3, model.getAllMeasures().size());
        Assert.assertEquals(3, modelContext.getTargetIndexPlan().getAllLayoutIds(false).size());

        // query will hit the built layout
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(project, newSql);
        Assert.assertEquals(1, olapContexts.size());
        NLayoutCandidate candidate = olapContexts.get(0).getStorageContext().getBatchCandidate();
        Assert.assertEquals(layoutId.longValue(), candidate.getLayoutEntity().getId());
    }

    @Test
    public void testExactlyMatchModelIsBetterThanPartialMatchModel() throws SqlParseException {
        // 1. create small inner-join model
        final String project = "newten";
        String sql = "select CAL_DT, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT='2012-01-10' group by CAL_DT ";
        val proposeContext = AccelerationUtil.runWithSmartContext(KylinConfig.getInstanceFromEnv(), project,
                new String[] { sql }, true);
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        Assert.assertFalse(proposeContext.getProposedModels().isEmpty());
        String dataflow = proposeContext.getProposedModels().get(0).getId();
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(dataflow);
        addLayout(df, 1000L);

        // 2. create a big inner-join model
        String sql1 = "select CAL_DT, count(*) from test_kylin_fact "
                + "inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "where CAL_DT='2012-01-10' group by CAL_DT ";
        val proposeContext2 = AccelerationUtil.runWithSmartContext(KylinConfig.getInstanceFromEnv(), project,
                new String[] { sql1 }, true);
        Assert.assertFalse(proposeContext2.getProposedModels().isEmpty());
        NDataModel dataModel1 = proposeContext2.getProposedModels().get(0);
        String dataflow1 = dataModel1.getId();
        val df1 = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(dataflow1);
        addLayout(df1, 980L);

        // 3. config inner partial match inner join, then the small join should hit the small model.
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.match-partial-inner-join-model", "true");
        Assert.assertFalse(OlapContextTestUtil.matchJoins(dataModel1, context).isEmpty());
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals(context.getStorageContext().getBatchCandidate().getLayoutEntity().getModel().getId(),
                dataflow);

    }

    private void addLayout(NDataflow dataflow, long rowcount) {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "newten");
        var indexPlan = indexPlanManager.getIndexPlanByModelAlias(dataflow.getModelAlias());
        NIndexPlanManager.NIndexPlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();

            val newAggIndex = new IndexEntity();
            newAggIndex.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(0, 1));
            newAggIndex.setMeasures(Lists.newArrayList(100000));
            val newLayout1 = new LayoutEntity();
            newLayout1.setId(newAggIndex.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(0, 1, 100000));
            val newLayout2 = new LayoutEntity();
            newLayout2.setId(newAggIndex.getId() + 2);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(1, 0, 100000));
            newAggIndex.setLayouts(Lists.newArrayList(newLayout1, newLayout2));
            cuboids.add(newAggIndex);

            copyForWrite.setIndexes(cuboids);
        };
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), updater);

        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout layout1 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 10001L);
        layout1.setRows(rowcount);
        NDataLayout layout2 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 10002L);
        layout2.setRows(rowcount);
        dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2);
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "newten").updateDataflow(dataflowUpdate);
    }

    @Test
    public void testSortByCandidatesId_when_candidatesCostAreTheSame() throws SqlParseException {
        // prepare table desc snapshot path
        String project = "default";
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject())
                .getTableDesc("DEFAULT.TEST_ACCOUNT").setLastSnapshotPath(
                        "default/table_snapshot/DEFAULT.TEST_ACCOUNT/d6ba492b-13bf-444d-b6e3-71bfa903344d");

        // can be answered by both [nmodel_basic] & [nmodel_basic_inner]
        String sql = "select count(*) from TEST_ACCOUNT group by ACCOUNT_ID";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", context.getStorageContext().getLookupCandidate().getTable());
    }

    /** need write another test case to cover related function. */
    @Test
    public void testUseOnlyModelsInPriorities() throws SqlParseException {
        try (QueryContext queryContext = QueryContext.current()) {
            String project = "default";
            NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject())
                    .getTableDesc("DEFAULT.TEST_ACCOUNT").setLastSnapshotPath(
                            "default/table_snapshot/DEFAULT.TEST_ACCOUNT/d6ba492b-13bf-444d-b6e3-71bfa903344d");
            // can be answered by both [nmodel_basic] & [nmodel_basic_inner] 
            String sql = "select count(*) from TEST_ACCOUNT group by ACCOUNT_ID";

            OlapContext context1 = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
            Assert.assertEquals("DEFAULT.TEST_ACCOUNT", context1.getStorageContext().getLookupCandidate().getTable());

            overwriteSystemProp("kylin.query.use-only-in-priorities", "TRUE");
            queryContext.setModelPriorities(new String[] {});
            OlapContext context2 = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
            Assert.assertEquals("DEFAULT.TEST_ACCOUNT", context2.getStorageContext().getLookupCandidate().getTable());

            queryContext.setModelPriorities(new String[] { "ALL_FIXED_LENGTH" });
            OlapContext context3 = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
            Assert.assertEquals("DEFAULT.TEST_ACCOUNT", context3.getStorageContext().getLookupCandidate().getTable());
        }
    }

    @Test
    public void testNonEquiJoinMatch() throws SqlParseException {
        //   TEST_KYLIN_FACT
        //              \
        //             TEST_ACCOUNT  on equi-join
        //                  \
        //              SELLER_ACCOUNT  on non-equi-join
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        // 1. create small inner-join model
        final String project = "newten";
        String sql = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON SELLER_ACCOUNT.ACCOUNT_BUYER_LEVEL = TEST_ORDER.BUYER_ID "
                + "AND SELLER_ACCOUNT.ACCOUNT_COUNTRY>=TEST_ORDER.TEST_EXTENDED_COLUMN "
                + "AND SELLER_ACCOUNT.ACCOUNT_COUNTRY<TEST_ORDER.TEST_TIME_ENC";
        val proposeContext = AccelerationUtil.runWithSmartContext(KylinConfig.getInstanceFromEnv(), project,
                new String[] { sql }, true);
        Assert.assertEquals(1, proposeContext.getModelContexts().size());

        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql, true).get(0);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", context.getJoins().get(1).getFKSide().getTableIdentity());
    }

    @Test
    public void testSelectRealizationWithModelViewSql() throws SqlParseException {
        overwriteSystemProp("kylin.query.auto-model-view-enabled", "true");

        final String project = "newten";
        String sql = "select lstg_format_name, price, sum(item_count) as item_cnt "
                + "from test_kylin_fact join test_account on test_kylin_fact.seller_id = test_account.account_id\n"
                + "group by lstg_format_name, price";
        AbstractContext context = AccelerationUtil.runWithSmartContext(KylinConfig.getInstanceFromEnv(), project,
                new String[] { sql }, true);
        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        String modelAlias = model.getAlias();
        String sqlView = "select lstg_format_name from newten." + modelAlias + " group by lstg_format_name";

        // all layouts in the model has not been built
        {
            Consumer<NoRealizationFoundException> consumer = e -> Assert
                    .assertTrue(e.getMessage().contains("No realization found for OlapContext"));
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(project, sqlView, true, consumer).get(0);
            Assert.assertEquals(modelAlias, olapContext.getBoundedModelAlias());
        }

        // all layouts in the model has been built
        {
            NDataflow df = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(model.getUuid());
            mockBuiltDataLayouts(df);

            Consumer<NoRealizationFoundException> consumer = e -> Assert
                    .assertEquals("There is no need to select realizations for OlapContexts.", e.getMessage());
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(project, sqlView, true, consumer).get(0);
            Assert.assertEquals(modelAlias, olapContext.getBoundedModelAlias());
        }
    }

    private void mockBuiltDataLayouts(NDataflow dataflow) {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), dataflow.getProject());
        var indexPlan = indexPlanManager.getIndexPlanByModelAlias(dataflow.getModelAlias());
        List<LayoutEntity> allLayouts = indexPlan.getAllLayouts();
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout[] dataLayouts = allLayouts.stream()
                .map(layout -> NDataLayout.newDataLayout(dataflow.getLastSegment().getSegDetails(), layout.getId()))
                .toArray(NDataLayout[]::new);
        dataflowUpdate.setToAddOrUpdateLayouts(dataLayouts);
        NDataflowManager.getInstance(getTestConfig(), dataflow.getProject()).updateDataflow(dataflowUpdate);
    }

    @Test
    public void testHybridStreaming() throws SqlParseException {
        String project = "streaming_test";
        String sql = "select count(*), sum(LO_LINENUMBER) from SSB_STREAMING";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        IRealization realization = context.getRealization();
        Assert.assertTrue(realization instanceof HybridRealization);
        HybridRealization hybridRealization = (HybridRealization) realization;
        Assert.assertTrue(hybridRealization.getBatchRealization() instanceof NDataflow);
        Assert.assertTrue(hybridRealization.getStreamingRealization() instanceof NDataflow);
        Assert.assertTrue(hybridRealization.getStreamingRealization().isStreaming());
        Assert.assertFalse(realization.getAllDimensions().isEmpty());
        Assert.assertTrue(realization.getDateRangeStart() < realization.getDateRangeEnd());
        Assert.assertEquals(IStorageAware.ID_NDATA_STORAGE, realization.getStorageType());
        Assert.assertNotNull(realization.getUuid());
        Assert.assertFalse(realization.isStreaming());
        Assert.assertEquals(realization.getProject(), project);
        FunctionDesc functionDesc = hybridRealization.getBatchRealization().getMeasures().get(1).getFunction();
        Assert.assertNotNull(hybridRealization.findAggrFunc(functionDesc));
        Assert.assertTrue(hybridRealization.hasPrecalculatedFields());
        Assert.assertEquals("model_streaming", context.getRealization().getModel().getAlias());
        Assert.assertFalse(context.getStorageContext().isBatchCandidateEmpty());
        Assert.assertFalse(context.getStorageContext().isStreamCandidateEmpty());

        hybridRealization.setUuid("123");
        hybridRealization.setProject(project);
        Assert.assertEquals("123", hybridRealization.getUuid());

        HybridRealization hybridTest = new HybridRealization(hybridRealization.getBatchRealization(), null, "");
        Assert.assertNull(hybridTest.getBatchRealization());

        hybridTest = new HybridRealization(null, null, "");
        Assert.assertNull(hybridTest.getBatchRealization());
        Assert.assertNull(hybridTest.getStreamingRealization());

        hybridTest = new HybridRealization(null, hybridRealization.getStreamingRealization(), "");
        Assert.assertNull(hybridTest.getStreamingRealization());

        {
            Candidate candidate = new Candidate(hybridRealization, context, null);
            QueryableSeg queryableSeg = candidate.getQueryableSeg();
            queryableSeg.setBatchSegments(Lists.newArrayList());
            queryableSeg.setStreamingSegments(Lists.newArrayList());
            queryableSeg.setChSegToLayoutsMap(Maps.newHashMap());
            CapabilityResult result = DataflowCapabilityChecker.hybridRealizationCheck(hybridRealization, candidate,
                    context.getSQLDigest());
            Assert.assertTrue(Integer.MAX_VALUE - 1 - result.getCost() < 0.0001);
        }
    }

    @Test
    public void testHybridStreamingMatchLookUp() throws Exception {
        String project = "streaming_test";
        String sql = "select * from SSB.CUSTOMER";
        NTableMetadataManager.getInstance(getTestConfig(), project).getTableDesc("SSB.CUSTOMER")
                .setLastSnapshotPath("default/table_snapshot/SSB.CUSTOMER/cf3eddab-4686-721c-ee1f-a502ed95163e");

        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        Assert.assertNull(context.getRealization());
        Assert.assertTrue(context.getStorageContext().getBatchCandidate().isEmpty());
        Assert.assertTrue(context.getStorageContext().getStreamCandidate().isEmpty());
        Assert.assertEquals("SSB.CUSTOMER", context.getStorageContext().getLookupCandidate().getTable());
    }

    @Test
    public void testHybridNoRealization() throws SqlParseException {
        String project = "streaming_test";
        String sql = "select LO_SHIPMODE from SSB_STREAMING group by LO_SHIPMODE";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql, true).get(0);
        try {
            RealizationChooser.attemptSelectCandidate(context);
        } catch (NoStreamingRealizationFoundException e) {
            Assert.assertEquals(e.getErrorCode().getCodeString(),
                    ServerErrorCode.STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), MsgPicker.getMsg().getNoStreamingModelFound());
            Assert.assertTrue(context.getStorageContext().isBatchCandidateEmpty());
            Assert.assertTrue(context.getStorageContext().isStreamCandidateEmpty());
        }
        Assert.assertNull(context.getRealization());
    }

    /**
     * Query hybrid realizations
     */
    @Test
    public void testHybridNoRealizationNoReCut() throws SqlParseException {
        String project = "streaming_test";
        String sql = "select LO_SHIPMODE from SSB_STREAMING group by LO_SHIPMODE";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql, false).get(0);
        try {
            RealizationChooser.attemptSelectCandidate(context);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoStreamingRealizationFoundException);
            Assert.assertEquals(((NoStreamingRealizationFoundException) e).getErrorCode().getCodeString(),
                    ServerErrorCode.STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), MsgPicker.getMsg().getNoStreamingModelFound());
        }
    }

    @Test
    public void testHybridSegmentOverRange() throws SqlParseException {
        String project = "streaming_test";
        String sql = "select count(*) from SSB_STREAMING where LO_PARTITIONCOLUMN > '2021-07-01 00:00:00'";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals("model_streaming", context.getRealization().getModel().getAlias());
        Assert.assertTrue(context.getStorageContext().isDataSkipped());

        String sql2 = "select count(*) from SSB_STREAMING where LO_PARTITIONCOLUMN >= '2021-05-28 15:25:00'";
        OlapContext context2 = OlapContextTestUtil.getOlapContexts(project, sql2).get(0);
        RealizationChooser.attemptSelectCandidate(context2);
        Assert.assertEquals("model_streaming", context2.getRealization().getModel().getAlias());
        Assert.assertTrue(context2.getStorageContext().isBatchCandidateEmpty());
        Assert.assertFalse(context2.getStorageContext().isStreamCandidateEmpty());
        Assert.assertEquals(20001L, context2.getStorageContext().getStreamCandidate().getLayoutId());
    }

    @Test
    public void testHybridNoStreamingRealization() throws SqlParseException {
        String project = "streaming_test";
        String sql1 = "select min(LO_SHIPMODE) from SSB_STREAMING where LO_PARTITIONCOLUMN < '2021-05-01 00:00:00'";
        String sql2 = "select min(LO_SHIPMODE) from SSB_STREAMING where LO_PARTITIONCOLUMN > '2021-05-01 00:00:00'";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql1, true).get(0);
        try {
            RealizationChooser.attemptSelectCandidate(context);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoStreamingRealizationFoundException);
            Assert.assertEquals(((NoStreamingRealizationFoundException) e).getErrorCode().getCodeString(),
                    ServerErrorCode.STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), MsgPicker.getMsg().getNoStreamingModelFound());
        }
        Assert.assertTrue(context.getStorageContext().isStreamCandidateEmpty());

        OlapContext context2 = OlapContextTestUtil.getOlapContexts(project, sql2, true).get(0);
        try {
            RealizationChooser.attemptSelectCandidate(context2);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoStreamingRealizationFoundException);
            Assert.assertEquals(((NoStreamingRealizationFoundException) e).getErrorCode().getCodeString(),
                    ServerErrorCode.STREAMING_MODEL_NOT_FOUND.toErrorCode().getCodeString());
            Assert.assertEquals(e.getMessage(), MsgPicker.getMsg().getNoStreamingModelFound());
        }
        Assert.assertTrue(context2.getStorageContext().isBatchCandidateEmpty());
    }

    @Test
    public void testHybridDimensionAsMetrics() throws SqlParseException {
        String project = "streaming_test";
        String sql = "select min(LO_CUSTKEY) from SSB_STREAMING";
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        RealizationChooser.attemptSelectCandidate(context);
        Assert.assertEquals(30001L, context.getStorageContext().getStreamCandidate().getLayoutId());
        Assert.assertEquals(30001L, context.getStorageContext().getBatchCandidate().getLayoutId());
    }

    // https://olapio.atlassian.net/browse/AL-9077
    @Test
    public void testJoinSameTableWithAlias() throws SqlParseException {
        String project = "streaming_test";
        String sql = "SELECT P3.C_CUSTKEY, P4.C_CUSTKEY FROM SSB.P_LINEORDER P1 JOIN SSB.CUSTOMER P3 "
                + "ON P1.LO_CUSTKEY = P3.C_CUSTKEY JOIN SSB.CUSTOMER P4 ON P1.LO_SUPPKEY = P4.C_CUSTKEY "
                + "WHERE P3.C_CUSTKEY in(8) and P4.C_CUSTKEY in(2) GROUP BY P3.C_CUSTKEY, P4.C_CUSTKEY";
        AccelerationUtil.runWithSmartContext(getTestConfig(), project, new String[] { sql }, true);
        OlapContext context = OlapContextTestUtil.getOlapContexts(project, sql).get(0);
        List<String> filterConditions = context.getExpandedFilterConditions().stream().map(RexNode::toString)
                .collect(Collectors.toList());
        Assert.assertEquals(2, filterConditions.size());
        // SSB.CUSTOMER_1.C_CUSTKEY & SSB.CUSTOMER.C_CUSTKEY
        Assert.assertNotEquals(filterConditions.get(0).split(",")[0], filterConditions.get(1).split(",")[0]);
    }
}
