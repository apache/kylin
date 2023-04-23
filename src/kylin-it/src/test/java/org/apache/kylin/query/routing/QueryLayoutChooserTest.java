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

package org.apache.kylin.query.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryLayoutChooserTest extends NLocalWithSparkSessionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testCCNullChecking() throws SqlParseException {
        // match aggIndex - null in group by cols
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        {
            String sql = "select distinct DEAL_AMOUNT from test_kylin_fact \n";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);

            // model with computedColumns
            String modelWithCCId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
            NDataflow dataflow = dataflowManager.getDataflow(modelWithCCId);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);

            // model without computedColumns
            String modelWithNoCCId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
            NDataflow dataflowNoCC = dataflowManager.getDataflow(modelWithNoCCId);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflowNoCC,
                    dataflowNoCC.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNull(layoutCandidate);
        }

        // match aggIndex - null in agg col
        {
            String sql = "select sum(DEAL_AMOUNT) from test_kylin_fact \n";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);

            // model with computedColumns
            String modelWithCCId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
            NDataflow dataflow = dataflowManager.getDataflow(modelWithCCId);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);

            // model without computedColumns
            String modelWithNoCCId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
            NDataflow dataflowNoCC = dataflowManager.getDataflow(modelWithNoCCId);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflowNoCC,
                    dataflowNoCC.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNull(layoutCandidate);
        }

        // match tableIndex
        {
            String sql = "select DEAL_AMOUNT from test_kylin_fact \n";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);

            // model with computedColumns
            String modelWithCCId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
            NDataflow dataflow = dataflowManager.getDataflow(modelWithCCId);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);

            // model without computedColumns
            String modelWithNoCCId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
            NDataflow dataflowNoCC = dataflowManager.getDataflow(modelWithNoCCId);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflowNoCC,
                    dataflowNoCC.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNull(layoutCandidate);
        }
    }

    @Test
    public void testSelectIndexInOneModel() throws SqlParseException {
        // prepare metadata
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataSegDetails segDetails = dataflowManager.getDataflow(modelId).getLatestReadySegment().getSegDetails();
            NDataLayout lowestCostLayout = NDataLayout.newDataLayout(segDetails, 10001L);
            lowestCostLayout.setRows(1000L);

            // update
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            dataflowUpdate.setToAddOrUpdateLayouts(lowestCostLayout);
            dataflowManager.updateDataflow(dataflowUpdate);
            return null;
        }, getProject());

        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        // 1. test match aggIndex
        {
            String sql = "select CAL_DT, count(price) as GMV from test_kylin_fact \n"
                    + " where CAL_DT='2012-01-10' group by CAL_DT ";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(10001L, layoutCandidate.getLayoutEntity().getId());
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
            Assert.assertEquals(1000.0D, layoutCandidate.getCost(), 0.01);
        }

        // 2. tableIndex match
        {
            String sql = "select CAL_DT from test_kylin_fact where CAL_DT='2012-01-10'";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertTrue(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }
    }

    /**
     * This method used for validating the logic of NQueryLayoutChooser#filterColumnComparator.
     */
    @Test
    public void testFilterColsAffectIndexSelection() throws SqlParseException {
        // prepare metadata
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanAlreadyExistingLayoutsInSegments(modelId);
        addDesiredLayoutsToIndexPlanAndSegments(getProject(), modelId);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);

        {
            String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                    + " where CAL_DT='2012-01-10' and TRANS_ID > 10000 group by CAL_DT, TRANS_ID ";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(1010001, layoutCandidate.getLayoutEntity().getId());
        }

        {
            String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                    + " where CAL_DT > '2012-01-10' and TRANS_ID = 10000 group by CAL_DT, TRANS_ID ";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(1010002, layoutCandidate.getLayoutEntity().getId());
        }

        // same filter level, select the col with the smallest cardinality
        {
            String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                    + " where CAL_DT = '2012-01-10' and TRANS_ID = 10000 group by CAL_DT, TRANS_ID ";
            mockTableStats();
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(1010002, layoutCandidate.getLayoutEntity().getId());
        }
    }

    private void addDesiredLayoutsToIndexPlanAndSegments(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
            indexMgr.updateIndexPlan(modelId, copyForWrite -> {
                // prepare one index with three layouts have different colOrder
                IndexEntity oneIndex = new IndexEntity();
                long nextAggregationIndexId = copyForWrite.getNextAggregationIndexId();
                oneIndex.setId(nextAggregationIndexId);
                oneIndex.setDimensions(Lists.newArrayList(1, 2, 3));
                oneIndex.setMeasures(Lists.newArrayList(100000));
                LayoutEntity newLayout1 = new LayoutEntity();
                newLayout1.setId(oneIndex.getId() + 1);
                newLayout1.setAuto(true);
                newLayout1.setColOrder(Lists.newArrayList(2, 1, 3, 100000));
                LayoutEntity newLayout2 = new LayoutEntity();
                newLayout2.setId(oneIndex.getId() + 2);
                newLayout2.setAuto(true);
                newLayout2.setColOrder(Lists.newArrayList(1, 2, 3, 100000));
                oneIndex.setLayouts(Lists.newArrayList(newLayout1, newLayout2));

                // prepare another index
                IndexEntity anotherIndex = new IndexEntity();
                anotherIndex.setId(nextAggregationIndexId + IndexEntity.INDEX_ID_STEP);
                anotherIndex.setDimensions(Lists.newArrayList(1, 2, 3, 4));
                anotherIndex.setMeasures(Lists.newArrayList(100000));
                LayoutEntity newLayout = new LayoutEntity();
                newLayout.setId(anotherIndex.getId() + 1);
                newLayout.setAuto(true);
                newLayout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000));
                anotherIndex.setLayouts(Lists.newArrayList(newLayout));

                // add two indexes to the IndexPlan
                List<IndexEntity> indexes = copyForWrite.getIndexes();
                indexes.add(oneIndex);
                indexes.add(anotherIndex);
            });

            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataSegment latestSegment = dfMgr.getDataflow(modelId).getLatestReadySegment();
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            NDataLayout layout1 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1010001L);
            layout1.setRows(1000L);
            NDataLayout layout2 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1010002L);
            layout2.setRows(1000L);
            NDataLayout layout3 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1020001L);
            layout3.setRows(1000L);
            dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2, layout3);
            dfMgr.updateDataflow(dataflowUpdate);
            return null;
        }, project);
    }

    @Test
    public void testDerivedColsSelection() throws SqlParseException {
        // prepare metadata
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanAlreadyExistingLayoutsInSegments(modelId);
        mockDerivedIndex(getProject(), modelId);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);

        String sql = "select test_kylin_fact.lstg_format_name, META_CATEG_NAME, count(*) as TRANS_CNT \n"
                + " from test_kylin_fact \n" //
                + "left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + " left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + " left JOIN edw.test_sites as test_sites ON test_kylin_fact.lstg_site_id = test_sites.site_id\n"
                + " group by test_kylin_fact.lstg_format_name, META_CATEG_NAME";
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(1010001L, layoutCandidate.getLayoutEntity().getId());
    }

    private void mockDerivedIndex(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
            indexMgr.updateIndexPlan(modelId, copyForWrite -> {
                // one index
                IndexEntity oneIndex = new IndexEntity();
                long nextAggregationIndexId = copyForWrite.getNextAggregationIndexId();
                oneIndex.setId(nextAggregationIndexId);
                oneIndex.setDimensions(Lists.newArrayList(1, 3, 4, 5, 8));
                oneIndex.setMeasures(Lists.newArrayList(100000));
                LayoutEntity newLayout1 = new LayoutEntity();
                newLayout1.setId(oneIndex.getId() + 1);
                newLayout1.setAuto(true);
                newLayout1.setColOrder(Lists.newArrayList(3, 1, 5, 4, 8, 100000));
                oneIndex.setLayouts(Lists.newArrayList(newLayout1));

                // another index
                IndexEntity anotherIndex = new IndexEntity();
                anotherIndex.setId(nextAggregationIndexId + IndexEntity.INDEX_ID_STEP);
                anotherIndex.setDimensions(Lists.newArrayList(1, 3, 4, 8));
                anotherIndex.setMeasures(Lists.newArrayList(100000));
                LayoutEntity newLayout2 = new LayoutEntity();
                newLayout2.setId(anotherIndex.getId() + 1);
                newLayout2.setAuto(true);
                newLayout2.setColOrder(Lists.newArrayList(3, 1, 4, 8, 100000));
                anotherIndex.setLayouts(Lists.newArrayList(newLayout2));

                // add two indexes
                List<IndexEntity> indexes = copyForWrite.getIndexes();
                indexes.add(oneIndex);
                indexes.add(anotherIndex);
            });

            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            NDataSegment latestSegment = dfMgr.getDataflow(modelId).getLatestReadySegment();
            NDataLayout layout1 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1010001L);
            layout1.setRows(1000L);
            NDataLayout layout2 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1020001L);
            layout2.setRows(1000L);
            dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2);
            dfMgr.updateDataflow(dataflowUpdate);
            return null;
        }, project);
    }

    @Test
    public void testDerivedDimWhenModelHasMultipleSameDimTable() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject())
                .getDataflow("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");

        // prepare table desc snapshot path
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject());
        TableDesc tableDesc = tblMgr.getTableDesc("DEFAULT.TEST_ACCOUNT");
        tableDesc.setLastSnapshotPath(
                "default/table_snapshot/DEFAULT.TEST_ACCOUNT/d6ba492b-13bf-444d-b6e3-71bfa903344d");

        String sql = "select b.ACCOUNT_BUYER_LEVEL from \"DEFAULT\".\"TEST_KYLIN_FACT\" a\n"
                + "left join \"DEFAULT\".\"TEST_ACCOUNT\" b on a.SELLER_ID = b.ACCOUNT_ID";
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(20000000001L, layoutCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testSumExprWithAggPushDownEnabled() throws SqlParseException {
        getTestConfig().setProperty("kylin.query.convert-sum-expression-enabled", "true");
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        String sql = "select  sum( " //
                + "( case  when (  case when (lineorder.lo_orderdate = t0.x_measure__0) then true\n"
                + " when not (lineorder.lo_orderdate = t0.x_measure__0) then false\n" //
                + " else null end ) then lineorder.lo_quantity\n" //
                + " else cast(null as integer)  end  )  ) as sum_lo_quantity_sum______88\n" //
                + "from  ssb.lineorder lineorder  cross join (\n" //
                + " select  max(lineorder.lo_orderdate) as x_measure__0\n" //
                + " from  ssb.lineorder lineorder group by  1.1000000000000001 ) t0\n"
                + "group by  1.1000000000000001\n";

        getTestConfig().setProperty("kylin.query.calcite.aggregate-pushdown-enabled", "true");
        List<OLAPContext> olapContexts = OlapContextTestUtil.getHepRulesOptimizedOlapContexts(getProject(), sql, false);

        // validate the first
        OLAPContext oneOlapContext = olapContexts.get(0);
        Map<String, String> oneMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), oneOlapContext);
        oneOlapContext.fixModel(dataflow.getModel(), oneMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), oneOlapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(1L, layoutCandidate.getLayoutEntity().getId());

        // validate the second
        OLAPContext anotherOlapContext = olapContexts.get(1);
        Map<String, String> anotherMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), anotherOlapContext);
        anotherOlapContext.fixModel(dataflow.getModel(), anotherMap);
        NLayoutCandidate anotherCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), anotherOlapContext.getSQLDigest(), null);
        Assert.assertNotNull(anotherCandidate);
        Assert.assertEquals(1L, anotherCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testSumExprWithAggPushDownDisabled() throws SqlParseException {

        getTestConfig().setProperty("kylin.query.convert-sum-expression-enabled", "true");
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        String sql = "select  sum( " //
                + "( case  when (  case when (lineorder.lo_orderdate = t0.x_measure__0) then true\n"
                + " when not (lineorder.lo_orderdate = t0.x_measure__0) then false\n" //
                + " else null end ) then lineorder.lo_quantity\n" //
                + " else cast(null as integer)  end  )  ) as sum_lo_quantity_sum______88\n" //
                + "from  ssb.lineorder lineorder  cross join (\n" //
                + " select  max(lineorder.lo_orderdate) as x_measure__0\n" //
                + " from  ssb.lineorder lineorder group by  1.1000000000000001 ) t0\n"
                + "group by  1.1000000000000001\n";

        getTestConfig().setProperty("kylin.query.calcite.aggregate-pushdown-enabled", "false");
        List<OLAPContext> olapContexts = OlapContextTestUtil.getHepRulesOptimizedOlapContexts(getProject(), sql, false);

        // validate the first
        OLAPContext oneOlapContext = olapContexts.get(0);
        Map<String, String> oneMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), oneOlapContext);
        oneOlapContext.fixModel(dataflow.getModel(), oneMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), oneOlapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(1L, layoutCandidate.getLayoutEntity().getId());

        // validate the second
        OLAPContext anotherOlapContext = olapContexts.get(1);
        Map<String, String> anotherMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), anotherOlapContext);
        anotherOlapContext.fixModel(dataflow.getModel(), anotherMap);
        NLayoutCandidate anotherCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), anotherOlapContext.getSQLDigest(), null);
        Assert.assertNotNull(anotherCandidate);
        Assert.assertEquals(20000000001L, anotherCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testShardByCol() throws SqlParseException {
        // prepare metadata
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanAlreadyExistingLayoutsInSegments(modelId);
        // 1010001 no shard, dims=[trans_id, cal_dt, lstg_format_name]
        // 1010002 shard=trans_id, dims=[cal_dt, trans_id, lstg_format_name]
        // 1010003 shard=cal_dt, dims=[cal_dt, trans_id, lstg_format_name]
        // cardinality: cal_dt = 1000
        // cardinality: trans_id = 10000
        mockShardByLayout(getProject(), modelId);
        mockTableStats();
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);

        {
            String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                    + " where CAL_DT = '2012-01-10' and TRANS_ID = 10000 group by CAL_DT, TRANS_ID ";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);

            // hit layout 1010002
            // 1. shardby layout are has higher priority over non-shardby layout, 
            // so 1010001 is skipped, although it has a better dim order
            // 2. trans_id has a higher cardinality, so 1010002 with shard on trans_id 
            // is preferred over 1010003 with shard on cal_dt
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(1010002, layoutCandidate.getLayoutEntity().getId());
        }

        {
            String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                    + " where CAL_DT = '2012-01-10' and TRANS_ID > 10000 group by CAL_DT, TRANS_ID ";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(1010003, layoutCandidate.getLayoutEntity().getId());
        }
    }

    private void mockShardByLayout(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), getProject());
            indexMgr.updateIndexPlan(modelId, copyForWrite -> {
                IndexEntity oneIndex = new IndexEntity();
                oneIndex.setId(copyForWrite.getNextAggregationIndexId());
                oneIndex.setDimensions(Lists.newArrayList(1, 2, 3));
                oneIndex.setMeasures(Lists.newArrayList(100000));
                // mock no shardby column
                LayoutEntity newLayout1 = new LayoutEntity();
                newLayout1.setId(oneIndex.getId() + 1);
                newLayout1.setAuto(true);
                newLayout1.setColOrder(Lists.newArrayList(1, 2, 3, 100000));
                // mock shardby trans_id
                LayoutEntity newLayout2 = new LayoutEntity();
                newLayout2.setId(oneIndex.getId() + 2);
                newLayout2.setAuto(true);
                newLayout2.setColOrder(Lists.newArrayList(2, 1, 3, 100000));
                newLayout2.setShardByColumns(Lists.newArrayList(1));
                //mock shardby cal_dt
                LayoutEntity newLayout3 = new LayoutEntity();
                newLayout3.setId(oneIndex.getId() + 3);
                newLayout3.setAuto(true);
                newLayout3.setColOrder(Lists.newArrayList(2, 1, 3, 100000));
                newLayout3.setShardByColumns(Lists.newArrayList(2));
                oneIndex.setLayouts(Lists.newArrayList(newLayout1, newLayout2, newLayout3));
                copyForWrite.getIndexes().add(oneIndex);
            });
            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataSegment latestSegment = dfMgr.getDataflow(modelId).getLatestReadySegment();
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            NDataLayout layout1 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1010001L);
            layout1.setRows(1000L);
            NDataLayout layout2 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1010002L);
            layout2.setRows(1000L);
            NDataLayout layout3 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1010003L);
            layout3.setRows(1000L);
            dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2, layout3);
            dfMgr.updateDataflow(dataflowUpdate);
            return null;
        }, project);
    }

    @Test
    public void testUnmatchedCountColumn() throws SqlParseException {
        overwriteSystemProp("kylin.query.replace-count-column-with-count-star", "true");
        String modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        String sql = "select avg(TEST_KYLIN_FACT.ITEM_COUNT) from TEST_KYLIN_FACT";
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);

        Assert.assertNotNull(layoutCandidate);
        List<NDataModel.Measure> allMeasures = dataflow.getModel().getAllMeasures();
        Assert.assertTrue(containMeasure(allMeasures, "COUNT", "1"));
        Assert.assertTrue(containMeasure(allMeasures, "SUM", "DEFAULT.TEST_KYLIN_FACT.PRICE"));
        Assert.assertFalse(containMeasure(allMeasures, "COUNT", "DEFAULT.TEST_KYLIN_FACT.PRICE"));
    }

    /**
     * Start with an empty project to get the OlapContexts not pollute is very important.
     */
    @Test
    public void testTableIndexAndAggIndex() throws SqlParseException {
        String emptyProject = "newten";
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        String project = "table_index";
        String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);

        {
            String sql = "select sum(ORDER_ID) from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }

        {
            String sql = "select max(ORDER_ID) from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }

        {
            String sql = "select min(ORDER_ID) from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }

        {
            String sql = "select count(ORDER_ID) from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }

        {
            String sql = "select count(distinct ORDER_ID) from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }

        {
            String sql = "select collect_set(ORDER_ID) from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNull(layoutCandidate);
        }

        {
            getTestConfig().setProperty("kylin.engine.segment-online-mode", "ANY");
            String sql = "select max(PRICE)from TEST_KYLIN_FACT";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(emptyProject, sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertTrue(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }
    }

    /**
     * When the useTableIndexAnswerNonRawQuery configuration is enabled, if the TableIndex has a lower cost,
     * it will be used to answer the query. However, if the isPreferAggIndex configuration is enabled too,
     * the aggregate query will be used to respond.
     */
    @Test
    public void testPreferAggIndexEnabled() throws SqlParseException {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
        String project = "table_index";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
            NDataSegment latestSegment = dfMgr.getDataflow(modelId).getLatestReadySegment();
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            NDataLayout layout1 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 1L);
            layout1.setRows(1000L);
            NDataLayout layout2 = NDataLayout.newDataLayout(latestSegment.getSegDetails(), 20000000001L);
            layout2.setRows(100L);
            dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2);
            dfMgr.updateDataflow(dataflowUpdate);
            return null;
        }, project);
        String sql = "select LSTG_FORMAT_NAME,count(*) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME";

        {
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> tableAlias2ModelAliasMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), tableAlias2ModelAliasMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertFalse(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }

        {
            overwriteSystemProp("kylin.query.layout.prefer-aggindex", "false");
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> tableAlias2ModelAliasMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), tableAlias2ModelAliasMap);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertTrue(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
        }
    }

    @Test
    public void testTableIndexAnswerAggQueryUseProjectConfig() throws SqlParseException {
        String project = "table_index";
        MetadataTestUtils.updateProjectConfig(project, "kylin.query.use-tableindex-answer-non-raw-query", "true");
        String sql = "select max(PRICE)from TEST_KYLIN_FACT";
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);

        String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertTrue(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
    }

    @Test
    public void testTableIndexAnswerNonRawQueryQueryUseModelConfig() throws SqlParseException {
        String project = "table_index";
        MetadataTestUtils.updateProjectConfig(project, "kylin.query.use-tableindex-answer-non-raw-query", "false");
        String uuid = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(uuid);
        indexMgr.updateIndexPlan(uuid, copyForWrite -> {
            LinkedHashMap<String, String> props = copyForWrite.getOverrideProps();
            props.put("kylin.query.use-tableindex-answer-non-raw-query", "true");
            copyForWrite.setOverrideProps(props);
        });
        String sql = "select max(PRICE) from TEST_KYLIN_FACT";
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);

        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertTrue(layoutCandidate.getLayoutEntity().getIndex().isTableIndex());
    }

    @Test
    public void testMatchJoinWithFilter() throws SqlParseException {
        final List<String> filters = ImmutableList.of(" b.SITE_NAME is not null",
                " b.SITE_NAME is not null and b.SITE_NAME is null", " b.SITE_NAME = '英国'", " b.SITE_NAME < '英国'",
                " b.SITE_NAME > '英国'", " b.SITE_NAME >= '英国'", " b.SITE_NAME <= '英国'", " b.SITE_NAME <> '英国'",
                " b.SITE_NAME like '%英国%'", " b.SITE_NAME not like '%英国%'", " b.SITE_NAME not in ('英国%')",
                " b.SITE_NAME similar to '%英国%'", " b.SITE_NAME not similar to '%英国%'",
                " b.SITE_NAME is not distinct from '%英国%'", " b.SITE_NAME between '1' and '2'",
                " b.SITE_NAME not between '1' and '2'", " b.SITE_NAME <= '英国' OR b.SITE_NAME >= '英国'",
                " b.SITE_NAME = '英国' is not false", " b.SITE_NAME = '英国' is not true", " b.SITE_NAME = '英国' is false",
                " b.SITE_NAME = '英国' is true");
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        for (String filter : filters) {
            String sql = "select CAL_DT from test_kylin_fact a inner join EDW.test_sites b \n"
                    + " on a.LSTG_SITE_ID = b.SITE_ID where " + filter;
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelName);
            NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                    dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
            Assert.assertNotNull(layoutCandidate);
            Assert.assertEquals(20000010001L, layoutCandidate.getLayoutEntity().getId());
        }
    }

    @Test
    public void testMatchJoinWithEnhancedMode() throws SqlParseException {
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        String sql = "SELECT \n" + "COUNT(\"TEST_KYLIN_FACT\".\"SELLER_ID\")\n" + "FROM \n"
                + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n" // left or inner join
                + "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND "
                + "\"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n" // left or inner join
                + "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n" // left or inner join
                + "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\"\n"
                + "GROUP BY \"TEST_KYLIN_FACT\".\"TRANS_ID\"";
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), olapContext.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(1L, layoutCandidate.getLayoutEntity().getId());
    }

    public boolean containMeasure(List<NDataModel.Measure> allMeasures, String expression, String parameter) {
        for (NDataModel.Measure measure : allMeasures) {
            if (measure.getFunction().getExpression().equals(expression)
                    && measure.getFunction().getParameters().get(0).toString().equals(parameter)) {
                return true;
            }
        }
        return false;
    }

    private void cleanAlreadyExistingLayoutsInSegments(String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataSegDetails segDetails = dfMgr.getDataflow(modelId).getLatestReadySegment().getSegDetails();
            List<NDataLayout> allLayouts = segDetails.getAllLayouts();

            // update
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            dataflowUpdate.setToRemoveLayouts(allLayouts.toArray(new NDataLayout[0]));
            dfMgr.updateDataflow(dataflowUpdate);
            return null;
        }, getProject());
    }

    private void mockTableStats() {
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        TableDesc tableDesc = tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        TableExtDesc tableExt = tableManager.getOrCreateTableExt(tableDesc);
        tableExt = tableManager.copyForWrite(tableExt);
        List<TableExtDesc.ColumnStats> columnStats = Lists.newArrayList();

        for (ColumnDesc columnDesc : tableDesc.getColumns()) {
            if (columnDesc.isComputedColumn()) {
                continue;
            }
            TableExtDesc.ColumnStats colStats = tableExt.getColumnStatsByName(columnDesc.getName());
            if (colStats == null) {
                colStats = new TableExtDesc.ColumnStats();
                colStats.setColumnName(columnDesc.getName());
            }
            if ("CAL_DT".equals(columnDesc.getName())) {
                colStats.setCardinality(1000);
            } else if ("TRANS_ID".equals(columnDesc.getName())) {
                colStats.setCardinality(10000);
            } else {
                colStats.setCardinality(100);
            }
            columnStats.add(colStats);
        }
        tableExt.setColumnStats(columnStats);
        tableManager.saveTableExt(tableExt);
    }
}
