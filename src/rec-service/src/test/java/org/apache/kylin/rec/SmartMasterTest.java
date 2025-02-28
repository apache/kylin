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

package org.apache.kylin.rec;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class SmartMasterTest extends AutoTestOnLearnKylinData {

    @Test
    public void testDoProposeWhenPushDownIsDisabled() {
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), proj);
        // the project does not contain any realization
        Assert.assertEquals(0, modelManager.listAllModels().size());
        NDataModel model1, model2;
        {
            String[] sqlStatements = new String[] {
                    "SELECT f.leaf_categ_id FROM kylin_sales f left join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN'" };
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqlStatements, true);
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            model1 = modelContext.getTargetModel();
            Assert.assertEquals(1, modelManager.listAllModels().size());
            Assert.assertNotNull(modelManager.getDataModelDesc(model1.getId()));
        }

        {
            String[] sqlStatements = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                    + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                    + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                    + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqlStatements, true);
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            model2 = modelContext.getTargetModel();
            Assert.assertEquals(3, modelManager.listAllModels().size());
            Assert.assertNotNull(modelManager.getDataModelDesc(model2.getId()));
        }
    }

    @Test
    public void testRenameModel() {
        String[] sqlStatements1 = new String[] {
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" };
        AbstractContext context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqlStatements1, true);
        AbstractContext.ModelContext modelContext1 = context1.getModelContexts().get(0);
        NDataModel model1 = modelContext1.getTargetModel();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_1", model1.getAlias());

        // reuse model, without propose name
        String[] sqlStatements2 = new String[] {
                "SELECT f.leaf_categ_id FROM kylin_sales f left join KYLIN_CATEGORY_GROUPINGS o "
                        + "on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id "
                        + "WHERE f.lstg_format_name = 'ABIN'" };
        val context2 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqlStatements2, true);
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        NDataModel model2 = modelContext2.getTargetModel();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_1", model2.getAlias());

        // mock user change name alias to lower case
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), proj);
        modelManager.updateDataModel(model2.getUuid(), copyForWrite -> {
            String newAlias = copyForWrite.getAlias().toLowerCase(Locale.ROOT);
            copyForWrite.setAlias(newAlias);
        });
        NDataModel model = modelManager.getDataModelDesc(model2.getUuid());
        Assert.assertEquals("auto_model_kylin_sales_1", model.getAlias());

        //
        String[] sqlStatements3 = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o "
                + "on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
        val context3 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqlStatements3, true);
        Assert.assertEquals(2, context3.getModelContexts().size());
        AbstractContext.ModelContext modelContext3 = context3.getModelContexts().get(0);
        NDataModel model3 = modelContext3.getTargetModel();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_2", model3.getAlias());
        AbstractContext.ModelContext modelContext4 = context3.getModelContexts().get(1);
        NDataModel model4 = modelContext4.getTargetModel();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_3", model4.getAlias());
    }

    @Test
    public void testLoadingModelCannotOffline() {
        String[] sqls = { "select item_count, sum(price) from kylin_sales group by item_count" };
        val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
        Assert.assertFalse(context1.getAccelerateInfoMap().get(sqls[0]).isNotSucceed());

        // set model maintain type to semi-auto
        MetadataTestUtils.toSemiAutoMode(proj);

        // update existing model to offline
        NDataModel targetModel = context1.getModelContexts().get(0).getTargetModel();
        NDataflowManager dataflowMgr = NDataflowManager.getInstance(getTestConfig(), proj);
        NDataflow dataflow = dataflowMgr.getDataflow(targetModel.getUuid());
        NDataflowUpdate copiedDataFlow = new NDataflowUpdate(dataflow.getUuid());
        copiedDataFlow.setStatus(RealizationStatusEnum.OFFLINE);
        dataflowMgr.updateDataflow(copiedDataFlow);

        // propose in semi-auto-mode
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), proj, sqls);

        String expectedPendingMsg = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        AccelerateInfo accelerateInfo = context2.getAccelerateInfoMap().get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertEquals(expectedPendingMsg, accelerateInfo.getPendingMsg());
    }

    @Test
    public void testCountDistinctTwoParamColumn() {
        // case1 
        {
            String[] sqls = new String[] { "SELECT part_dt, SUM(price) AS GMV, COUNT(1) AS TRANS_CNT,\n"
                    + "COUNT(DISTINCT lstg_format_name), COUNT(DISTINCT seller_id, lstg_format_name) AS DIST_SELLER_FORMAT\n"
                    + "FROM kylin_sales GROUP BY part_dt" };
            val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            final List<NDataModel.Measure> allMeasures = context1.getModelContexts().get(0).getTargetModel()
                    .getAllMeasures();
            Assert.assertEquals(4, allMeasures.size());

            final NDataModel.Measure measure3 = allMeasures.get(2);
            Assert.assertEquals("COUNT_DISTINCT_KYLIN_SALES_LSTG_FORMAT_NAME", measure3.getName());
            Assert.assertEquals(1, measure3.getFunction().getParameterCount());
            Assert.assertEquals("bitmap", measure3.getFunction().getReturnDataType().getName());

            final NDataModel.Measure measure4 = allMeasures.get(3);
            Assert.assertEquals("COUNT_DISTINCT_KYLIN_SALES_SELLER_ID_KYLIN_SALES_LSTG_FORMAT_NAME",
                    measure4.getName());
            Assert.assertEquals(2, measure4.getFunction().getParameterCount());
            Assert.assertEquals("hllc", measure4.getFunction().getReturnDataType().getName());
        }

        // case2
        {
            String[] sqls = new String[] {
                    "SELECT COUNT(DISTINCT META_CATEG_NAME) AS CNT, MAX(META_CATEG_NAME) AS max_name\n"
                            + "FROM kylin_category_groupings" };
            AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            final List<NDataModel.Measure> allMeasures = context.getModelContexts().get(0).getTargetModel()
                    .getAllMeasures();
            Assert.assertEquals(3, allMeasures.size());

            final NDataModel.Measure measure1 = allMeasures.get(1);
            Assert.assertEquals("COUNT_DISTINCT_KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME", measure1.getName());
            Assert.assertEquals(1, measure1.getFunction().getParameterCount());
            Assert.assertEquals("bitmap", measure1.getFunction().getReturnDataType().getName());

            final NDataModel.Measure measure2 = allMeasures.get(2);
            Assert.assertEquals("MAX_KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME", measure2.getName());
            Assert.assertEquals(1, measure2.getFunction().getParameterCount());
            Assert.assertEquals("varchar", measure2.getFunction().getReturnDataType().getName());
        }
    }

    @Test
    public void testOneSqlToManyLayouts() {
        String[] sqls = new String[] { "select a.*, kylin_sales.lstg_format_name as lstg_format_name \n"
                + "from ( select part_dt, sum(price) as sum_price from kylin_sales\n"
                + "         where part_dt > '2010-01-01' group by part_dt) a \n"
                + "join kylin_sales on a.part_dt = kylin_sales.part_dt \n"
                + "group by lstg_format_name, a.part_dt, a.sum_price" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertEquals(1, context.getModelContexts().size());
        Assert.assertEquals(1, accelerateInfoMap.size());
        Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = accelerateInfoMap.get(sqls[0]).getRelatedLayouts();
        Assert.assertEquals(2, relatedLayouts.size());
    }

    @Test
    public void testMaintainModelTypeWithNoInitialModel() {
        // set to manual model type
        MetadataTestUtils.toPureExpertMode(proj);

        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), lstg_format_name, sum(price) from kylin_sales \n"
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name"

        };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), proj, sqls);
        final AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        // null validation
        final NDataModel originalModel = modelContext.getOriginModel();
        final IndexPlan originalIndexPlan = modelContext.getOriginIndexPlan();
        Assert.assertNull(originalModel);
        Assert.assertNull(originalIndexPlan);

        final NDataModel targetModel = modelContext.getTargetModel();
        final IndexPlan targetIndexPlan = modelContext.getTargetIndexPlan();
        Assert.assertNull(targetModel);
        Assert.assertNull(targetIndexPlan);
    }

    @Test
    public void testManualMaintainType() {
        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            final NDataModel targetModel = context.getModelContexts().get(0).getTargetModel();
            final List<JoinTableDesc> joinTables = targetModel.getJoinTables();
            Assert.assertTrue(CollectionUtils.isEmpty(joinTables));
        }

        // set maintain model type to pure-expert-mode
        MetadataTestUtils.toPureExpertMode(proj);
        {
            // -------------- add extra table -----------------------
            sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                    + " left join kylin_cal_dt on cal_dt = part_dt \n"
                    + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            val context = AccelerationUtil.runModelReuseContext(getTestConfig(), proj, sqls);

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
            final String pendingMsg = accelerateInfoMap.get(sqls[0]).getPendingMsg();
            Assert.assertNotNull(pendingMsg);
            Assert.assertEquals(expectedMessage, pendingMsg);
            Assert.assertNull(context.getModelContexts().get(0).getTargetModel());
        }

        // set maintain model type to semi-auto-mode
        MetadataTestUtils.toSemiAutoMode(proj);
        {
            // -------------- add extra table -----------------------
            sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                    + " left join kylin_cal_dt on cal_dt = part_dt \n"
                    + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            val context = AccelerationUtil.runModelReuseContext(getTestConfig(), proj, sqls);

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
            final String pendingMsg = accelerateInfoMap.get(sqls[0]).getPendingMsg();
            Assert.assertNotNull(pendingMsg);
            Assert.assertEquals(expectedMessage, pendingMsg);
            Assert.assertNull(context.getModelContexts().get(0).getTargetModel());
        }
    }

    @Test
    public void testInitTargetModelError() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);

        smartMaster.getProposer("SQLAnalysisProposer").execute();

        // after cutting context, change "DEFAULT.KYLIN_CAL_DT" to be a incremental load table
        val tableManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        val table = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        table.setIncrementLoading(true);
        tableManager.updateTableDesc(table);

        smartMaster.getProposer("ModelSelectProposer").execute();
        smartMaster.getProposer("ModelOptProposer").execute();
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        smartMaster.getProposer("IndexPlanOptProposer").execute();
        context.saveMetadata();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        final AccelerateInfo accelerateInfo = Lists.newArrayList(accelerateInfoMap.values()).get(0);
        Assert.assertTrue(accelerateInfo.isFailed());
        Assert.assertEquals("Only one incremental loading table can be set in model!",
                accelerateInfo.getFailedCause().getMessage());
    }

    @Test
    public void testWithoutSaveModel() {
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.executePropose();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        Assert.assertFalse(Lists.newArrayList(accelerateInfoMap.values()).get(0).isFailed());
    }

    @Test
    public void testProposeOnExistingRuleBasedIndexPlan() {
        String[] sqls = new String[] { "select CAL_DT from KYLIN_CAL_DT group by CAL_DT limit 10" };

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "rule_based", sqls, true);

        final List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        final AbstractContext.ModelContext modelContext = modelContexts.get(0);
        val originalAllIndexesMap = modelContext.getOriginIndexPlan().getAllIndexesMap();
        val originalWhiteListIndexesMap = modelContext.getOriginIndexPlan().getWhiteListIndexesMap();
        Assert.assertEquals(1, originalAllIndexesMap.size());
        Assert.assertEquals(0, originalWhiteListIndexesMap.size());

        val targetAllIndexesMap = modelContext.getTargetIndexPlan().getAllIndexesMap();
        Assert.assertEquals(originalAllIndexesMap, targetAllIndexesMap);
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();

        final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
    }

    @Test
    public void testRenameAllColumns() {
        // test all named columns rename
        String[] sqlStatements = new String[] { "SELECT\n"
                + "BUYER_ACCOUNT.ACCOUNT_COUNTRY, SELLER_ACCOUNT.ACCOUNT_COUNTRY "
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT \n" + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n"
                + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n"
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n"
                + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n"
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n"
                + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n"
                + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY" };
        val context = new SmartContext(getTestConfig(), "newten", sqlStatements);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        smartMaster.getProposer("ModelSelectProposer").execute();
        smartMaster.getProposer("ModelOptProposer").execute();
        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        String[] namedColumns = model.getAllNamedColumns().stream().map(NDataModel.NamedColumn::getAliasDotColumn)
                .sorted().toArray(String[]::new);

        String namedColumn1 = namedColumns[2];
        String namedColumn2 = namedColumns[7];
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_COUNTRY", namedColumn1);
        Assert.assertEquals("TEST_ACCOUNT_1.ACCOUNT_COUNTRY", namedColumn2);
    }

    @Test
    public void testProposeShortNameOfDimensionAndMeasureAreSame() {
        String[] sqls = new String[] {
                "select test_account.account_id, test_account1.account_id, count(test_account.account_country) \n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id, test_account1.account_id",
                "select test_account.account_id, count(test_account.account_country), count(test_account1.account_country)\n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id",
                "select test_kylin_fact.cal_dt, test_cal_dt.cal_dt, count(price) \n"
                        + "from test_kylin_fact inner join edw.test_cal_dt\n"
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                        + "group by test_kylin_fact.cal_dt, test_cal_dt.cal_dt" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", sqls, true);
        Map<String, AccelerateInfo> accelerationInfoMap = context.getAccelerateInfoMap();
        accelerationInfoMap.forEach((key, value) -> Assert.assertFalse(value.isNotSucceed()));
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(2, modelContexts.size());
        NDataModel model1 = modelContexts.get(0).getTargetModel();
        List<NDataModel.NamedColumn> dimensionList1 = model1.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        Assert.assertEquals("CAL_DT_TEST_CAL_DT", dimensionList1.get(0).getName());
        Assert.assertEquals("TEST_CAL_DT.CAL_DT", dimensionList1.get(0).getAliasDotColumn());
        Assert.assertEquals("CAL_DT_TEST_KYLIN_FACT", dimensionList1.get(1).getName());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", dimensionList1.get(1).getAliasDotColumn());
        List<NDataModel.Measure> measureList1 = model1.getAllMeasures();
        Assert.assertEquals("COUNT_ALL", measureList1.get(0).getName());
        Assert.assertEquals("COUNT_TEST_KYLIN_FACT_PRICE", measureList1.get(1).getName());

        NDataModel model2 = modelContexts.get(1).getTargetModel();
        List<NDataModel.NamedColumn> dimensionList2 = model2.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        Assert.assertEquals("ACCOUNT_ID_TEST_ACCOUNT", dimensionList2.get(0).getName());
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_ID", dimensionList2.get(0).getAliasDotColumn());
        Assert.assertEquals("ACCOUNT_ID_TEST_ACCOUNT1", dimensionList2.get(1).getName());
        Assert.assertEquals("TEST_ACCOUNT1.ACCOUNT_ID", dimensionList2.get(1).getAliasDotColumn());
        List<NDataModel.Measure> measureList2 = model2.getAllMeasures();
        Assert.assertEquals("COUNT_ALL", measureList2.get(0).getName());
        Assert.assertEquals("COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY", measureList2.get(1).getName());
        Assert.assertEquals("COUNT_TEST_ACCOUNT1_ACCOUNT_COUNTRY", measureList2.get(2).getName());

        // mock error case for dimension and this case should be autocorrected when inherited from existed model
        dimensionList1.get(0).setName("TEST_KYLIN_FACT_CAL_DT");
        model1.setMvcc(model1.getMvcc() + 1);
        NDataModelManager.getInstance(getTestConfig(), "newten").updateDataModelDesc(model1);
        val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sqls[2] }, true);
        accelerationInfoMap = context1.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[2]).isFailed());

        // mock error case for measure
        measureList2.get(2).setName("COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY");
        model2.setMvcc(model2.getMvcc() + 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataModelManager.getInstance(getTestConfig(), "newten").updateDataModelDesc(model2);
            return null;
        }, "newten");

        val context2 = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sqls[1] }, true);
        accelerationInfoMap = context2.getAccelerateInfoMap();
        Assert.assertTrue(accelerationInfoMap.get(sqls[1]).isFailed());
        Assert.assertEquals("Duplicate measure name occurs: COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY",
                accelerationInfoMap.get(sqls[1]).getFailedCause().getMessage());
    }

    @Test
    public void testProposeSelectStatementStartsWithParentheses() {
        String sql = "(((SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" <> 'A')) "
                + "UNION ALL SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" = 'A'))) "
                + "UNION ALL SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" > 'A'))) "
                + "UNION ALL SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" IN ('A'))))\n";

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sql }, true);
        Map<String, AccelerateInfo> accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sql).isNotSucceed());
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        final NDataModel targetModel = modelContexts.get(0).getTargetModel();
        final List<NDataModel.NamedColumn> dimensions = targetModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        final List<NDataModel.Measure> allMeasures = targetModel.getAllMeasures();
        Assert.assertEquals(2, dimensions.size());
        Assert.assertEquals(2, allMeasures.size());
    }

    @Test
    public void testProposeNewModel_InSemiMode() {
        String[] sqls = new String[] {
                "select test_account.account_id, test_account1.account_id, count(test_account.account_country) \n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id, test_account1.account_id" };

        // set maintain model type to manual
        MetadataTestUtils.toSemiAutoMode("newten");
        val context = AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", sqls);
        Assert.assertFalse(context.getModelContexts().get(0).isTargetModelMissing());
        val newModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, newModel.getJoinTables().size());
        Assert.assertEquals("TEST_ACCOUNT", newModel.getRootFactTable().getTableName());
        Assert.assertEquals(2, newModel.getAllMeasures().size());
        Assert.assertEquals("COUNT", newModel.getAllMeasures().get(0).getFunction().getExpression());
        Assert.assertEquals("COUNT", newModel.getAllMeasures().get(1).getFunction().getExpression());
        Assert.assertEquals(10, newModel.getAllNamedColumns().size());
        Assert.assertEquals(2, newModel.getEffectiveDimensions().size());
        val index_plan = context.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, index_plan.getAllIndexes().size());
        Assert.assertEquals(0, index_plan.getRuleBaseLayouts().size());

        // case 2. repropose new model and won't reuse the origin model,
        // check the models' info is equaled with origin model
        val context2 = AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", sqls);
        val reproposalModel = context2.getModelContexts().get(0).getTargetModel();
        Assert.assertNotEquals(newModel.getId(), reproposalModel.getId());
        Assert.assertTrue(reproposalModel.getJoinsGraph().match(newModel.getJoinsGraph(), Maps.newHashMap(), false));
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(reproposalModel.getAllMeasures(), newModel.getAllMeasures()));
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(reproposalModel.getAllNamedColumns(), newModel.getAllNamedColumns()));
        val reproposalIndexPlan = context2.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(index_plan.getAllLayouts(), reproposalIndexPlan.getAllLayouts()));

        // case 3. reuse the origin model
        val context3 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);
        val model3 = context3.getModelContexts().get(0).getTargetModel();
        Assert.assertTrue(model3.getId().equals(reproposalModel.getId()) || model3.getId().equals(newModel.getId()));

        // case 4. reuse origin model and generate recommendation
        String[] secondSqls = new String[] { "select max(test_account1.ACCOUNT_CONTACT) from "
                + "\"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 "
                + "on test_account.account_id = test_account1.account_id" };
        val context4 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", secondSqls);
        List<AbstractContext.ModelContext> modelContexts = context4.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        AbstractContext.ModelContext modelContext = modelContexts.get(0);
        val reusedModel = modelContext.getTargetModel();
        Assert.assertTrue(
                reusedModel.getId().equals(reproposalModel.getId()) || reusedModel.getId().equals(newModel.getId()));
        Assert.assertEquals(0, modelContext.getCcRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
        Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());

        // case 5. create a new model and without recommendation
        String[] thirdSqls = new String[] { "select test_kylin_fact.cal_dt, test_cal_dt.cal_dt, sum(price) \n"
                + "from test_kylin_fact inner join edw.test_cal_dt\n"
                + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "group by test_kylin_fact.cal_dt, test_cal_dt.cal_dt" };
        val context5 = AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", thirdSqls);
        List<AbstractContext.ModelContext> modelContexts5 = context5.getModelContexts();
        Assert.assertEquals(1, modelContexts5.size());
        AbstractContext.ModelContext modelContext5 = modelContexts5.get(0);
        Assert.assertTrue(modelContext5.getCcRecItemMap().isEmpty());
        Assert.assertTrue(modelContext5.getDimensionRecItemMap().isEmpty());
        Assert.assertTrue(modelContext5.getMeasureRecItemMap().isEmpty());
        Assert.assertTrue(modelContext5.getIndexRexItemMap().isEmpty());
        Assert.assertFalse(modelContext5.isTargetModelMissing());
        Assert.assertEquals(1, modelContext5.getTargetModel().getJoinTables().size());
    }
}
