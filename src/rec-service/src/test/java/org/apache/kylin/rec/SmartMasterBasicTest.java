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
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.model.ModelTree;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class SmartMasterBasicTest extends AutoTestOnLearnKylinData {

    private NTableMetadataManager tableMetadataManager;
    private NIndexPlanManager indexPlanManager;
    private NDataflowManager dataflowManager;

    @Before
    public void setupManagers() throws Exception {
        setUp();
        KylinConfig kylinConfig = getTestConfig();
        tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, proj);
        dataflowManager = NDataflowManager.getInstance(kylinConfig, proj);
    }

    @Test
    public void test() {

        // 1st round - input SQLs, create model and cube_plan
        test1stRound();

        // 2nd round - input SQLs, update model and cube_plan
        test2ndRound();

        // 3rd round - input complex SQLs, update model and cube_plan
        test3rdRound();

        // 4th round - unload all queries
        test4thRound();
    }

    private void test1stRound() {
        final int expectedEffectiveOlapCtxNum = 4;
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("DEFAULT.KYLIN_SALES");

        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt" //
        };

        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);

        // validation after initializing NSmartMaster
        AbstractContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        Assert.assertEquals(1, smartContext.getModelContexts().size());
        AbstractContext.ModelContext mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getModelTree());
        ModelTree modelTree = mdCtx.getModelTree();
        Assert.assertEquals(expectedEffectiveOlapCtxNum, modelTree.getOlapContexts().size());
        Assert.assertEquals(kylinSalesTblDesc, modelTree.getRootFactTable());

        // select model
        smartMaster.getProposer("ModelSelectProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNull(mdCtx.getTargetModel());
        Assert.assertNull(mdCtx.getOriginModel());

        // optimize model
        smartMaster.getProposer("ModelOptProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        NDataModel model = mdCtx.getTargetModel();
        Assert.assertNotNull(model);
        Assert.assertEquals(kylinSalesTblDesc, model.getRootFactTable().getTableDesc());
        Assert.assertFalse(model.getEffectiveCols().isEmpty());
        Assert.assertFalse(model.getEffectiveMeasures().isEmpty());

        // select IndexPlan
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNull(mdCtx.getOriginIndexPlan());
        Assert.assertNull(mdCtx.getTargetIndexPlan());

        // optimize IndexPlan
        smartMaster.getProposer("IndexPlanOptProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        Assert.assertNotNull(indexPlan);
        Assert.assertEquals(mdCtx.getTargetModel().getUuid(), indexPlan.getUuid());
        List<IndexEntity> indexEntities = indexPlan.getAllIndexes();
        Assert.assertEquals(2, indexEntities.size());
        Assert.assertEquals(3, collectAllLayouts(indexEntities).size());

        // validation before save
        Assert.assertEquals(0, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(0, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(0, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(6, accelerateInfoMap.size());
        Assert.assertEquals(4, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        context.saveMetadata();
        AccelerationUtil.onlineModel(context);
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
    }

    private void test2ndRound() {
        final int expectedEffectiveOlapCtxNum = 4;
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("DEFAULT.KYLIN_SALES");

        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name",
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'",
                "select part_dt, lstg_format_name, sum(price * item_count + 2) "
                        + "from kylin_sales where part_dt > '2012-01-01' "
                        + "union select part_dt, lstg_format_name, price "
                        + "from kylin_sales where part_dt < '2012-01-01'",
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" //
        };

        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);

        // validation after initializing NSmartMaster
        AbstractContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        Assert.assertEquals(1, smartContext.getModelContexts().size());
        AbstractContext.ModelContext mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getModelTree());
        ModelTree modelTree = mdCtx.getModelTree();
        Assert.assertEquals(expectedEffectiveOlapCtxNum, modelTree.getOlapContexts().size());
        Assert.assertEquals(kylinSalesTblDesc, modelTree.getRootFactTable());

        // select model
        smartMaster.getProposer("ModelSelectProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getTargetModel());
        Assert.assertNotNull(mdCtx.getOriginModel());

        // optimize model
        smartMaster.getProposer("ModelOptProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        NDataModel model = mdCtx.getTargetModel();
        Assert.assertEquals(kylinSalesTblDesc, model.getRootFactTable().getTableDesc());
        Assert.assertEquals(model.getUuid(), mdCtx.getOriginModel().getUuid());
        Assert.assertFalse(model.getEffectiveCols().isEmpty());
        Assert.assertFalse(model.getEffectiveMeasures().isEmpty());

        // select IndexPlan
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getTargetIndexPlan());
        Assert.assertNotNull(mdCtx.getOriginIndexPlan());

        // optimize IndexPlan
        smartMaster.getProposer("IndexPlanOptProposer").execute();
        mdCtx = smartContext.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        Assert.assertNotNull(indexPlan);
        Assert.assertEquals(indexPlan.getUuid(), mdCtx.getOriginIndexPlan().getUuid());
        Assert.assertEquals(mdCtx.getTargetModel().getUuid(), indexPlan.getUuid());
        List<IndexEntity> indexEntities = indexPlan.getAllIndexes();
        Assert.assertEquals(4, indexEntities.size());
        Assert.assertEquals(5, collectAllLayouts(indexEntities).size());

        // validation before save
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(5, accelerateInfoMap.size());
        Assert.assertEquals(4, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        context.saveMetadata();
        // validation after save

        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
    }

    private void test3rdRound() {

        String[] sqls = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums " //
                + "FROM ( " //
                + "  SELECT f.leaf_categ_id FROM kylin_sales f\n" //
                + "    INNER JOIN KYLIN_CATEGORY_GROUPINGS o\n" //
                + "    ON f.leaf_categ_id = o.leaf_categ_id AND f.LSTG_SITE_ID = o.site_id\n" //
                + "  WHERE f.lstg_format_name = 'ABIN'\n" //
                + ") t1 INNER JOIN (\n" //
                + "    SELECT leaf_categ_id FROM kylin_sales f\n" //
                + "      INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id\n" //
                + "    WHERE buyer_id > 100\n" //
                + ") t2 ON t1.leaf_categ_id = t2.leaf_categ_id\n" //
                + "GROUP BY t1.leaf_categ_id\n" //
                + "ORDER BY nums, leaf_categ_id" };

        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);

        // validation after initializing NSmartMaster
        AbstractContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        Assert.assertEquals(2, smartContext.getModelContexts().size());

        // select Model
        smartMaster.getProposer("ModelSelectProposer").execute();
        for (AbstractContext.ModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNull(modelContext.getOriginModel());
            Assert.assertNull(modelContext.getTargetModel());
        }

        // optimize Model
        smartMaster.getProposer("ModelOptProposer").execute();
        NDataModel model0 = smartContext.getModelContexts().get(0).getTargetModel();
        Assert.assertNotNull(model0);
        Assert.assertEquals(48, model0.getEffectiveCols().size());
        Assert.assertEquals(2, model0.getEffectiveDimensions().size());
        Assert.assertEquals(1, model0.getEffectiveMeasures().size());
        NDataModel model1 = smartContext.getModelContexts().get(1).getTargetModel();
        Assert.assertNotNull(model1);
        Assert.assertEquals(17, model1.getEffectiveCols().size());
        Assert.assertEquals(2, model1.getEffectiveDimensions().size());
        Assert.assertEquals(1, model1.getEffectiveMeasures().size());

        // select IndexPlan
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        for (AbstractContext.ModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNull(modelContext.getOriginIndexPlan());
            Assert.assertNull(modelContext.getTargetIndexPlan());
        }

        // optimize IndexPlan
        smartMaster.getProposer("IndexPlanOptProposer").execute();
        IndexPlan indexPlan0 = smartContext.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertNotNull(indexPlan0);
        Assert.assertEquals(1, indexPlan0.getAllIndexes().size());
        IndexPlan indexPlan1 = smartContext.getModelContexts().get(1).getTargetIndexPlan();
        Assert.assertNotNull(indexPlan1);
        Assert.assertEquals(1, indexPlan1.getAllIndexes().size());

        // validation before save
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.size());
        Assert.assertEquals(1, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        context.saveMetadata();
        AccelerationUtil.onlineModel(context);
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(3, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
    }

    private void test4thRound() {
        String[] sqls = new String[] {
                // 1st round
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt", //

                // 2nd round
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name", //
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'", //
                "select part_dt, lstg_format_name, sum(price * item_count + 2) "
                        + "from kylin_sales where part_dt > '2012-01-01' "
                        + "union select part_dt, lstg_format_name, price "
                        + "from kylin_sales where part_dt < '2012-01-01'",
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name", //

                // 3rd round
                "select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT "
                        + " from kylin_sales as test_kylin_fact where test_kylin_fact.lstg_format_name <= 'ABZ' "
                        + " group by test_kylin_fact.lstg_format_name having count(seller_id) > 2", //
                "SELECT t1.leaf_categ_id, COUNT(*) AS nums " //
                        + "FROM ( " //
                        + "  SELECT f.leaf_categ_id FROM kylin_sales f\n" //
                        + "    INNER JOIN KYLIN_CATEGORY_GROUPINGS o\n" //
                        + "    ON f.leaf_categ_id = o.leaf_categ_id AND f.LSTG_SITE_ID = o.site_id\n" //
                        + "  WHERE f.lstg_format_name = 'ABIN'\n" //
                        + ") t1 INNER JOIN (\n" //
                        + "    SELECT leaf_categ_id FROM kylin_sales f\n" //
                        + "      INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id\n" //
                        + "    WHERE buyer_id > 100\n" //
                        + ") t2 ON t1.leaf_categ_id = t2.leaf_categ_id\n" //
                        + "GROUP BY t1.leaf_categ_id\n" //
                        + "ORDER BY nums, leaf_categ_id" //
        };

        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);

        // validation after initializing NSmartMaster
        AbstractContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        Assert.assertEquals(3, smartContext.getModelContexts().size());
        Assert.assertEquals(11, collectAllOlapContexts(smartContext).size());

        // select model
        smartMaster.getProposer("ModelSelectProposer").execute();
        for (AbstractContext.ModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNotNull(modelContext.getOriginModel());
            Assert.assertNotNull(modelContext.getTargetModel());
        }

        // optimize model
        smartMaster.getProposer("ModelOptProposer").execute();

        // select IndexPlan
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        for (AbstractContext.ModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNotNull(modelContext.getOriginIndexPlan());
        }

        // optimize IndexPlan
        smartMaster.getProposer("IndexPlanOptProposer").execute();
        List<IndexEntity> allProposedIndexes = Lists.newArrayList();
        smartContext.getModelContexts().forEach(modelContext -> {
            allProposedIndexes.addAll(modelContext.getTargetIndexPlan().getAllIndexes());
        });
        Assert.assertEquals(7, allProposedIndexes.size());
        Assert.assertEquals(8, collectAllLayouts(allProposedIndexes).size());

        // validation before save
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(3, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(13, accelerateInfoMap.size());
        Assert.assertEquals(10, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        context.saveMetadata();
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(3, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
    }
}
