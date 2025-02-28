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

package org.apache.kylin.auto;

import java.util.List;
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelSelectProposer;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class ReuseModelCcTest extends SuggestTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        prepareModels();
        kylinConfig.setProperty("kylin.query.transformers",
                "org.apache.kylin.query.util.PowerBIConverter,org.apache.kylin.query.util.DefaultQueryTransformer,org.apache.kylin.query.util.EscapeTransformer,org.apache.kylin.query.util.KeywordDefaultDirtyHack,org.apache.kylin.query.security.RowFilter,org.apache.kylin.query.security.HackSelectStarWithColumnACL");
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, left join
     * To be accelerated sql statements:
     *   sql1. select count(item_count + price) from test_kylin_fact group by cal_dt;
     *   sql2. select count(item_count + price), count(item_count * price) from test_kylin_fact group by cal_dt;
     *   sql3. select * from test_kylin_fact;
     *   sql4. select * from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     *   sql5. select * from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     * <p>
     * expectation: all sql statements will success
     */
    @Test
    public void testReuseModelOfSmartMode() {
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "false");
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        List<NDataModel> dataModels = modelManager.listAllModels();
        Assert.assertEquals(2, dataModels.size());

        String[] statements = { "select count(item_count + price) from test_kylin_fact group by cal_dt", // sql1
                "select count(item_count + price), count(item_count * price) from test_kylin_fact group by cal_dt", // sql2
                "select * from test_kylin_fact", // sql3
                "select * from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt", // sql4
                "select * from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt" // sql5
        };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), statements, true);

        Assert.assertFalse(context.getAccelerateInfoMap().get(statements[0]).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(statements[1]).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(statements[2]).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(statements[3]).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(statements[4]).isNotSucceed());
        List<NDataModel> dataModelsAfterAutoModeling = modelManager.listAllModels();
        Assert.assertEquals(3, dataModelsAfterAutoModeling.size());
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, left join
     * To be accelerated sql statements:
     *   sql1. select count(item_count + price) from test_kylin_fact group by cal_dt;
     *   sql2. select count(item_count + price), count(item_count * price) from test_kylin_fact group by cal_dt;
     *   sql3. select * from test_kylin_fact;
     *   sql4. select * from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     *   sql5. select * from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     * <p>
     * expectation:
     *   first round: sql1 and sql2 will success, sql3, sql4 and sql5 will pending.
     *   second round: accelerate all pending sql statements, sql3 and sql4 will success, sql5 will pending
     *   third round: accelerate sql5, sql5 still pending for semi-auto mode cannot create new model
     */
    @Test
    public void testReuseModelOfSemiAutoMode() {
        transferProjectToSemiAutoMode();
        String[] statements = { "select count(item_count + price) from test_kylin_fact group by cal_dt", // sql1
                "select count(item_count + price), count(item_count * price) from test_kylin_fact group by cal_dt", // sql2
                "select * from test_kylin_fact", // sql3
                "select * from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt", // sql4
                "select * from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt" // sql5
        };

        // first round
        val context = AccelerationUtil.runModelReuseContext(kylinConfig, getProject(), statements);

        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(statements[0]).isNotSucceed());
        Assert.assertFalse(accelerateInfoMap.get(statements[1]).isNotSucceed());
        Assert.assertTrue(accelerateInfoMap.get(statements[2]).isPending());
        Assert.assertEquals(ModelSelectProposer.CC_ACROSS_MODELS_PENDING_MSG,
                accelerateInfoMap.get(statements[2]).getPendingMsg());
        Assert.assertTrue(accelerateInfoMap.get(statements[3]).isPending());
        Assert.assertEquals(ModelSelectProposer.CC_ACROSS_MODELS_PENDING_MSG,
                accelerateInfoMap.get(statements[3]).getPendingMsg());
        Assert.assertTrue(accelerateInfoMap.get(statements[4]).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG,
                accelerateInfoMap.get(statements[4]).getPendingMsg());

        // mock apply recommendations of ccs, dimensions and measures
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelContexts.forEach(modelContext -> {
            NDataModel targetModel = modelContext.getTargetModel();
            if (targetModel != null) {
                dataModelManager.updateDataModel(targetModel.getUuid(), copyForWrite -> {
                    copyForWrite.setComputedColumnDescs(targetModel.getComputedColumnDescs());
                    copyForWrite.setAllMeasures(targetModel.getAllMeasures());
                    copyForWrite.setAllNamedColumns(targetModel.getAllNamedColumns());
                });
            }
        });

        // second round
        val context2 = AccelerationUtil.runModelReuseContext(kylinConfig, getProject(),
                new String[] { statements[2], statements[3], statements[4] });

        accelerateInfoMap = context2.getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(statements[2]).isNotSucceed());
        Assert.assertFalse(accelerateInfoMap.get(statements[3]).isNotSucceed());
        Assert.assertTrue(accelerateInfoMap.get(statements[4]).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG,
                accelerateInfoMap.get(statements[4]).getPendingMsg());

        // third round
        val context3 = AccelerationUtil.runModelReuseContext(kylinConfig, getProject(), new String[] { statements[4] });

        accelerateInfoMap = context3.getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(statements[4]).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG,
                accelerateInfoMap.get(statements[4]).getPendingMsg());
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, left join
     * To be accelerated sql statements:
     *   sql. select count(item_count + price), count(item_count * price)
     *        from test_kylin_fact join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     * <p>
     * expectation: pending
     */
    @Test
    public void testCannotReuseModelOfSemiAutoMode() {
        transferProjectToSemiAutoMode();
        String sql = "select count(item_count + price), count(item_count * price) "
                + "from test_kylin_fact join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt";
        val context = AccelerationUtil.runModelReuseContext(kylinConfig, getProject(), new String[] { sql });

        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sql).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfoMap.get(sql).getPendingMsg());
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, inner join
     * To be accelerated sql statements: select * from test_kylin_fact;
     * <p>
     * expectation: pending
     */
    @Test
    public void testSelectStarReuseLeftJoinOfSemiAutoMode() {
        transferProjectToSemiAutoMode();
        NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, getProject());
        List<String> modelId = Lists.newArrayList();
        modelManager.listAllModels().forEach(dataModel -> {
            if (dataModel.getAlias().equals("AUTO_MODEL_TEST_KYLIN_FACT_2")
                    && dataModel.getRootFactTable().getAlias().equals("TEST_KYLIN_FACT")) {
                modelId.add(dataModel.getUuid());
            }
        });
        Assert.assertEquals(1, modelId.size());
        modelManager.updateDataModel(modelId.get(0), cp -> cp.getJoinTables().get(0).getJoin().setType("inner"));

        String sql = "select * from test_kylin_fact";
        val context = AccelerationUtil.runModelReuseContext(kylinConfig, getProject(), new String[] { sql });

        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfoMap.get(sql).getPendingMsg());
    }

    /**
     * model1: Model3 contains a computed column {lineitem.l_extendedprice * lineitem.l_discount}, inner join
     * model2: Model4 contains a computed column {lineitem.l_quantity * lineitem.l_extendedprice},
     *         no join, share same fact table
     * To be accelerated sql: select * from tpch.lineitem
     * <p>
     * expectation: success
     */
    @Test
    public void testSelectStarCheckInnerJoinOfSemiAutoMode() {
        prepareMoreModels();
        transferProjectToSemiAutoMode();
        String sql = "select * from TPCH.LINEITEM";
        val context = AccelerationUtil.runModelReuseContext(kylinConfig, getProject(), new String[] { sql });

        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfoMap.get(sql).getPendingMsg());
    }

    private void transferProjectToSemiAutoMode() {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(getProject(), copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.query.metadata.expose-computed-column", "true");
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    private void prepareModels() {
        String[] sqls = {
                "select count(item_count + price) from test_kylin_fact left join edw.test_cal_dt "
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt",
                "select sum(item_count * price) from test_kylin_fact left join test_category_groupings "
                        + "on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                        + "and test_kylin_fact.lstg_site_id = test_category_groupings.site_id" //
        };
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { sqls[0] }, true);
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { sqls[1] }, true);
    }

    private void prepareMoreModels() {
        String[] sqls = { //
                "select count(l_extendedprice * l_discount) from tpch.lineitem "
                        + "inner join tpch.part on lineitem.l_partkey = part.p_partkey",
                "select sum(l_quantity * l_extendedprice) from tpch.lineitem" //
        };
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { sqls[0] }, true);
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { sqls[1] }, true);
    }
}
