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

package org.apache.kylin.rec.model;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.SmartContext;
import org.apache.kylin.rec.SmartMaster;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.index.IndexSuggester;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class ModelMasterTest extends AutoTestOnLearnKylinData {

    @Test
    public void testNormal() {
        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales left join kylin_cal_dt on cal_dt = part_dt group by part_dt" //
        };

        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx);

        ModelMaster modelMaster = new ModelMaster(mdCtx);

        // propose initial cube
        NDataModel dataModel = modelMaster.proposeInitialModel();
        {
            Assert.assertNotNull(dataModel);
            List<NDataModel.Measure> allMeasures = dataModel.getAllMeasures();
            Assert.assertTrue(dataModel.getAllNamedColumns().isEmpty());
            Assert.assertEquals(1, allMeasures.size());
            Assert.assertEquals("COUNT_ALL", allMeasures.get(0).getName());
        }

        dataModel = modelMaster.proposeComputedColumn(dataModel);
        dataModel = modelMaster.proposeJoins(dataModel);
        {
            val joins = dataModel.getJoinTables();
            Assert.assertNotNull(joins);
            Assert.assertEquals(1, joins.size());
            Assert.assertEquals(NDataModel.TableKind.FACT, joins.get(0).getKind());
            Assert.assertEquals("LEFT", joins.get(0).getJoin().getType());
            Assert.assertEquals("DEFAULT.KYLIN_CAL_DT", joins.get(0).getTable());
            Assert.assertEquals("KYLIN_CAL_DT", joins.get(0).getAlias());
            Assert.assertArrayEquals(new String[] { "KYLIN_CAL_DT.CAL_DT" }, joins.get(0).getJoin().getPrimaryKey());
            Assert.assertArrayEquals(new String[] { "KYLIN_SALES.PART_DT" }, joins.get(0).getJoin().getForeignKey());
        }

        // propose again, should return same result
        NDataModel dm1 = modelMaster.proposeJoins(dataModel);
        Assert.assertEquals(dm1, dataModel);

        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            List<NDataModel.Measure> allMeasures = dataModel.getAllMeasures();
            Assert.assertFalse(dataModel.getAllNamedColumns().isEmpty());
            Assert.assertEquals(3, allMeasures.size());
            Assert.assertEquals("COUNT_ALL", allMeasures.get(0).getName());
            Assert.assertEquals("SUM_KYLIN_SALES_PRICE", allMeasures.get(1).getName());
        }

        // propose again, should return same result
        NDataModel dm2 = modelMaster.proposeScope(dataModel);
        Assert.assertEquals(dm2, dataModel);
    }

    @Test
    public void testSqlWithoutPartition() {

        String[] sqls = new String[] {
                "SELECT kylin_category_groupings.meta_categ_name, kylin_category_groupings.categ_lvl2_name, "
                        + " sum(kylin_sales.price) as GMV, count(*) as trans_cnt"
                        + " FROM kylin_sales inner JOIN kylin_category_groupings"
                        + " ON kylin_sales.leaf_categ_id = kylin_category_groupings.leaf_categ_id"
                        + " AND kylin_sales.lstg_site_id = kylin_category_groupings.site_id"
                        + " group by kylin_category_groupings.meta_categ_name ,kylin_category_groupings.categ_lvl2_name" //
        };
        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx);

        ModelMaster modelMaster = new ModelMaster(mdCtx);

        NDataModel dataModel = modelMaster.proposeInitialModel();
        dataModel = modelMaster.proposeComputedColumn(dataModel);
        dataModel = modelMaster.proposeJoins(dataModel);
        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(48, dataModel.getAllNamedColumns().size());
            Assert.assertEquals(43, dataModel.getColumnIdByColumnName("KYLIN_SALES.PART_DT"));
            Assert.assertEquals(2, dataModel.getAllMeasures().size());
            Assert.assertEquals(1, dataModel.getJoinTables().size());
        }
    }

    @Test
    public void testDimensionAsMeasure() {
        // we expect sqls can be accelerated will not blocked by its counterpart
        String[] sqls = new String[] { //
                " SELECT SUM((CASE WHEN 1.1000000000000001 = 0 THEN CAST(NULL AS DOUBLE) "
                        + "ELSE \"TEST_KYLIN_FACT\".\"PRICE\" / 1.1000000000000001 END)) "
                        + "AS \"sum_price\" FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"",

                " SELECT SUM(\"TEST_KYLIN_FACT\".\"PRICE\" * 2 + 2) "
                        + "AS \"double_price\" FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"",

                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"",

        };
        val context = new SmartContext(getTestConfig(), "newten", sqls);
        context.setSkipEvaluateCC(true);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(AccelerationUtil.onlineHook);

        final AbstractContext smartContext = smartMaster.getContext();
        final List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());

        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        AccelerateInfo info0 = accelerateInfoMap.get(sqls[0]);
        Assert.assertTrue(info0.isPending());
        Assert.assertTrue(info0.getPendingMsg().startsWith(IndexSuggester.OTHER_UNSUPPORTED_MEASURE));
        AccelerateInfo info1 = accelerateInfoMap.get(sqls[1]);
        Assert.assertTrue(info1.isPending());
        Assert.assertTrue(info1.getPendingMsg().startsWith(IndexSuggester.OTHER_UNSUPPORTED_MEASURE));
        AccelerateInfo info2 = accelerateInfoMap.get(sqls[2]);
        Assert.assertFalse(info2.isFailed());
        Assert.assertEquals(1, info2.getRelatedLayouts().size());
    }

    @Test
    public void testProposePartition() {
        // we expect sqls can be accelerated will not blocked by its counterpart
        String[] sqls = new String[] { //
                " SELECT SUM((CASE WHEN 1.1000000000000001 = 0 THEN CAST(NULL AS DOUBLE) "
                        + "ELSE \"TEST_KYLIN_FACT\".\"PRICE\" / 1.1000000000000001 END)) "
                        + "AS \"sum_price\" FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"",

                " SELECT SUM(\"TEST_KYLIN_FACT\".\"PRICE\" * 2 + 2) "
                        + "AS \"double_price\" FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"",

                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"",

        };
        val context = new SmartContext(getTestConfig(), "newten", sqls);
        context.setSkipEvaluateCC(true);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        final AbstractContext smartContext = smartMaster.getContext();
        final List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        Assert.assertNull(modelContexts.get(0).getTargetModel().getPartitionDesc());
    }

    @Test
    public void testSqlWithEmptyAllColsInContext() {
        String[] sqls = new String[] { "SELECT count(*) as cnt from " + "KYLIN_SALES as KYLIN_SALES  \n"
                + "INNER JOIN KYLIN_CAL_DT as KYLIN_CAL_DT ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT \n"
                + "INNER JOIN KYLIN_CATEGORY_GROUPINGS as KYLIN_CATEGORY_GROUPINGS \n"
                + "    ON KYLIN_SALES.LEAF_CATEG_ID = KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND KYLIN_SALES.LSTG_SITE_ID = KYLIN_CATEGORY_GROUPINGS.SITE_ID \n"
                + "INNER JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID \n"
                + "INNER JOIN KYLIN_COUNTRY as BUYER_COUNTRY ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY \n" };
        val context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();

        AbstractContext ctx = smartMaster.getContext();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(0, ctx.getModelContexts().get(0).getModelTree().getOlapContexts().iterator().next()
                .getAllColumns().size());
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx);

        ModelMaster modelMaster = new ModelMaster(mdCtx);
        // propose model
        NDataModel dataModel = modelMaster.proposeInitialModel();
        dataModel = modelMaster.proposeComputedColumn(dataModel);
        dataModel = modelMaster.proposeJoins(dataModel);
        dataModel = modelMaster.proposeScope(dataModel);
        {
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(157, dataModel.getAllNamedColumns().size());
        }
    }

    @Test
    public void testEnableDisableNonEquiJoinModeling() {
        String[] sqls = new String[] {
                "select avg(PRICE), ACCOUNT_ID\n" + "from KYLIN_SALES \n" + "left join KYLIN_ACCOUNT\n"
                        + "ON KYLIN_SALES.BUYER_ID > KYLIN_ACCOUNT.ACCOUNT_ID\n" + "group by price, account_id" };
        KylinConfig conf = getTestConfig();
        conf.setProperty("kylin.query.non-equi-join-model-enabled", "TRUE");
        conf.setProperty("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        {
            val context = new SmartContext(conf, proj, sqls);
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.getProposer("SQLAnalysisProposer").execute();

            AbstractContext ctx = smartMaster.getContext();
            Assert.assertEquals(2, ctx.getModelContexts().size());
        }

        conf.setProperty("kylin.query.non-equi-join-model-enabled", "FALSE");
        conf.setProperty("kylin.model.non-equi-join-recommendation-enabled", "FALSE");
        {
            val context = new SmartContext(conf, proj, sqls);
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.getProposer("SQLAnalysisProposer").execute();

            AbstractContext ctx = smartMaster.getContext();
            Assert.assertEquals(2, ctx.getModelContexts().size());
        }
    }

    @Test
    public void testProposerJoinTypeCapital() {
        String[] sqls = new String[] { "select part_dt from kylin_sales inner join kylin_cal_dt on cal_dt = part_dt "
                + "left JOIN KYLIN_ACCOUNT as BUYER_ACCOUNT ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID " };
        AbstractContext context = new SmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        ModelMaster modelMaster = new ModelMaster(smartMaster.getContext().getModelContexts().get(0));
        NDataModel dataModel = modelMaster.proposeInitialModel();

        dataModel = modelMaster.proposeJoins(dataModel);
        Assert.assertEquals("LEFT", dataModel.getJoinTables().get(0).getJoin().getType());
        Assert.assertEquals("INNER", dataModel.getJoinTables().get(1).getJoin().getType());
    }
}
