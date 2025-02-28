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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GreedyModelTreesBuilderTest extends NLocalWithSparkSessionTest {

    @Ignore("Cannot propose in PureExpertMode")
    @Test
    public void testPartialJoinInExpertMode() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_account.account_country = test_country.country",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact" };

        // create model A join B join C
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] { sqls[0] });

        val originalIndexPlan = NIndexPlanManager.getInstance(getTestConfig(), "newten").listAllIndexPlans().get(0);
        Assert.assertEquals(1, originalIndexPlan.getAllIndexes().size());

        MetadataTestUtils.toPureExpertMode("newten");

        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        val indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "newten").listAllIndexPlans().get(0);
        val layouts = indexPlan.getAllLayouts();
        List<String> columns = Lists.newArrayList();

        Assert.assertEquals(1, context2.getModelContexts().size());
        Assert.assertEquals(3, layouts.size());
        layouts.forEach(layout -> {
            Assert.assertTrue(layout.getIndex().isTableIndex());
            Assert.assertEquals(1, layout.getColumns().size());
            columns.add(layout.getColumns().get(0).getIdentity());
        });
        context2.getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });
        Collections.sort(columns);
        Assert.assertArrayEquals(columns.toArray(), new String[] { "TEST_KYLIN_FACT.CAL_DT",
                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.TRANS_ID" });
    }

    /**
     * acceleration A , A <-> B , A <-> B <-> C when exist model A <-> B <-> C
     * <p>
     * expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeContainEachOther() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_account.account_country = test_country.country",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact" };

        // create model A join B join C
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] { sqls[0] });

        val originalIndexPlan = NIndexPlanManager.getInstance(getTestConfig(), "newten").listAllIndexPlans().get(0);
        Assert.assertEquals(1, originalIndexPlan.getAllIndexes().size());

        MetadataTestUtils.toSemiAutoMode("newten");

        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);
        List<AbstractContext.ModelContext> modelContexts = context2.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        AbstractContext.ModelContext modelContext = modelContexts.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMap = modelContext.getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMap.size());
        NDataModel model = modelContext.getTargetModel();
        List<String> columns = Lists.newArrayList();
        indexRexItemMap.forEach((unique, layoutRecItemV2) -> {
            Assert.assertFalse(layoutRecItemV2.isAgg());
            Assert.assertEquals(1, layoutRecItemV2.getLayout().getColOrder().size());
            int columnId = layoutRecItemV2.getLayout().getColOrder().get(0);
            columns.add(model.getColRef(columnId).getIdentity());
        });
        Collections.sort(columns);
        Assert.assertArrayEquals(columns.toArray(),
                new String[] { "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.TRANS_ID" });
        context2.getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });
    }

    /**
     * acceleration A <-> B , A <-> C, A <-> D when exist model A <-> B <-> C
     * <p>
     * expect A <-> B , A <-> C accelerate succeed, A <-> D accelerate failed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeContainFail() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select test_kylin_fact.CAL_DT from test_kylin_fact inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" };

        // create model A join B join C
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY" });

        // accelerate
        MetadataTestUtils.toSemiAutoMode("newten");
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        // validation results
        Assert.assertEquals(2, context2.getModelContexts().size());
        Assert.assertFalse(context2.getAccelerateInfoMap().get(sqls[0]).isNotSucceed());
        Assert.assertFalse(context2.getAccelerateInfoMap().get(sqls[1]).isNotSucceed());
        Assert.assertTrue(context2.getAccelerateInfoMap().get(sqls[2]).isNotSucceed());
    }

    /**
     * acceleration A <-> B , A <-> C, A <-> D when exist model A <-> B <-> C <-> D
     * <p>
     * expect A <-> B , A <-> C, A <-> D accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeNotContainEachOther() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        // create model A join B join C join D
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" });

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select test_kylin_fact.CAL_DT from test_kylin_fact inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" };

        // accelerate
        MetadataTestUtils.toSemiAutoMode("newten");
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        // validation results, all accelerate succeed.
        Assert.assertEquals(1, context2.getModelContexts().size());
        context2.getAccelerateInfoMap()
                .forEach((sql, accelerateInfo) -> Assert.assertFalse(accelerateInfo.isNotSucceed()));
    }

    /**
     * acceleration A <-> B , A <-> C, D when exist model A <-> B <-> C, D
     * <p>
     * expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeWithMultiModel() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select TEST_ORDER.ORDER_ID from TEST_ORDER" };

        // create model A join B join C, D
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select TEST_ORDER.ORDER_ID from TEST_ORDER" });

        // accelerate
        MetadataTestUtils.toSemiAutoMode("newten");
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        // validation results, all accelerate succeed.
        Assert.assertEquals(2, context2.getModelContexts().size());
        context2.getAccelerateInfoMap()
                .forEach((sql, accelerateInfo) -> Assert.assertFalse(accelerateInfo.isNotSucceed()));
    }

    /**
     * acceleration A <-> B , A -> D when exist model A <-> B <-> C -> D
     * <p>
     * expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeMixInnerJoinAndLeftJoin() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select test_kylin_fact.CAL_DT from test_kylin_fact left join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" };

        // create model A join B join C left join D
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY left join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" });

        // accelerate
        MetadataTestUtils.toSemiAutoMode("newten");
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        // validation results, all accelerate succeed.
        Assert.assertEquals(1, context2.getModelContexts().size());
        context2.getAccelerateInfoMap()
                .forEach((sql, accelerateInfo) -> Assert.assertFalse(accelerateInfo.isNotSucceed()));
    }

    /**
     * acceleration A <-> B , A <-> B' when exist model B' <-> A <-> B
     * <p>
     * expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeTableOfSameName() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account as account1 on test_kylin_fact.seller_id = account1.account_id",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account as account2 on test_kylin_fact.ORDER_ID = account2.ACCOUNT_SELLER_LEVEL" };

        // create model A join B join C
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account as account1 on test_kylin_fact.seller_id = account1.account_id inner join test_account as account2 on test_kylin_fact.ORDER_ID = account2.ACCOUNT_SELLER_LEVEL " });

        // accelerate
        MetadataTestUtils.toSemiAutoMode("newten");
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        // validation results, all accelerate succeed.
        Assert.assertEquals(1, context2.getModelContexts().size());
        context2.getAccelerateInfoMap()
                .forEach((sql, accelerateInfo) -> Assert.assertFalse(accelerateInfo.isNotSucceed()));
    }

    @Test
    public void testPartialJoinInSemiAutoModeWithCC() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select count(test_kylin_fact.trans_ID+1),count(test_kylin_fact.trans_id+1) from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select count(test_kylin_fact.TRANS_ID+1) from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY" };

        // create model A join B join C
        AccelerationUtil.runModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY" });

        // accelerate
        MetadataTestUtils.toSemiAutoMode("newten");
        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);

        // validation results
        Assert.assertEquals(1, context2.getModelContexts().size());
        context2.getAccelerateInfoMap()
                .forEach((sql, accelerateInfo) -> Assert.assertFalse(accelerateInfo.isNotSucceed()));
    }

    @Test
    public void testPartialJoinInSmartMode() {
        MetadataTestUtils.updateProjectConfig("newten", "kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_account.account_country = test_country.country",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact" };

        // create model A join B join C
        AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sqls[0] }, true);

        val originalModels = NDataModelManager.getInstance(getTestConfig(), "newten").listAllModels();
        Assert.assertEquals(1, originalModels.size());

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", sqls, true);

        Assert.assertEquals(3, context.getModelContexts().size());
        Assert.assertEquals(3, NDataModelManager.getInstance(getTestConfig(), "newten").listAllModels().size());
        context.getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });
    }

    @Test
    public void testBitmapMeasure() {
        String[] sqls = new String[] { "select bitmap_uuid(test_kylin_fact.trans_id) from test_kylin_fact",
                "select count(distinct test_kylin_fact.trans_id) from test_kylin_fact" };
        for (String sql : sqls) {
            List<NDataModel.Measure> recommendMeassures = getRecommendModel(sql).get(0).getAllMeasures();
            Assert.assertEquals(2, recommendMeassures.size());
            Assert.assertEquals(FunctionDesc.FUNC_COUNT_DISTINCT,
                    recommendMeassures.get(1).getFunction().getExpression());
        }
    }

    @Test
    public void testIntersectionMeasure() {
        String[] sqls = new String[] {
                "select INTERSECT_BITMAP_UUID(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A']) from test_kylin_fact",
                "select INTERSECT_COUNT(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A']) from test_kylin_fact",
                "select INTERSECT_BITMAP_UUID(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A']) from test_kylin_fact",
                "select INTERSECT_VALUE(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A']) from test_kylin_fact",
                "select INTERSECT_VALUE_V2(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A'],'RAWSTRING') from test_kylin_fact",
                "select INTERSECT_BITMAP_UUID_V2(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A'],'RAWSTRING') from test_kylin_fact",
                "select INTERSECT_COUNT_V2(test_kylin_fact.trans_id,LSTG_FORMAT_NAME,array['A'],'RAWSTRING') from test_kylin_fact" };
        for (String sql : sqls) {
            NDataModel recommendModel = getRecommendModel(sql).get(0);
            Assert.assertTrue(recommendModel.getDimensionNameIdMap().containsKey("TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
            List<NDataModel.Measure> recommendMeasures = recommendModel.getAllMeasures();
            Assert.assertEquals(2, recommendMeasures.size());
            Assert.assertEquals(FunctionDesc.FUNC_COUNT_DISTINCT,
                    recommendMeasures.get(1).getFunction().getExpression());
        }
    }

    private List<NDataModel> getRecommendModel(String sql) {
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sql }, true);
        return context.getProposedModels();
    }
}
