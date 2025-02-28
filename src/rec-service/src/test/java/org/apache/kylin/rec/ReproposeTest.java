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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.Constant;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class ReproposeTest extends AutoTestOnLearnKylinData {

    @Test
    public void testProposeOnReusableModel() {

        String sql = "select item_count, lstg_format_name, sum(price)\n" //
                + "from kylin_sales\n" //
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10;";
        // init a reusable model and build indexes
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql }, true);
        val modelContext = context.getModelContexts().get(0);
        val allLayouts = modelContext.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(1, allLayouts.size());
        val initialLayout = allLayouts.get(0);
        Assert.assertEquals("[1, 3, 100000, 100001]", initialLayout.getColOrder().toString());

        // 1. case1: the layout is the best, will not propose another layout
        String sql1 = "select item_count, lstg_format_name, sum(price)\n" //
                + "from kylin_sales where item_count > 0\n" //
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10;";
        val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql1 }, true);
        val modelContext1 = context1.getModelContexts().get(0);
        val allLayouts1 = modelContext1.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(1, allLayouts1.size());
        val layout1 = allLayouts1.get(0);
        Assert.assertEquals("[1, 3, 100000, 100001]", layout1.getColOrder().toString());

        // 2. case2: the layout used is not the best, propose another layout
        String sql2 = "select item_count, sum(price)\n" //
                + "from kylin_sales where item_count > 0\n" //
                + "group by item_count\n" //
                + "order by item_count\n" //
                + "limit 10;";
        val context2 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql2 }, true);
        val modelContext2 = context2.getModelContexts().get(0);
        val allLayouts2 = modelContext2.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(2, allLayouts2.size());
        val layout20 = allLayouts2.get(0);
        val layout21 = allLayouts2.get(1);
        Assert.assertEquals("[1, 3, 100000, 100001]", layout20.getColOrder().toString());
        Assert.assertEquals("[1, 100000, 100001]", layout21.getColOrder().toString());
    }

    @Test
    public void testReproposeChangedByTableStats() {
        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), proj);

        // 1. initial propose
        String sql = "select seller_id, lstg_format_name, count(1), sum(price)\n" //
                + "from kylin_sales\n" //
                + "where seller_id = 10000002 or lstg_format_name = 'FP-non GTC'\n" //
                + "group by seller_id, lstg_format_name\n" //
                + "order by seller_id, lstg_format_name\n" //
                + "limit 20";
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql }, true);
        val modelContext = context.getModelContexts().get(0);
        val allLayouts = modelContext.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(1, allLayouts.size());
        val initialLayout = allLayouts.get(0);
        Assert.assertEquals("[9, 3, 100000, 100001]", initialLayout.getColOrder().toString());

        // 2. mock a sampling result
        String tableIdentity = "DEFAULT.KYLIN_SALES";
        val tableDesc = tableMgr.getTableDesc(tableIdentity);
        final TableExtDesc oldExtDesc = tableMgr.getOrCreateTableExt(tableDesc);
        TableExtDesc tableExt = new TableExtDesc(oldExtDesc);
        tableExt.setIdentity(tableIdentity);
        val col1 = tableExt.getColumnStatsByName("LSTG_FORMAT_NAME");
        col1.setCardinality(10000);
        col1.setTableExtDesc(tableExt);
        val col2 = tableExt.getColumnStatsByName("SELLER_ID");
        col2.setCardinality(100);
        col2.setTableExtDesc(tableExt);
        tableMgr.mergeAndUpdateTableExt(oldExtDesc, tableExt);

        // 3. re-propose with table stats
        val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql }, true);
        val modelContexts1 = context1.getModelContexts();
        Assert.assertEquals(1, modelContexts1.size());
        val modelContext1 = modelContexts1.get(0);
        val layouts = modelContext1.getTargetIndexPlan().getAllLayouts();
        Assert.assertEquals(2, layouts.size());
        val layout10 = layouts.get(0);
        val layout11 = layouts.get(1);
        Assert.assertEquals("[9, 3, 100000, 100001]", layout10.getColOrder().toString());
        Assert.assertEquals("[3, 9, 100000, 100001]", layout11.getColOrder().toString());
    }

    @Test
    public void testProposedLayoutConsistency() {
        List<String> sqlList = Lists.newArrayList("select test_kylin_fact.order_id, lstg_format_name\n"
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id\n"
                + "order by test_kylin_fact.order_id, lstg_format_name\n",
                "select lstg_format_name, seller_id, sum(price)\n"
                        + "from test_kylin_fact left join test_account on test_kylin_fact.seller_id = test_account.account_id\n"
                        + "group by lstg_format_name, seller_id\n" //
                        + "order by lstg_format_name, seller_id\n",
                "select lstg_format_name, sum(price)\n"
                        + "from test_kylin_fact left join test_account on test_kylin_fact.seller_id = test_account.account_id\n"
                        + "inner join test_country on test_account.account_country = test_country.country\n"
                        + "group by lstg_format_name\n" //
                        + "order by lstg_format_name\n",
                "select lstg_format_name, test_kylin_fact.leaf_categ_id, sum(price)\n"
                        + "from test_kylin_fact inner join test_category_groupings on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n"
                        + "group by lstg_format_name, test_kylin_fact.leaf_categ_id\n"
                        + "order by lstg_format_name, test_kylin_fact.leaf_categ_id\n",
                "select account_country\n"
                        + "from test_account inner join test_country on test_account.account_country = test_country.country\n"
                        + "order by account_country\n" //
        );
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                sqlList.toArray(new String[0]), true);
        final Map<String, AccelerateInfo> secondRoundMap = context.getAccelerateInfoMap();
        final Set<String> secondRoundProposedColOrders = collectColOrders(secondRoundMap.values());

        // Suggested layouts should be independent of the order of input sqls
        {
            Collections.shuffle(sqlList); // shuffle and propose
            val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                    sqlList.toArray(new String[0]), true);
            final Map<String, AccelerateInfo> thirdRoundMap = context1.getAccelerateInfoMap();
            final Set<String> thirdRoundProposedColOrders = collectColOrders(thirdRoundMap.values());
            Assert.assertEquals(secondRoundProposedColOrders, thirdRoundProposedColOrders);
        }

        // Suggested layouts should be independent of modeling by a single batch or multi-batches
        {
            List<String> batchOne = sqlList.subList(0, sqlList.size() / 2);
            val context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                    batchOne.toArray(new String[0]), true);
            final Map<String, AccelerateInfo> batchOneMap = context1.getAccelerateInfoMap();

            List<String> batchTwo = sqlList.subList(sqlList.size() / 2, sqlList.size());
            val context2 = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                    batchTwo.toArray(new String[0]), true);
            final Map<String, AccelerateInfo> batchTwoMap = context2.getAccelerateInfoMap();

            Set<String> batchProposedColOrders = Sets.newHashSet();
            batchProposedColOrders.addAll(collectColOrders(batchOneMap.values()));
            batchProposedColOrders.addAll(collectColOrders(batchTwoMap.values()));
            Assert.assertEquals(secondRoundProposedColOrders, batchProposedColOrders);
        }
    }

    private Set<String> collectColOrders(Collection<AccelerateInfo> accelerateInfoList) {
        Set<String> allProposedColOrder = Sets.newHashSet();
        accelerateInfoList.forEach(accelerateInfo -> {
            final Set<AccelerateInfo.QueryLayoutRelation> layouts = accelerateInfo.getRelatedLayouts();
            Set<String> colOrders = collectAllColOrders(getTestConfig(), getProject(), layouts);
            allProposedColOrder.addAll(colOrders);
        });
        return allProposedColOrder;
    }

    private static Set<String> collectAllColOrders(KylinConfig kylinConfig, String project,
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts) {
        Set<String> sets = Sets.newHashSet();
        if (CollectionUtils.isEmpty(relatedLayouts)) {
            return sets;
        }

        relatedLayouts.forEach(layoutRelation -> {
            final List<String> colOrderNames = findColOrderNames(kylinConfig, project, layoutRelation);
            sets.add(String.join(",", colOrderNames));
        });

        return sets;
    }

    private static List<String> findColOrderNames(KylinConfig kylinConfig, String project,
            AccelerateInfo.QueryLayoutRelation queryLayoutRelation) {
        List<String> colOrderNames = Lists.newArrayList();

        final IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project)
                .getIndexPlan(queryLayoutRelation.getModelId());
        val layout = indexPlan.getLayoutEntity(queryLayoutRelation.getLayoutId());
        ImmutableList<Integer> colOrder = layout.getColOrder();
        BiMap<Integer, TblColRef> effectiveDimCols = layout.getIndex().getEffectiveDimCols();
        ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures = layout.getIndex().getEffectiveMeasures();
        colOrder.forEach(column -> {
            if (column < NDataModel.MEASURE_ID_BASE) {
                colOrderNames.add(effectiveDimCols.get(column).getName());
            } else {
                colOrderNames.add(effectiveMeasures.get(column).getName());
            }
        });
        return colOrderNames;
    }

    @Test
    public void testModelRenameProposerWithVeryLongFactTableName() {
        String sql = "select item_count, lstg_format_name, sum(price)\n" //
                + "from KYLIN_SALES_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG\n" //
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10;";
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql }, true);
        val modelContext = context.getModelContexts().get(0);
        val autoModelAlias = modelContext.getTargetModel().getAlias();

        Assert.assertEquals(Constant.MODEL_ALIAS_LEN_LIMIT, autoModelAlias.length());
        Assert.assertEquals(
                "AUTO_MODEL_KYLIN_SALES_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_L_TRUNC_1",
                autoModelAlias);
    }

    @Test
    public void testProposeWithConstantQuery() {
        // 1. init a base model
        String sql = "select sum(price) from kylin_sales";
        AccelerationUtil.runWithSmartContext(getTestConfig(), proj, new String[] { sql }, true);

        // 2. propose with constant query
        String sql1 = "select split('1,2,3','[,]',2) from KYLIN_SALES";
        val context1 = new SmartContext(getTestConfig(), proj, new String[] { sql1 });
        SmartMaster smartMaster1 = new SmartMaster(context1);
        smartMaster1.executePropose();
        val modelContext1 = smartMaster1.getContext().getModelContexts().get(0);
        Assert.assertFalse(modelContext1.getDimensionRecItemMap().isEmpty());
    }

    private String getProject() {
        return "newten";
    }
}
