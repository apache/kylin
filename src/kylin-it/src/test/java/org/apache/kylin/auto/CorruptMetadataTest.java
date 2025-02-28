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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import lombok.val;

public class CorruptMetadataTest extends SuggestTestBase {

    @Test
    public void testIndexMissingDimension() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "index_missing_dimension", sqls, true);

        assertAccelerationInfoMap(sqls, context);
        String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
        String expectedCauseMeassage = "layout 1's dimension is illegal";
        Exception e = (Exception) context.getAccelerateInfoMap().get(sqls[0]).getFailedCause();
        assertWithException(e, expectedMessage, expectedCauseMeassage);
    }

    @Test
    public void testIndexMissingMeasure() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "index_missing_measure", sqls, true);

        assertAccelerationInfoMap(sqls, context);
        String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
        String expectedCauseMessage = "layout 1's measure is illegal";
        Exception e = (Exception) context.getAccelerateInfoMap().get(sqls[0]).getFailedCause();
        assertWithException(e, expectedMessage, expectedCauseMessage);
    }

    @Test
    public void testCorruptColOrder() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "corrupt_colOrder", sqls, true);

        assertAccelerationInfoMap(sqls, context);
        String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
        String expectedCauseMessage = "layout 1's measure is illegal";
        Exception e = (Exception) context.getAccelerateInfoMap().get(sqls[0]).getFailedCause();
        assertWithException(e, expectedMessage, expectedCauseMessage);
    }

    /**
     * The result of auto-modeling will recommend three indexes, but only one index exists
     * in the currently provided model json file.
     */
    @Test
    public void testMissingOneIndex() {
        overwriteSystemProp("kylin.query.print-logical-plan", "true");
        String[] sqls = new String[] { "select a.*, test_kylin_fact.lstg_format_name as lstg_format_name \n"
                + "from ( select cal_dt, sum(price) as sum_price from test_kylin_fact\n"
                + "         where cal_dt > '2010-01-01' group by cal_dt) a \n"
                + "join test_kylin_fact on a.cal_dt = test_kylin_fact.cal_dt \n"
                + "group by lstg_format_name, a.cal_dt, a.sum_price" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "index_missing", sqls, true);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().size());

        final List<IndexEntity> originalIndexes = modelContext.getOriginIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, originalIndexes.get(0).getId());

        Assert.assertEquals(3, modelContext.getTargetIndexPlan().getAllIndexes().size());
        final List<IndexEntity> targetIndexes = modelContext.getTargetIndexPlan().getAllIndexes().stream()
                .sorted(Comparator.comparing(IndexEntity::getId)).collect(Collectors.toList());
        Assert.assertEquals(10000L, targetIndexes.get(0).getId());
        Assert.assertEquals(20000L, targetIndexes.get(1).getId());
        Assert.assertEquals(20000000000L, targetIndexes.get(2).getId());
    }

    @Test
    public void testMissingOneIndexManually() {
        MetadataTestUtils.toPureExpertMode("index_missing");
        String[] sqls = new String[] { "select a.*, test_kylin_fact.lstg_format_name as lstg_format_name \n"
                + "from ( select cal_dt, sum(price) as sum_price from test_kylin_fact\n"
                + "         where cal_dt > '2010-01-01' group by cal_dt) a \n"
                + "join test_kylin_fact on a.cal_dt = test_kylin_fact.cal_dt \n"
                + "group by lstg_format_name, a.cal_dt, a.sum_price" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "index_missing", sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().size());

        final List<IndexEntity> originalIndexes = modelContext.getOriginIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, originalIndexes.get(0).getId());

        Assert.assertEquals(3, modelContext.getTargetIndexPlan().getAllIndexes().size());
        final List<IndexEntity> targetIndexes = modelContext.getTargetIndexPlan().getAllIndexes().stream()
                .sorted(Comparator.comparing(IndexEntity::getId)).collect(Collectors.toList());
        Assert.assertEquals(10000L, targetIndexes.get(0).getId());
        Assert.assertEquals(20000L, targetIndexes.get(1).getId());
        Assert.assertEquals(20000000000L, targetIndexes.get(2).getId());
    }

    /**
     * The result of auto-modeling will recommend a index with two layouts, but only one layout exists
     * in the currently provided IndexPlan json file.
     */
    @Test
    public void testMissingLayout() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where lstg_format_name = 'xxx' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt",
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where cal_dt > '2012-02-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "index_missing_layout", sqls, true);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().get(0).getLayouts().size());

        final List<LayoutEntity> originalLayouts = modelContext.getOriginIndexPlan().getAllIndexes().get(0)
                .getLayouts();
        Assert.assertEquals(2, originalLayouts.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts().size());
        final List<LayoutEntity> layouts = modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts();
        Assert.assertEquals(2, layouts.get(0).getId());
        Assert.assertEquals(3, layouts.get(1).getId());
    }

    @Test
    public void testMissingLayoutManually() {
        MetadataTestUtils.toPureExpertMode("index_missing_layout");
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where lstg_format_name = 'xxx' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt",
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where cal_dt > '2012-02-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "index_missing_layout", sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().get(0).getLayouts().size());

        final List<LayoutEntity> originalLayouts = modelContext.getOriginIndexPlan().getAllIndexes().get(0)
                .getLayouts();
        Assert.assertEquals(2, originalLayouts.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts().size());
        final List<LayoutEntity> layouts = modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts();
        Assert.assertEquals(2, layouts.get(0).getId());
        Assert.assertEquals(3, layouts.get(1).getId());
    }

    /**
     * There are no indexes in the IndexPlan's json file
     */
    @Test
    public void testLoadEmptyCubePlan() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "index_plan_empty", sqls, true);
        AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(0, modelContexts.get(0).getOriginIndexPlan().getAllIndexes().size());
        Assert.assertEquals(1, modelContexts.get(0).getTargetIndexPlan().getAllIndexes().size());
    }

    @Test
    public void testLoadEmptyCubePlanManually() {
        MetadataTestUtils.toPureExpertMode("index_plan_empty");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "index_plan_empty", sqls);
        AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(0, modelContexts.get(0).getOriginIndexPlan().getAllIndexes().size());
        Assert.assertEquals(1, modelContexts.get(0).getTargetIndexPlan().getAllIndexes().size());
    }

    /**
     * There are no columns and measures in the model's json file
     */
    @Test
    public void testLoadEmptyModel() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), "model_empty", sqls, true);

        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sqls[0]).isFailed());
        final AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel());
        Assert.assertNull(modelContext.getOriginIndexPlan());
        Assert.assertNotNull(modelContext.getTargetModel());
        Assert.assertNotNull(modelContext.getTargetIndexPlan());
    }

    @Test
    public void testLoadEmptyModelManually() {
        MetadataTestUtils.toPureExpertMode("model_empty");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "model_empty", sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        Assert.assertEquals(expectedMessage, accelerateInfo.getPendingMsg());
        Assert.assertNull(context.getModelContexts().get(0).getOriginModel());
        Assert.assertNull(context.getModelContexts().get(0).getTargetModel());
    }

    @Ignore("Not support yet")
    @Test
    public void testModelWithoutIndexPlan() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "model_without_index_plan", sqls, true);
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sqls[0]).isFailed());
        final AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel());
        Assert.assertNull(modelContext.getOriginIndexPlan());
        Assert.assertNotNull(modelContext.getTargetModel());
        Assert.assertNotNull(modelContext.getTargetIndexPlan());
    }

    @Test
    public void testModelWithoutIndexPlanManually() {
        MetadataTestUtils.toPureExpertMode("model_without_index_plan");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "model_without_index_plan", sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        Assert.assertEquals(expectedMessage, accelerateInfo.getPendingMsg());
        Assert.assertNull(context.getModelContexts().get(0).getOriginModel());
        Assert.assertNull(context.getModelContexts().get(0).getTargetModel());
    }

    // Losing the join table and measure will produce the same result, no longer shown here
    @Test
    public void testModelMissingUsedColumn() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "model_missing_column", sqls, true);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        final AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel());
        Assert.assertEquals(11, modelContext.getTargetModel().getEffectiveCols().size());
    }

    @Test
    public void testModelMissingColumnManually() {
        MetadataTestUtils.toPureExpertMode("model_missing_column");

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "model_missing_column", sqls);
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertEquals(expectedMessage, accelerateInfo.getPendingMsg());
        Assert.assertNull(context.getModelContexts().get(0).getOriginModel());
        Assert.assertNull(context.getModelContexts().get(0).getOriginIndexPlan());
        Assert.assertNull(context.getModelContexts().get(0).getTargetModel());
        Assert.assertNull(context.getModelContexts().get(0).getTargetIndexPlan());
    }

    /**
     * Missing column in the model and no IndexPlan json file.
     */
    @Test
    public void testModelMissingUnusedColumn() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "flaw_model", sqls, true);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        final AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(7, modelContext.getOriginModel().getEffectiveCols().size());
        Assert.assertEquals(11, modelContext.getTargetModel().getEffectiveCols().size());
    }

    @Ignore("Semi-Auto-mode need a new design")
    @Test
    public void testModelMissingUnusedColumnManually() {
        MetadataTestUtils.toPureExpertMode("flaw_model");

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationUtil.runModelReuseContext(getTestConfig(), "flaw_model", sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        final AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(modelContext.getTargetModel(), modelContext.getOriginModel());
        Assert.assertEquals(7, modelContext.getOriginModel().getEffectiveCols().size());
    }

    private void assertAccelerationInfoMap(String[] sqls, AbstractContext context) {
        if (context != null) {
            Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
            Assert.assertTrue(accelerateInfo.isFailed());
            Assert.assertTrue(accelerateInfo.getRelatedLayouts().isEmpty());
        }
    }

    private void assertWithException(Exception e, String expectedMessage, String expectedCauseMessage) {
        Assert.assertTrue(e instanceof TransactionException);
        Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        Assert.assertTrue(e.getMessage().startsWith(expectedMessage));
        Assert.assertTrue(e.getCause().getMessage().startsWith(expectedCauseMessage));
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/corrupt_metadata");
        kylinConfig = getTestConfig();
    }

    @Override
    public String[] getOverlay() {
        return new String[] { "src/test/resources/corrupt_metadata" };
    }
}
