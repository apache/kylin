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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelOptProposer;
import org.apache.kylin.rec.ModelSelectProposer;
import org.apache.kylin.rec.SmartContext;
import org.apache.kylin.rec.SmartMaster;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.QueryResultComparator;
import org.apache.kylin.util.SuggestTestBase;
import org.apache.spark.sql.KapFunctions;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.udf.UdfManager;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

@SuppressWarnings("squid:S2699")
public class AutoBasicTest extends SuggestTestBase {

    @Test
    public void testAutoSingleModel() throws Exception {

        // 1. Create simple model with one fact table
        String targetModelId;
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 1);
            AbstractContext context = proposeWithSmartMaster(queries);
            buildAllModels(kylinConfig, getProject());

            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            AbstractContext.ModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            targetModelId = dataModel.getUuid();
            Assert.assertEquals(1, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 2. Feed query with left join using same fact table, should update same model
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 1, 2);
            AbstractContext context = proposeWithSmartMaster(queries);
            buildAllModels(kylinConfig, getProject());

            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            AbstractContext.ModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            if (!TestUtils.isSkipBuild()) {
                Assert.assertEquals(targetModelId, dataModel.getUuid());
            }
            Assert.assertEquals(2, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 3. Auto suggested model is able to serve related query
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 3);
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            ExecAndCompExt.execAndCompare(queries, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
        }

        // 4. Feed bad queries
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql_bad", 0, 0);
            AbstractContext context = proposeWithSmartMaster(queries);
            buildAllModels(kylinConfig, getProject());

            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            Assert.assertEquals(0, modelContexts.size());
        }

        // 5. Feed query with inner join using same fact table, should create another model
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 3, 4);
            AbstractContext context = proposeWithSmartMaster(queries);
            buildAllModels(kylinConfig, getProject());

            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            AbstractContext.ModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            if (!TestUtils.isSkipBuild()) {
                Assert.assertNotEquals(targetModelId, dataModel.getUuid());
            }
            Assert.assertEquals(2, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 6. Finally, run all queries
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 4);
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            ExecAndCompExt.execAndCompare(queries, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
        }

        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    @Test
    public void testUsedColumnsIsTomb() {
        String[] sqls = new String[] { "select lstg_format_name from test_kylin_fact group by lstg_format_name",
                "select sum(price * item_count) from test_kylin_fact" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        Assert.assertFalse(context.getAccelerateInfoMap().get(sqls[0]).isFailed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(sqls[1]).isFailed());
        NDataModel dataModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, dataModel.getComputedColumnDescs().size());

        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelManager = NDataModelManager.getInstance(config, getProject());
            modelManager.updateDataModel(dataModel.getUuid(), cp -> {
                // delete computed column add a existing column
                cp.getAllNamedColumns().forEach(column -> {
                    if (column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.lstg_format_name")) {
                        column.setStatus(NDataModel.ColumnStatus.TOMB);
                    }
                    if (column.getAliasDotColumn().contains("CC_AUTO_")) {
                        column.setName("modified_cc_column");
                        column.setStatus(NDataModel.ColumnStatus.TOMB);
                    }
                });
                cp.getAllNamedColumns().get(cp.getAllNamedColumns().size() - 1).setStatus(NDataModel.ColumnStatus.TOMB);
                cp.getComputedColumnDescs().clear();
                cp.getComputedColumnUuids().clear();
                cp.getAllMeasures().forEach(measure -> {
                    if (measure.getId() == 100001) {
                        measure.setTomb(true);
                    }
                });
                cp.setMvcc(cp.getMvcc() + 1);
            });
            return true;
        }, getProject());

        // verify update success
        NDataModel updatedModel = NDataModelManager.getInstance(kylinConfig, getProject())
                .getDataModelDesc(dataModel.getUuid());
        Assert.assertTrue(updatedModel.getComputedColumnDescs().isEmpty());
        List<NDataModel.NamedColumn> targetColumns = updatedModel.getAllNamedColumns().stream()
                .filter(column -> column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.lstg_format_name")
                        || column.getAliasDotColumn().contains("CC_AUTO_"))
                .collect(Collectors.toList());
        Assert.assertEquals(2, targetColumns.size());
        targetColumns.forEach(column -> {
            Assert.assertFalse(column.isExist());
            if (column.getAliasDotColumn().contains("CC_AUTO_")) {
                Assert.assertEquals("modified_cc_column", column.getName());
            }
        });
        Assert.assertTrue(updatedModel.getAllMeasures().get(1).isTomb());

        // update model to semi-auto-mode
        MetadataTestUtils.toSemiAutoMode(getProject());
        val context3 = AccelerationUtil.genOptRec(kylinConfig, getProject(), sqls);
        val accelerateInfoMap = context3.getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertFalse(accelerateInfoMap.get(sqls[1]).isNotSucceed());
        List<AbstractContext.ModelContext> modelContexts = context3.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        AbstractContext.ModelContext modelContext = modelContexts.get(0);
        NDataModel model = modelContext.getTargetModel();
        List<NDataModel.Measure> allMeasures = model.getAllMeasures();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        Assert.assertEquals(2, allMeasures.size());
        Map<String, MeasureRecItemV2> measureRecItemMap = modelContext.getMeasureRecItemMap();
        Assert.assertEquals(1, measureRecItemMap.size());
        NDataModel.NamedColumn namedColumn = model.getAllNamedColumns().stream()
                .filter(column -> column.getAliasDotColumn().contains("CC_AUTO_")) //
                .findFirst().orElse(null);
        Assert.assertNotNull(namedColumn);
        Assert.assertEquals(NDataModel.ColumnStatus.EXIST, namedColumn.getStatus());
    }

    @Test
    public void testAutoMultipleModel() throws Exception {

        Map<String, IndexPlan> indexPlanOfParts = new HashMap<>();
        Map<String, IndexPlan> indexPlanOfAll = new HashMap<>();

        // 1. Feed queries part1
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 2);
            AbstractContext context = proposeWithSmartMaster(queries);
            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            for (AbstractContext.ModelContext modelContext : modelContexts) {
                IndexPlan indexPlan = modelContext.getTargetIndexPlan();
                indexPlanOfParts.put(indexPlan.getId(), indexPlan);
            }
        }

        // 2. Feed queries part2
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 2, 4);
            AbstractContext context = proposeWithSmartMaster(queries);
            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            for (AbstractContext.ModelContext modelContext : modelContexts) {
                IndexPlan indexPlan = modelContext.getTargetIndexPlan();
                indexPlanOfParts.put(indexPlan.getId(), indexPlan);
            }
        }

        // 3. Retry all queries
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 4);
            AbstractContext context = proposeWithSmartMaster(queries);
            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            for (AbstractContext.ModelContext modelContext : modelContexts) {
                IndexPlan indexPlan = modelContext.getTargetIndexPlan();
                indexPlanOfAll.put(indexPlan.getId(), indexPlan);
            }
        }

        // 4. Suggested cuboids should be consistent no matter modeling with partial or full queries
        {
            Assert.assertEquals(indexPlanOfParts.size(), indexPlanOfAll.size());
            for (IndexPlan actual : indexPlanOfAll.values()) {
                IndexPlan expected = indexPlanOfParts.get(actual.getId());
                Assert.assertNotNull(expected);
                // compare cuboids
                Assert.assertEquals(expected.getAllIndexes().size(), actual.getAllIndexes().size());
                Assert.assertEquals(expected.getAllLayouts().size(), actual.getAllLayouts().size());
                for (IndexEntity actualCuboid : actual.getAllIndexes()) {
                    IndexEntity expectedCuboid = expected.getIndexEntity(actualCuboid.getId());
                    MatcherAssert.assertThat(expectedCuboid.getDimensions(),
                            CoreMatchers.is(actualCuboid.getDimensions()));
                    MatcherAssert.assertThat(expectedCuboid.getMeasures(), CoreMatchers.is(actualCuboid.getMeasures()));
                }
            }
        }

        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    /**
     * Test a query only only with count(*), can build and query from IndexPlan,
     * don't move it.
     */
    @Test
    public void testCountStar() throws Exception {
        new TestScenario(ExecAndCompExt.CompareLevel.SAME, "sql_for_automodeling/sql_count_star").execute();
    }

    @Test
    public void testSelectTimestamp() throws Exception {
        new TestScenario(ExecAndCompExt.CompareLevel.SAME, "sql_for_automodeling/sql_timestamp").execute();
    }

    @Test
    public void testLimitCorrectness() throws Exception {
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        new TestScenario(ExecAndCompExt.CompareLevel.SAME, true, "query/sql").execute();
    }

    /**
     * (auto-modeling) one sql generates many OlapContexts, but it failed to accelerate.
     * The second OlapContext failed to propose cc when proposing target model.
     */
    @Test
    public void testPartialFailedWhenProposingWhenOneSqlAccelerating() {
        KylinConfig kylinConfig = getTestConfig();
        final String project = "newten";
        String sql = "select l.cal_dt, sum(left_join_gvm) as left_join_sum, sum(inner_join_gvm) as inner_join_sum\n" //
                + "from (\n" //
                + "    select test_kylin_fact.cal_dt, sum(price) as left_join_gvm\n" //
                + "    from test_kylin_fact " //
                + "       left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "       left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id " //
                + "         AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" //
                + "    group by test_kylin_fact.cal_dt\n" //
                + "  ) l inner join (\n" //
                + "    select t2.cal_dt, SUM(PRICE_TOTAL + 1) as inner_join_gvm\n" //
                + "    from (select price*item_count as price_total, cal_dt, leaf_categ_id, lstg_site_id from test_kylin_fact) t2 \n" //
                + "        inner JOIN edw.test_cal_dt as test_cal_dt ON t2.cal_dt = test_cal_dt.cal_dt\n" //
                + "        inner JOIN test_category_groupings ON t2.leaf_categ_id = test_category_groupings.leaf_categ_id " //
                + "          AND t2.lstg_site_id = test_category_groupings.site_id\n" //
                + "    group by t2.cal_dt\n" //
                + "  ) i on l.cal_dt = i.cal_dt\n" //
                + "group by l.cal_dt";

        val context = new SmartContext(kylinConfig, project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        smartMaster.getProposer("ModelSelectProposer").execute();

        // assert everything is ok after select model
        val accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertTrue(accelerateInfoMap.get(sql).getRelatedLayouts().isEmpty());
        smartMaster.getProposer("ModelOptProposer").execute();

        // assert it failed in the step of optimize model
        final List<AbstractContext.ModelContext> modelContexts = smartMaster.getContext().getModelContexts();
        val accelerateInfoMapAfterOpt = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(2, modelContexts.size());
        Assert.assertFalse(accelerateInfoMapAfterOpt.get(sql).isNotSucceed());
    }

    @Test
    public void testSemiAutoWillCreateNewLayouts() {
        KylinConfig kylinConfig = getTestConfig();
        final String project = "newten";
        String sql = "select test_kylin_fact.cal_dt, sum(price) as left_join_gvm\n" //
                + "    from test_kylin_fact "
                + "       left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "       left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "         AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "    group by test_kylin_fact.cal_dt";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, project, new String[] { sql }, true);

        // confirm auto-modeling is ok
        val accelerateInfoMap = context.getAccelerateInfoMap();
        val modelContexts = context.getModelContexts();
        Assert.assertFalse(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertEquals(1, modelContexts.size());
        IndexPlan targetIndexPlan = modelContexts.get(0).getTargetIndexPlan();
        Assert.assertEquals(1, targetIndexPlan.getAllLayouts().size());

        //set maintain model type to manual
        MetadataTestUtils.toSemiAutoMode(project);

        // propose model under the scene of manual maintain type
        sql = "select l.cal_dt, sum(left_join_gvm) as left_join_sum, sum(inner_join_gvm) as inner_join_sum\n"
                + "from (\n" //
                + "    select test_kylin_fact.cal_dt, sum(price) as left_join_gvm\n" //
                + "    from test_kylin_fact "
                + "       left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "       left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "         AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "    group by test_kylin_fact.cal_dt\n" //
                + "  ) l inner join (\n" //
                + "    select test_kylin_fact.cal_dt, sum(price+1) as inner_join_gvm\n" //
                + "    from test_kylin_fact\n" //
                + "        left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "        left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "          AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "    group by test_kylin_fact.cal_dt\n" //
                + "  ) i on l.cal_dt = i.cal_dt\n" //
                + "group by l.cal_dt";
        val context2 = AccelerationUtil.runModelReuseContext(kylinConfig, project, new String[] { sql });
        // assert everything is ok after optimize model
        val modelContextsOfSemi = context2.getModelContexts();
        Assert.assertEquals(1, modelContextsOfSemi.size());
        IndexPlan indexPlanOfSemi = modelContextsOfSemi.get(0).getTargetIndexPlan();
        Assert.assertEquals(2, indexPlanOfSemi.getAllLayouts().size());
        val accelerationMapOfSemiMode = context2.getAccelerateInfoMap();
        Assert.assertFalse(accelerationMapOfSemiMode.get(sql).isNotSucceed());
        Assert.assertEquals(2, accelerationMapOfSemiMode.get(sql).getRelatedLayouts().size());
    }

    @Test
    public void testNoCompatibleModelToReuse() {
        String[] sqls = { "select cal_dt from test_kylin_fact",
                "select lstg_format_name from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sqls[0] }, true);
        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        Assert.assertFalse(context.getAccelerateInfoMap().get(sqls[0]).isNotSucceed());

        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls);
        val modelContexts2 = context2.getModelContexts();
        Assert.assertEquals(2, modelContexts2.size());
        Assert.assertFalse(context2.getAccelerateInfoMap().get(sqls[0]).isNotSucceed());
        AccelerateInfo accelerateInfo = context2.getAccelerateInfoMap().get(sqls[1]);
        Assert.assertTrue(accelerateInfo.isNotSucceed());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfo.getPendingMsg());
        Assert.assertEquals(ModelOptProposer.NO_COMPATIBLE_MODEL_MSG,
                Throwables.getRootCause(accelerateInfo.getFailedCause()).getMessage());
    }

    @Test
    public void testReuseAndCreateNewModel() {
        String[] sqls = { "select cal_dt from test_kylin_fact",
                "select cal_dt, lstg_format_name, sum(price * 0.8) from test_kylin_fact group by cal_dt, lstg_format_name",
                "select lstg_format_name, price from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", new String[] { sqls[0] }, true);
        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();
        Assert.assertNotNull(targetModel);
        Assert.assertFalse(context.getAccelerateInfoMap().get(sqls[0]).isNotSucceed());

        val context2 = AccelerationUtil.runModelReuseContext(getTestConfig(), "newten", sqls, true);
        val modelContexts2 = context2.getModelContexts();
        Assert.assertEquals(2, modelContexts2.size());
        Assert.assertFalse(context2.getAccelerateInfoMap().get(sqls[0]).isNotSucceed());
        Assert.assertFalse(context2.getAccelerateInfoMap().get(sqls[1]).isNotSucceed());
        Assert.assertFalse(context2.getAccelerateInfoMap().get(sqls[2]).isNotSucceed());
        AbstractContext.ModelContext modelContext1 = modelContexts2.get(0);
        Assert.assertEquals("AUTO_MODEL_TEST_KYLIN_FACT_1", modelContext1.getTargetModel().getAlias());
        Assert.assertEquals(1, modelContext1.getCcRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getIndexRexItemMap().size());
        AbstractContext.ModelContext modelContext2 = modelContexts2.get(1);
        Assert.assertEquals("AUTO_MODEL_TEST_KYLIN_FACT_2", modelContext2.getTargetModel().getAlias());
        Assert.assertEquals(0, modelContext2.getCcRecItemMap().size());
        Assert.assertEquals(2, modelContext2.getDimensionRecItemMap().size());
        Assert.assertEquals(0, modelContext2.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext2.getIndexRexItemMap().size());
    }

    @Test
    public void testIndexReducer() {
        // use smart-model to prepare a model
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();

        String[] sqls = {
                "select LSTG_FORMAT_NAME,slr_segment_cd ,sum(price) as GMV from test_kylin_fact\n"
                        + " group by LSTG_FORMAT_NAME ,slr_segment_cd",
                "select LSTG_FORMAT_NAME,slr_segment_cd ,sum(price) as GMV, min(price) as MMV from test_kylin_fact\n"
                        + " group by LSTG_FORMAT_NAME ,slr_segment_cd" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, project, sqls, true);

        Map<String, AccelerateInfo> accelerationInfoMap = context.getAccelerateInfoMap();
        val relatedLayoutsForSql0 = accelerationInfoMap.get(sqls[0]).getRelatedLayouts();
        val relatedLayoutsForSql1 = accelerationInfoMap.get(sqls[1]).getRelatedLayouts();
        long layoutForSql0 = relatedLayoutsForSql0.iterator().next().getLayoutId();
        long layoutForSql1 = relatedLayoutsForSql1.iterator().next().getLayoutId();
        Assert.assertEquals(layoutForSql0, layoutForSql1);

        // set to semi-auto to check tailoring layouts
        MetadataTestUtils.toSemiAutoMode(project);
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        NDataModel targetModel = modelContext.getTargetModel();
        NIndexPlanManager.getInstance(kylinConfig, project).updateIndexPlan(targetModel.getUuid(),
                copyForWrite -> copyForWrite.setIndexes(Lists.newArrayList()));

        val context2 = AccelerationUtil.genOptRec(kylinConfig, project, sqls);
        accelerationInfoMap = context2.getAccelerateInfoMap();
        val relatedLayoutsSemiForSql0 = accelerationInfoMap.get(sqls[0]).getRelatedLayouts();
        val relatedLayoutsSemiForSql1 = accelerationInfoMap.get(sqls[1]).getRelatedLayouts();
        long layoutSemiForSql0 = relatedLayoutsSemiForSql0.iterator().next().getLayoutId();
        long layoutSemiForSql1 = relatedLayoutsSemiForSql1.iterator().next().getLayoutId();
        Assert.assertEquals(layoutSemiForSql0, layoutSemiForSql1);
    }

    @Test
    public void testPercentileApprox() throws IOException, InterruptedException {
        List<Pair<String, String>> queries = fetchQueries("query/sql_percentile_2", 0, 0);
        String sql01 = queries.get(0).getSecond();
        String sql02 = queries.get(1).getSecond();

        double actualValue = 2467.5;
        double tDigestValue = 2467.5;
        double quantileSummaryValue = 2273;

        // check ss
        SparkSession originSS = SparderEnv.getSparkSession();
        getTestConfig().setProperty("kylin.query.percentile-approx-algorithm", "t-digest");
        SparderEnv.doInitSpark();
        Assert.assertTrue(SparderEnv.getSparkSession().sessionState().functionRegistry()
                .functionExists(KapFunctions.percentileFunction().name()));
        SparderEnv.setSparkSession(originSS);
        getTestConfig().setProperty("kylin.query.percentile-approx-algorithm", "");

        // PushDown
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        QueryResult sparkResult = ExecAndComp.queryWithSpark(getProject(), sql01, "default", queries.get(0).getFirst(),
                false);
        List<Double> values = sparkResult.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(actualValue, quantileSummaryValue), values);

        sparkResult = ExecAndComp.queryWithSpark(getProject(), sql02, "default", queries.get(0).getFirst(), false);
        values = sparkResult.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(actualValue, quantileSummaryValue), values);

        try {
            UdfManager.register(ss, KapFunctions.percentileFunction());
            sparkResult = ExecAndComp.queryWithSpark(getProject(), sql01, "default", queries.get(0).getFirst(), false);
            values = sparkResult.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                    .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList(actualValue, tDigestValue), values);

            sparkResult = ExecAndComp.queryWithSpark(getProject(), sql02, "default", queries.get(0).getFirst(), false);
            values = sparkResult.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                    .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList(actualValue, tDigestValue), values);
        } finally {
            ss.sessionState().functionRegistry().dropFunction(KapFunctions.percentileFunction().name());
        }

        // Build model
        proposeWithSmartMaster(queries);
        buildAllModels(kylinConfig, getProject());

        // PreAggregate
        ExecAndComp.EnhancedQueryResult result01 = ExecAndCompExt.queryModelWithOlapContext(getProject(), "default",
                sql01);
        values = result01.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(tDigestValue, tDigestValue), values);

        // PostAggregate
        ExecAndComp.EnhancedQueryResult result02 = ExecAndCompExt.queryModelWithOlapContext(getProject(), "default",
                sql02);
        values = result02.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(quantileSummaryValue, quantileSummaryValue), values);

        getTestConfig().setProperty("kylin.query.percentile-approx-algorithm", "t-digest");
        result02 = ExecAndCompExt.queryModelWithOlapContext(getProject(), "default", sql02);
        values = result02.getRowsIterable().iterator().next().stream().map(Double::parseDouble)
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(tDigestValue, tDigestValue), values);
    }

    @Test
    public void testStringPlus() throws IOException, InterruptedException {
        Assert.assertFalse(KylinConfig.getInstanceFromEnv().isCalciteCompatibleWithMsSqlPlusEnabled());
        List<Pair<String, String>> queries = fetchQueries("query/sql_string_plus", 0, 1);
        String sql = queries.get(0).getSecond();

        proposeWithSmartMaster(queries);
        buildAllModels(kylinConfig, getProject());
        ExecAndComp.EnhancedQueryResult result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "left", sql);
        QueryResult expect = new QueryResult(
                Arrays.asList(Arrays.asList("3.3", "1.21", "-0.7211000000000001", null, null, null, "3"),
                        Arrays.asList("712.0", "712.0", "712.0", null, null, null, null)),
                6, result.getColumns());
        Assert.assertTrue(
                QueryResultComparator.compareResults(expect, result.getQueryResult(), ExecAndComp.CompareLevel.SAME));

    }

    @Test
    public void testStringPlus2() throws InterruptedException, IOException {
        Assert.assertFalse(KylinConfig.getInstanceFromEnv().isCalciteCompatibleWithMsSqlPlusEnabled());
        List<Pair<String, String>> queries = fetchQueries("query/sql_string_plus", 0, 1);
        String sql = queries.get(0).getSecond();
        overwriteSystemProp("calcite.compatible-with-mssql-plus", "true");

        proposeWithSmartMaster(queries);
        buildAllModels(kylinConfig, getProject());
        ExecAndComp.EnhancedQueryResult result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "left", sql);
        QueryResult expected = new QueryResult(
                Arrays.asList(Arrays.asList("3.3", "1.21", "1.3-2.0211", null, "a1.33", "ab", "12"),
                        Arrays.asList("712.0", "712.0", "356356", null, "Auction356", "AuctionAuction", "1Auction")),
                6, result.getColumns());
        Assert.assertTrue(
                QueryResultComparator.compareResults(expected, result.getQueryResult(), ExecAndComp.CompareLevel.SAME));

        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    @Test
    public void testNumberPlus() throws IOException {
        Assert.assertFalse(KylinConfig.getInstanceFromEnv().isCalciteCompatibleWithMsSqlPlusEnabled());
        List<Pair<String, String>> queries = fetchQueries("query/sql_number_plus", 0, 2);
        String sql = queries.get(0).getSecond();

        ExecAndComp.EnhancedQueryResult result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "left", sql);
        QueryResult expect = new QueryResult(Collections.singletonList(Collections.singletonList("50")), 1,
                result.getColumns());
        Assert.assertTrue(
                QueryResultComparator.compareResults(expect, result.getQueryResult(), ExecAndComp.CompareLevel.SAME));

        sql = queries.get(1).getSecond();
        result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "left", sql);
        expect = new QueryResult(Collections.singletonList(Collections.singletonList("50")), 1, result.getColumns());
        Assert.assertTrue(
                QueryResultComparator.compareResults(expect, result.getQueryResult(), ExecAndComp.CompareLevel.SAME));

    }

    @Test
    public void testConstantStringPlus() throws IOException {
        Assert.assertFalse(KylinConfig.getInstanceFromEnv().isCalciteCompatibleWithMsSqlPlusEnabled());
        List<Pair<String, String>> queries = fetchQueries("query/sql_string_plus_constant", 0, 1);
        String sql = queries.get(0).getSecond();

        List<String> expect = new ArrayList<>(Arrays.asList("3", "3.0", null, "3.0", "3.0", null, null, null, null,
                "3.0", "3.0", null, "3.0", "3.0"));
        for (int i = expect.size(); i < 27; i++)
            expect.add(null);
        ExecAndComp.EnhancedQueryResult result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "default", sql);
        List<String> values = result.getQueryResult().getRowsIterable().iterator().next();

        Assert.assertEquals(expect, values);

        overwriteSystemProp("calcite.compatible-with-mssql-plus", "true");
        expect = new ArrayList<>(Arrays.asList("3", "3.0", null, "3.0", "3.0", null, null, null, null, //
                "3.0", "3.0", null, "12.0", "111", "11a", null, "1a1", "1aa", //
                null, null, null, null, "a11", "a1a", null, "aa1", "aaa"));
        result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "default", sql);
        values = result.getQueryResult().getRowsIterable().iterator().next();

        Assert.assertEquals(expect, values);
    }

    @Test
    public void testQueryWithDuplicatedJoinKey() throws IOException, InterruptedException {
        List<Pair<String, String>> queries = fetchQueries("query/sql_duplicated_join_key", 0, 0);
        String sql = queries.get(2).getSecond();
        proposeWithSmartMaster(queries.subList(0, 2));
        buildAllModels(kylinConfig, getProject());
        ExecAndComp.EnhancedQueryResult result = ExecAndCompExt.queryModelWithOlapContext(getProject(), "default", sql);
        List<String> values = result.getQueryResult().getRowsIterable().iterator().next();
        Assert.assertEquals(2, result.getOlapContexts().size());
        Assert.assertEquals(Collections.singletonList("14"), values);

        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    private AbstractContext proposeWithSmartMaster(List<Pair<String, String>> queries) {
        String[] sqls = queries.stream().map(Pair::getSecond).toArray(String[]::new);
        return AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
    }
}
