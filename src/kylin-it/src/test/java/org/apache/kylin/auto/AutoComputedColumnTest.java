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
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModel.Measure;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.OlapContextTestUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class AutoComputedColumnTest extends SuggestTestBase {
    private static final String PRICE_COLUMN = "\"TEST_KYLIN_FACT\".\"PRICE\"";
    private static final String ITEM_COUNT_COLUMN = "\"TEST_KYLIN_FACT\".\"ITEM_COUNT\"";
    private static final String FORMAT_COLUMN_NAME_COLUMN = "\"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"";
    private static final String SELLER_ID_COLUMN = "\"TEST_KYLIN_FACT\".\"SELLER_ID\"";
    private static final String PRICE_MULTIPLY_ITEM_COUNT = "\"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\"";
    private static final String PRICE_MULTIPLY_ITEM_COUNT_PLUS_ONE = PRICE_MULTIPLY_ITEM_COUNT + " + 1";
    private static final String PRICE_MULTIPLY_ITEM_COUNT_PLUS_TEN = PRICE_MULTIPLY_ITEM_COUNT + " + 10";

    private static final String PRICE_PLUS_ITEM_COUNT = "\"TEST_KYLIN_FACT\".\"PRICE\" + \"TEST_KYLIN_FACT\".\"ITEM_COUNT\"";

    @Before
    public void setupCCConf() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        overwriteSystemProp("kylin.query.optimized-sum-cast-double-rule-enabled", "FALSE");
    }

    @Test
    public void testTableIndexCCReuse() {
        String[] sqlWithCcExp = new String[] {
                "select seller_id ,sum(ITEM_COUNT * PRICE), count(1) from test_kylin_fact group by LSTG_FORMAT_NAME ,seller_id" };

        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqlWithCcExp, true);

        String[] sqls = new String[] { "select LSTG_FORMAT_NAME,ITEM_COUNT * PRICE * PRICE from test_kylin_fact",
                "select seller_id ,sum(ITEM_COUNT * PRICE * PRICE) as GMVM from test_kylin_fact group by LSTG_FORMAT_NAME ,seller_id" };

        val context2 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls);
        AccelerationUtil.onlineModel(context2);

        NDataModel targetModel = context2.getModelContexts().get(0).getTargetModel();
        IndexPlan targetIndex = context2.getModelContexts().get(0).getTargetIndexPlan();

        String expectedInnerExp0 = "(`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`) * `TEST_KYLIN_FACT`.`PRICE`";
        String expectedInnerExp1 = "`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`";
        List<ComputedColumnDesc> ccList = targetModel.getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals(expectedInnerExp0, ccList.get(0).getInnerExpression());
        Assert.assertEquals(expectedInnerExp1, ccList.get(1).getInnerExpression());

        IndexEntity tableIndex = targetIndex.getAllIndexes().stream().filter(IndexEntity::isTableIndex).findFirst()
                .orElse(null);
        Assert.assertNotNull(tableIndex);
        Assert.assertEquals(1, tableIndex.getLayouts().size());
        List<TblColRef> columns = tableIndex.getLayouts().get(0).getColumns();
        Assert.assertEquals(2, columns.size());
        columns.forEach(col -> {
            if (col.getColumnDesc().isComputedColumn()) {
                String columnExpr = col.getColumnDesc().getComputedColumnExpr();
                Assert.assertEquals(expectedInnerExp0, columnExpr);
                String doubleQuoteInnerExpr = col.getColumnDesc().getDoubleQuoteInnerExpr();
                Assert.assertEquals(expectedInnerExp0.replace('`', '"'), doubleQuoteInnerExpr);
            }
        });
    }

    @Test
    public void testComputedColumnSingle() throws SqlParseException {
        String query = "SELECT SUM(PRICE * ITEM_COUNT + 1), AVG(PRICE * ITEM_COUNT + 1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT_PLUS_ONE, cc.getExpression());

        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query);
        OlapContext olapContext = olapContexts.get(0);
        OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
        Assert.assertEquals(2, olapContext.getAggregations().size());
        for (FunctionDesc aggregation : olapContext.getAggregations()) {
            String collect = aggregation.getParameters().stream()
                    .map(param -> param.getColRef().getColumnDesc().getIdentity()).collect(Collectors.joining(","));
            Assert.assertTrue(collect.startsWith("TEST_KYLIN_FACT.CC_AUTO_"));
        }
    }

    @Test
    public void testComputedColumnMultiple() throws SqlParseException {
        String query = "SELECT SUM(PRICE * ITEM_COUNT + 1), AVG(PRICE * ITEM_COUNT * 0.9), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        String expectedInnerExp0 = PRICE_MULTIPLY_ITEM_COUNT + " * 0.9";
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals(expectedInnerExp0, ccList.get(0).getExpression());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT_PLUS_ONE, ccList.get(1).getExpression());

        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query);
        OlapContext olapContext = olapContexts.get(0);
        OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
        Assert.assertEquals(3, olapContext.getAggregations().size());
        for (FunctionDesc aggregation : olapContext.getAggregations()) {
            String collect = aggregation.getParameters().stream()
                    .map(param -> param.getColRef().getColumnDesc().getIdentity()).collect(Collectors.joining(","));
            Assert.assertTrue(collect.startsWith("TEST_KYLIN_FACT.CC_AUTO_"));
        }
    }

    /*
     * test points: 1. support propose more than one cc
     *              2. tolerance of failed sql
     *              3. unsupported sql in current calcite version but can propose
     */
    @Test
    public void testProposeMultiCCToOneModel() {
        // The 'price*item_count' should be replaced by auto1
        // The 'price+item_count' will produce another cc expression auto2.
        // (query5: left() supported by CALCITE-3005, left() supported by spark2.3+).
        String query1 = "select price*item_count from test_kylin_fact";
        String query2 = "select sum(price*item_count) from test_kylin_fact"; // one cc
        String query3 = "select sum(price*item_count), price from test_kylin_fact group by price";
        String query4 = "select sum(price+item_count) from test_kylin_fact"; // another cc
        String query5 = "select {fn left(lstg_format_name,-4)} as name, sum(price*item_count) "
                + "from test_kylin_fact group by lstg_format_name"; // left(...) will replaced by substring(...)
        String query6 = "select  {fn CHAR(lstg_format_name)}, sum(price*item_count) "
                + "from test_kylin_fact group by lstg_format_name";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { query1, query2, query3, query4, query5, query6 }, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        List<ComputedColumnDesc> computedColumns = modelContexts.get(0).getTargetModel().getComputedColumnDescs();
        computedColumns.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, computedColumns.get(0).getExpression());
        Assert.assertEquals(PRICE_PLUS_ITEM_COUNT, computedColumns.get(1).getExpression());

        val accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(query1).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query2).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query3).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query4).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query4).isFailed());

        val targetIndexPlan = modelContexts.get(0).getTargetIndexPlan();
        final List<IndexEntity> indexes = targetIndexPlan.getIndexes();
        indexes.sort(Comparator.comparing(IndexEntity::getId));
        Assert.assertEquals(10000L, indexes.get(0).getId());
        Assert.assertEquals(30000L, indexes.get(1).getId());
        Assert.assertEquals(40000L, indexes.get(2).getId());
        Assert.assertEquals(20000000000L, indexes.get(3).getId());
    }

    @Test
    public void testProposeCCToDifferentModelWithSameRootFactTable() {
        // different model share the same cc for having the same root fact table
        String query1 = "select sum(price * item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        String query2 = "select sum(price * item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "left join test_country on test_account.account_country = test_country.country";
        String query3 = "select sum(price + item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { query1, query2, query3 }, true);
        val modelContexts = context.getModelContexts();
        Assert.assertEquals(2, modelContexts.size());

        // case 1: different cc expression
        val ccList = modelContexts.get(0).getTargetModel().getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, ccList.get(0).getExpression());
        Assert.assertEquals(PRICE_PLUS_ITEM_COUNT, ccList.get(1).getExpression());

        // case 2: same cc expression
        val suggestedCC3 = modelContexts.get(1).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals(ccList.get(0).getColumnName(), suggestedCC3.getColumnName());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC3.getExpression());
    }

    @Test
    public void testProposedMultiCCToDifferentModelWithDifferentRootFactTable() {
        String query1 = "select sum(price*item_count) from test_kylin_fact";
        String query2 = "select sum(price*item_count), account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                + "group by account_id";
        String query3 = "select sum(price + item_count) from test_order inner join test_kylin_fact "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { query1, query2, query3 }, true);

        // suggestedCC1, suggestedCC2 and suggestedCC3  will be added to different root fact table
        val modelContexts = context.getModelContexts();
        Assert.assertEquals(3, modelContexts.size());
        val suggestedCC1 = modelContexts.get(0).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC1.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC1.getExpression());
        val suggestedCC2 = modelContexts.get(1).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", suggestedCC2.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC2.getExpression());
        Assert.assertNotEquals(suggestedCC1.getColumnName(), suggestedCC2.getColumnName());
        val suggestedCC3 = modelContexts.get(2).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("DEFAULT.TEST_ORDER", suggestedCC3.getTableIdentity());
        Assert.assertEquals(PRICE_PLUS_ITEM_COUNT, suggestedCC3.getExpression());
    }

    @Test
    public void testReproposeUseExistingModel() {
        // init a model with cc
        String query1 = "select sum(price*item_count), price from test_kylin_fact group by price";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1 }, true);
        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val modelContext = modelContexts.get(0);
        val computedColumns = modelContext.getTargetModel().getComputedColumnDescs();
        val suggestedCC = computedColumns.get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC.getExpression());

        // case 1: cannot use existing cc for different cc expression
        String query2 = "select sum(price+item_count) from test_kylin_fact"; // another cc
        val context1 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query2 }, true);
        val modelContextsList1 = context1.getModelContexts();
        Assert.assertEquals(1, modelContextsList1.size());
        val modelContext1 = modelContextsList1.get(0);
        Assert.assertNotNull(modelContext1.getOriginModel());
        val suggestCCList1 = modelContext1.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(2, suggestCCList1.size());
        val suggestedCC10 = suggestCCList1.get(0);
        Assert.assertEquals(suggestedCC.getColumnName(), suggestedCC10.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC10.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC10.getExpression());
        val suggestedCC11 = suggestCCList1.get(1);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC11.getTableIdentity());
        Assert.assertEquals(PRICE_PLUS_ITEM_COUNT, suggestedCC11.getExpression());

        // case 2: can use existing cc for the same cc expression
        String query3 = "select sum(price*item_count) from test_kylin_fact";
        String query4 = "select sum(price+item_count), lstg_format_name from test_kylin_fact group by lstg_format_name";
        val context2 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query3, query4 },
                true);
        val modelContextsList2 = context2.getModelContexts();
        val modelContext2 = modelContextsList2.get(0);
        Assert.assertNotNull(modelContext2.getOriginModel());
        val suggestCCList2 = modelContext2.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(2, suggestCCList2.size());
        Assert.assertEquals(suggestCCList2, suggestCCList1);
    }

    @Test
    public void testReproposeNewModelWithSameRootFactTable() {
        // init a model with cc
        String query1 = "select sum(price*item_count), price from test_kylin_fact group by price";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1 }, true);
        val modelContext = context.getModelContexts().get(0);
        val computedCCList = modelContext.getTargetModel().getComputedColumnDescs();
        val suggestedCC = computedCCList.get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC.getExpression());

        // case 1: same cc expression on the same root fact table
        String query2 = "select sum(price*item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        String query3 = "select sum(price+item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        val context1 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query2, query3 });
        AccelerationUtil.onlineModel(context1);
        val modelContext1 = context1.getModelContexts().get(0);
        Assert.assertNull(modelContext1.getOriginModel());
        val suggestedCCList1 = modelContext1.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList1.size());
        val suggestedCC10 = suggestedCCList1.get(0);
        Assert.assertEquals(suggestedCC.getColumnName(), suggestedCC10.getColumnName()); // share
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC10.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC10.getExpression());

        // case 2: different cc expression on the same root fact table
        val modelContext2 = context1.getModelContexts().get(1);
        Assert.assertNull(modelContext2.getOriginModel());
        val suggestedCCList2 = modelContext2.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList2.size());
        val suggestedCC20 = suggestedCCList2.get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC20.getTableIdentity());
        Assert.assertEquals(PRICE_PLUS_ITEM_COUNT, suggestedCC20.getExpression());
    }

    @Test
    public void testReproposeNewModelWithDifferentFactTable() {
        // init a model with cc
        String query1 = "select sum(price*item_count), price from test_kylin_fact group by price";
        val context1 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1 }, true);
        val modelContext = context1.getModelContexts().get(0);
        val computedCCList = modelContext.getTargetModel().getComputedColumnDescs();
        val suggestedCC = computedCCList.get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC.getExpression());

        // case 3: same cc expression on different root fact table
        String query4 = "select sum(price*item_count), account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                + "group by account_id";
        String query5 = "select sum(price+item_count) from test_order left join test_kylin_fact "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        val context3 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query4, query5 },
                true);
        val modelContext3 = context3.getModelContexts().get(0);
        Assert.assertNull(modelContext3.getOriginModel());
        val suggestedCCList3 = modelContext3.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList3.size());
        val suggestedCC30 = suggestedCCList3.get(0);
        Assert.assertNotEquals(suggestedCC.getColumnName(), suggestedCC30.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", suggestedCC30.getTableIdentity());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, suggestedCC30.getExpression());

        // case 4: different cc expression on the different root fact table
        val modelContext4 = context3.getModelContexts().get(1);
        Assert.assertNull(modelContext4.getOriginModel());
        val suggestedCCList4 = modelContext4.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList4.size());
        val suggestedCC40 = suggestedCCList4.get(0);
        Assert.assertEquals("DEFAULT.TEST_ORDER", suggestedCC40.getTableIdentity());
        Assert.assertEquals(PRICE_PLUS_ITEM_COUNT, suggestedCC40.getExpression());
    }

    @Test
    public void testComputedColumnNested() throws SqlParseException {
        String query = "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc1 = model.getComputedColumnDescs().get(0);
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, cc1.getExpression());
        {
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query);
            OlapContext olapContext = olapContexts.get(0);
            OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
            Assert.assertEquals(1, olapContext.getAggregations().size());
            for (FunctionDesc aggregation : olapContext.getAggregations()) {
                String collect = aggregation.getParameters().stream()
                        .map(param -> param.getColRef().getColumnDesc().getIdentity()).collect(Collectors.joining(","));
                Assert.assertTrue(collect.startsWith("TEST_KYLIN_FACT.CC_AUTO_"));
            }
        }

        // nested cc will reuse existing cc
        query = "SELECT SUM((PRICE * ITEM_COUNT) + 10), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context1 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        model = context1.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(2, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc2 = model.getComputedColumnDescs().get(1);
        Assert.assertEquals("(`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`) + 10",
                cc2.getInnerExpression());
        {
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query);
            OlapContext olapContext = olapContexts.get(0);
            OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
            Assert.assertEquals(1, olapContext.getAggregations().size());
            for (FunctionDesc aggregation : olapContext.getAggregations()) {
                String collect = aggregation.getParameters().stream()
                        .map(param -> param.getColRef().getColumnDesc().getIdentity()).collect(Collectors.joining(","));
                Assert.assertTrue(collect.startsWith("TEST_KYLIN_FACT.CC_AUTO_"));
            }
        }
    }

    @Test
    public void testComputedColumnUnnested() throws SqlParseException {
        String query = "SELECT SUM(PRICE * ITEM_COUNT), AVG((PRICE * ITEM_COUNT) + 10), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, ccList.get(0).getExpression());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT_PLUS_TEN, ccList.get(1).getExpression());

        {
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query);
            OlapContext olapContext = olapContexts.get(0);
            OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
            Assert.assertEquals(3, olapContext.getAggregations().size());
            for (FunctionDesc aggregation : olapContext.getAggregations()) {
                String collect = aggregation.getParameters().stream()
                        .map(param -> param.getColRef().getColumnDesc().getIdentity()).collect(Collectors.joining(","));
                Assert.assertTrue(collect.startsWith("TEST_KYLIN_FACT.CC_AUTO_"));
            }
        }
    }

    @Test
    public void testComputedColumnPassOnSumExpr() throws SqlParseException {
        String query = "SELECT SUM(PRICE_TOTAL), CAL_DT FROM (SELECT PRICE * ITEM_COUNT AS PRICE_TOTAL, CAL_DT FROM TEST_KYLIN_FACT) T GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, cc.getExpression());

        {
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query, true, true);
            OlapContext olapContext = olapContexts.get(0);
            OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
            Assert.assertEquals(1, olapContext.getAggregations().size());
            for (FunctionDesc aggregation : olapContext.getAggregations()) {
                String collect = aggregation.getParameters().stream()
                        .map(param -> param.getColRef().getColumnDesc().getIdentity()).collect(Collectors.joining(","));
                Assert.assertTrue(collect.startsWith("TEST_KYLIN_FACT.CC_AUTO_"));
            }
        }
    }

    @Test
    public void testComputedColumnFailOnSumExpr() {
        String query = "SELECT SUM(PRICE_TOTAL + 1), CAL_DT FROM (SELECT PRICE * ITEM_COUNT AS PRICE_TOTAL, CAL_DT FROM TEST_KYLIN_FACT) T GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT_PLUS_ONE, computedColumnDesc.getExpression());
    }

    @Test
    public void testComputedColumnFailOnRexOpt() throws SqlParseException {
        String query = "SELECT SUM(CASE WHEN 9 > 10 THEN 100 ELSE PRICE + 10 END), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals(PRICE_COLUMN + " + 10", cc.getExpression());

        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts(getProject(), query, false);
        OlapContext olapContext = olapContexts.get(0);
        OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
        List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
        Assert.assertEquals(1, aggList.size());
        Assert.assertEquals("[" + cc.getIdentityCcName() + "]", aggList.get(0).getParameters().toString());
    }

    @Test
    public void testComputedColumnsWontImpactFavoriteQuery() {
        // test all named columns rename
        String query = "SELECT SUM(CASE WHEN PRICE > 100 THEN 100 ELSE PRICE END), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CASE WHEN " + PRICE_COLUMN + " > 100 THEN 100 ELSE " + PRICE_COLUMN + " END",
                cc.getExpression());
        Assert.assertEquals(1, model.getEffectiveDimensions().size());
        Assert.assertEquals("CAL_DT", model.getEffectiveDimensions().get(0).getName());
        model.getEffectiveCols().forEach((integer, tblColRef) -> {
            if (tblColRef.getColumnDesc().isComputedColumn()) {
                Assert.assertEquals(cc.getFullName(), tblColRef.getIdentity());
            }
        });
        Measure measure = model.getEffectiveMeasures().get(100001);
        Assert.assertNotNull(measure);
        Assert.assertTrue(measure.getFunction().isSum());
        Assert.assertEquals(cc.getColumnName(), measure.getFunction().getParameters().get(0).getColRef().getName());

        IndexPlan indexPlan = context.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertEquals(1, indexPlan.getAllLayouts().get(0).getId());

        // Assert query info is updated
        AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(query);
        Assert.assertNotNull(accelerateInfo);
        Assert.assertFalse(accelerateInfo.isFailed());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().iterator().next().getLayoutId());
    }

    @Test
    public void testComputedColumnWithLikeClause() {
        String query = "SELECT 100.00 * SUM(CASE WHEN LSTG_FORMAT_NAME LIKE 'VIP%' THEN 100 ELSE 120 END), CAL_DT "
                + "FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query }, true);

        NDataModel model = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc cc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CASE WHEN " + FORMAT_COLUMN_NAME_COLUMN + " LIKE 'VIP%' THEN 100 ELSE 120 END",
                cc.getExpression());
        Assert.assertEquals(1, model.getEffectiveDimensions().size());
        Assert.assertEquals("CAL_DT", model.getEffectiveDimensions().get(0).getName());
        model.getEffectiveCols().forEach((integer, tblColRef) -> {
            if (tblColRef.getColumnDesc().isComputedColumn()) {
                Assert.assertEquals(cc.getFullName(), tblColRef.getIdentity());
            }
        });
        Measure measure = model.getEffectiveMeasures().get(100001);
        Assert.assertNotNull(measure);
        Assert.assertTrue(measure.getFunction().isSum());
        Assert.assertEquals(cc.getColumnName(), measure.getFunction().getParameters().get(0).getColRef().getName());

        IndexPlan indexPlan = context.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertEquals(1, indexPlan.getAllLayouts().get(0).getId());

        // Assert query info is updated
        AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(query);
        Assert.assertNotNull(accelerateInfo);
        Assert.assertFalse(accelerateInfo.isFailed());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().iterator().next().getLayoutId());
    }

    @Test
    public void testInferTypesOfCC() {
        String[] sqls = new String[] {
                "select {fn left(lstg_format_name,2)} as name, sum(price*item_count) from test_kylin_fact group by lstg_format_name ",
                "select sum({fn convert(price, SQL_BIGINT)}) as big67 from test_kylin_fact",
                "select sum({fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}) from test_kylin_fact group by lstg_format_name",
                "select {fn year(cast('2012-01-01' as date))} from test_kylin_fact",
                "select {fn convert({fn year(cast('2012-01-01' as date))}, varchar)} from test_kylin_fact",
                "select case when substring(lstg_format_name, 1, 4) like '%ABIN%' then item_count - 10 else item_count end as  item_count_new from test_kylin_fact" //
        };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[1]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[2]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[3]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[4]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[5]).isNotSucceed());

        computedColumns.sort(Comparator.comparing(ComputedColumnDesc::getInnerExpression));

        Assert.assertEquals(3, computedColumns.size());
        Assert.assertEquals("DOUBLE", computedColumns.get(0).getDatatype());
        Assert.assertEquals("CAST(CHAR_LENGTH(SUBSTRING(`TEST_KYLIN_FACT`.`LSTG_FORMAT_NAME`, 1, 4)) AS DOUBLE)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("BIGINT", computedColumns.get(1).getDatatype());
        Assert.assertEquals("CAST(`TEST_KYLIN_FACT`.`PRICE` AS BIGINT)",
                computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals("DECIMAL(30,4)", computedColumns.get(2).getDatatype());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                computedColumns.get(2).getInnerExpression().trim());
    }

    @Test
    public void testInferTypesOfCcWithUnsupportedFunctions() {
        overwriteSystemProp("kylin.query.transformers", "org.apache.kylin.query.util.ConvertToComputedColumn");

        String[] sqls = new String[] { "select count(trim(lstg_format_name)) from test_kylin_fact",
                "select sum(cast(item_count as bigint) * price) from test_kylin_fact" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CAST(" + ITEM_COUNT_COLUMN + " AS BIGINT) * " + PRICE_COLUMN,
                computedColumns.get(0).getExpression());
        Assert.assertEquals("CAST(`TEST_KYLIN_FACT`.`ITEM_COUNT` AS BIGINT) * `TEST_KYLIN_FACT`.`PRICE`",
                computedColumns.get(0).getInnerExpression());
        Assert.assertEquals("DECIMAL(38,4)", computedColumns.get(0).getDatatype());

        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(accelerationInfoMap.get(sqls[0]).isPending());
        Assert.assertFalse(accelerationInfoMap.get(sqls[1]).isNotSucceed());
    }

    @Test
    public void testRemoveUnsupportedCC() {
        String[] sqls = new String[] {
                "select {fn left(lstg_format_name,2)} as name, sum(price*item_count) from test_kylin_fact group by lstg_format_name ",
                "select sum({fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}) from test_kylin_fact group by lstg_format_name" };

        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
        val modelContexts = context.getModelContexts();
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("CAST(CHAR_LENGTH(SUBSTRING(`TEST_KYLIN_FACT`.`LSTG_FORMAT_NAME`, 1, 4)) AS DOUBLE)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("CAST(CHAR_LENGTH(SUBSTRING(" + FORMAT_COLUMN_NAME_COLUMN + " FROM 1 FOR 4)) AS DOUBLE)",
                computedColumns.get(0).getExpression().trim());
        Assert.assertEquals("DOUBLE", computedColumns.get(0).getDatatype());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals(PRICE_MULTIPLY_ITEM_COUNT, computedColumns.get(1).getExpression().trim());
        Assert.assertEquals("DECIMAL(30,4)", computedColumns.get(1).getDatatype());

        // set one CC to unsupported
        computedColumns.get(0)
                .setInnerExpression("CAST(LENGTH(SUBSTRING(TEST_KYLIN_FACT.LSTG_FORMAT_NAME FROM 1 FOR 4)) AS DOUBLE)");
        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(targetModel, computedColumns);
        Assert.assertEquals(2, targetModel.getComputedColumnDescs().size());
    }

    @Test
    public void testCCOnInnerCol() {
        String[] sqls = new String[] { "select max(cast(LEAF_CATEG_ID*SITE_ID as VARCHAR)) "
                + "from TEST_CATEGORY_GROUPINGS group by META_CATEG_NAME" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals(
                "CAST(`TEST_CATEGORY_GROUPINGS`.`LEAF_CATEG_ID` * `TEST_CATEGORY_GROUPINGS`.`SITE_ID` AS VARCHAR)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("VARCHAR", computedColumns.get(0).getDatatype());
    }

    @Test
    public void testCaseWhenWithMoreThanTwoLogicalOperands() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "LENIENT");
        String[] sqls = { "select case when coalesce(item_count, 0) <=10 and coalesce(price, 0) >= 0.0 then 'a'\n"
                + "            when coalesce(item_count, 0) < 0 then 'exception' else null end,\n"
                + "  sum(case when price > 1 and item_count < 10 and seller_id > 20 then 1 else 0 end),\n"
                + "  sum(case when price > 1 and item_count < 5 or seller_id > 10 then price else 0 end),\n"
                + "  sum(case when price + item_count + 1 > 5 then 1 else 0 end)\n"
                + "from test_kylin_fact group by 1" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        val targetModel1 = modelContexts.get(0).getTargetModel();
        val ccList1 = targetModel1.getComputedColumnDescs();
        ccList1.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        Assert.assertEquals(4, ccList1.size());
        val cc10 = ccList1.get(3);
        Assert.assertEquals(
                "CASE WHEN CASE WHEN \"TEST_KYLIN_FACT\".\"ITEM_COUNT\" IS NOT NULL THEN CAST(\"TEST_KYLIN_FACT\".\"ITEM_COUNT\" AS INTEGER) <= 10 ELSE TRUE END AND CASE WHEN \"TEST_KYLIN_FACT\".\"PRICE\" IS NOT NULL THEN CAST(\"TEST_KYLIN_FACT\".\"PRICE\" AS DECIMAL(19, 4)) >= 0.0 ELSE TRUE END THEN 'a' WHEN CASE WHEN \"TEST_KYLIN_FACT\".\"ITEM_COUNT\" IS NOT NULL THEN CAST(\"TEST_KYLIN_FACT\".\"ITEM_COUNT\" AS INTEGER) < 0 ELSE FALSE END THEN 'exception' ELSE NULL END",
                cc10.getExpression());
        Assert.assertEquals(
                "CASE WHEN CASE WHEN `TEST_KYLIN_FACT`.`ITEM_COUNT` IS NOT NULL THEN CAST(`TEST_KYLIN_FACT`.`ITEM_COUNT` AS INT) <= 10 ELSE TRUE END AND CASE WHEN `TEST_KYLIN_FACT`.`PRICE` IS NOT NULL THEN CAST(`TEST_KYLIN_FACT`.`PRICE` AS DECIMAL(19, 4)) >= 0.0 ELSE TRUE END THEN 'a' WHEN CASE WHEN `TEST_KYLIN_FACT`.`ITEM_COUNT` IS NOT NULL THEN CAST(`TEST_KYLIN_FACT`.`ITEM_COUNT` AS INT) < 0 ELSE FALSE END THEN 'exception' ELSE NULL END",
                cc10.getInnerExpression());
        Assert.assertEquals("VARCHAR", cc10.getDatatype());
        val cc11 = ccList1.get(0);
        Assert.assertEquals("CASE WHEN " + PRICE_PLUS_ITEM_COUNT + " + 1 > 5 THEN 1 ELSE 0 END", cc11.getExpression());
        Assert.assertEquals(
                "CASE WHEN `TEST_KYLIN_FACT`.`PRICE` + `TEST_KYLIN_FACT`.`ITEM_COUNT` + 1 > 5 THEN 1 ELSE 0 END",
                cc11.getInnerExpression());
        Assert.assertEquals("INTEGER", cc11.getDatatype());
        val cc12 = ccList1.get(1);
        Assert.assertEquals("CASE WHEN " + PRICE_COLUMN + " > 1 AND " + ITEM_COUNT_COLUMN + " < 10 " + "AND "
                + SELLER_ID_COLUMN + " > 20 THEN 1 ELSE 0 END", cc12.getExpression());
        Assert.assertEquals("CASE WHEN `TEST_KYLIN_FACT`.`PRICE` > 1 AND `TEST_KYLIN_FACT`.`ITEM_COUNT` < 10 "
                + "AND `TEST_KYLIN_FACT`.`SELLER_ID` > 20 THEN 1 ELSE 0 END", cc12.getInnerExpression());
        Assert.assertEquals("INTEGER", cc12.getDatatype());
        val cc13 = ccList1.get(2);
        Assert.assertEquals("CASE WHEN " + PRICE_COLUMN + " > 1 AND " + ITEM_COUNT_COLUMN + " < 5 " + "OR "
                + SELLER_ID_COLUMN + " > 10 THEN " + PRICE_COLUMN + " ELSE 0 END", cc13.getExpression());
        Assert.assertEquals(
                "CASE WHEN `TEST_KYLIN_FACT`.`PRICE` > 1 AND `TEST_KYLIN_FACT`.`ITEM_COUNT` < 5 "
                        + "OR `TEST_KYLIN_FACT`.`SELLER_ID` > 10 THEN `TEST_KYLIN_FACT`.`PRICE` ELSE 0 END",
                cc13.getInnerExpression());
        Assert.assertEquals("DECIMAL(19,4)", cc13.getDatatype());
    }

    @Test
    public void testCCOnInnerGroupCol() {
        String[] sqls = new String[] { "select is_screen_on, count(1) as num from\n" //
                + "(\n" //
                + "select trans_id,\n" //
                + "  case when TEST_ACCOUNT.ACCOUNT_ID >= 10000336 then 1\n" //
                + "    else 2\n" //
                + "    end as is_screen_on\n" //
                + "from TEST_KYLIN_FACT\n" //
                + "inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n" //
                + ")\n" //
                + "group by is_screen_on" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CASE WHEN `TEST_ACCOUNT`.`ACCOUNT_ID` >= 10000336 THEN 1 ELSE 2 END",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("INTEGER", computedColumns.get(0).getDatatype());
    }

    @Test
    public void testCCOnInnerFilterCol() {
        String[] sqls = new String[] { "select count(1) as num from\n" //
                + "(\n" //
                + "select trans_id, cal_dt\n" //
                + "from TEST_KYLIN_FACT\n" //
                + "inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n" //
                + "where\n" //
                + "  TEST_ACCOUNT.ACCOUNT_ID + TEST_KYLIN_FACT.ITEM_COUNT >= 10000336\n" //
                + "  and TEST_KYLIN_FACT.ITEM_COUNT > 100\n" //
                + "  or (\n" //
                + "    TEST_KYLIN_FACT.ITEM_COUNT * 100 <> 100000\n" //
                + "    and LSTG_FORMAT_NAME in ('FP-GTC', 'ABIN')\n" //
                + "  )\n" //
                + ")\n" //
                + "group by cal_dt" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("`TEST_ACCOUNT`.`ACCOUNT_ID` + `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * 100", computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals("BIGINT", computedColumns.get(0).getDatatype());
        Assert.assertEquals("INTEGER", computedColumns.get(1).getDatatype());
    }

    @Test
    public void testDisableCCOnInnerFilterCol() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "FALSE");
        String[] sqls = new String[] { "select count(1) as num from\n" //
                + "(\n" //
                + "select trans_id, cal_dt\n" //
                + "from TEST_KYLIN_FACT\n" //
                + "inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n" //
                + "where\n" //
                + "  TEST_ACCOUNT.ACCOUNT_ID + TEST_KYLIN_FACT.ITEM_COUNT >= 10000336\n" //
                + "  and TEST_KYLIN_FACT.ITEM_COUNT > 100\n" //
                + "  or (\n" //
                + "    TEST_KYLIN_FACT.ITEM_COUNT * 100 <> 100000\n" //
                + "    and LSTG_FORMAT_NAME in ('FP-GTC', 'ABIN')\n" //
                + "  )\n" //
                + ")\n" //
                + "group by cal_dt" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertEquals(0, computedColumns.size());
    }

    @Test
    public void testCCOnInnerFilterColGreaterThanMinCardinality() {
        String[] sqls = new String[] { "select price, item_count, count(1)\n" //
                + "from test_kylin_fact\n" //
                + "where TRANS_ID + ORDER_ID > 100\n" //
                + "group by price, item_count" };

        mockTableExtDesc("DEFAULT.TEST_KYLIN_FACT", "newten", new String[] { "TRANS_ID", "ORDER_ID" },
                new int[] { 99, 77 });
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.minimum-cardinality", "5000");
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`TRANS_ID` + `TEST_KYLIN_FACT`.`ORDER_ID`",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("BIGINT", computedColumns.get(0).getDatatype());
    }

    @Test
    public void testCCOnInnerFilterColLessThanMinCardinality() {
        String[] sqls = new String[] { "select price, item_count, count(1)\n" //
                + "from test_kylin_fact\n" //
                + "where TRANS_ID + ORDER_ID > 100\n" //
                + "group by price, item_count" };
        mockTableExtDesc("DEFAULT.TEST_KYLIN_FACT", "newten", new String[] { "TRANS_ID", "ORDER_ID" },
                new int[] { 99, 77 });
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.minimum-cardinality", "10000");
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertEquals(0, computedColumns.size());
    }

    @Test
    public void testCCWithKeywordColumn() {
        String[] sqls = new String[] { "select id from keyword.test_keyword_column group by id" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, "keyword", sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();

        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        cc1.setTableIdentity("KEYWORD.TEST_KEYWORD_COLUMN");
        cc1.setExpression("year(TEST_KEYWORD_COLUMN.\"DATE\")");
        cc1.setInnerExpression("year(`TEST_KEYWORD_COLUMN`.`DATE`)");
        cc1.setColumnName("CC_1");

        ComputedColumnDesc cc2 = new ComputedColumnDesc();
        cc2.setTableIdentity("KEYWORD.TEST_KEYWORD_COLUMN");
        cc2.setExpression("substring(TEST_KEYWORD_COLUMN.\"STRING\", 1, 2)");
        cc2.setInnerExpression("substring(`TEST_KEYWORD_COLUMN`.`STRING`, 1, 2)");
        cc2.setColumnName("CC_1");

        List<ComputedColumnDesc> computedColumnDescs = Lists.newArrayList(cc1, cc2);

        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(targetModel, computedColumnDescs);
        Assert.assertEquals("INTEGER", cc1.getDatatype());
        Assert.assertEquals("VARCHAR", cc2.getDatatype());
    }

    @Test
    public void testCCOnInnerGroupColGreaterThanMinCardinality() {
        String[] sqls = new String[] { "select sum(price), sum(item_count)\n" //
                + "from (\n" //
                + "select TRANS_ID + ORDER_ID as NEW_ID, price, item_count\n" //
                + "from test_kylin_fact\n" //
                + ")\n" //
                + "group by NEW_ID" };

        mockTableExtDesc("DEFAULT.TEST_KYLIN_FACT", "newten", new String[] { "TRANS_ID", "ORDER_ID" },
                new int[] { 99, 77 });
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.group-key.minimum-cardinality", "5000");
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`TRANS_ID` + `TEST_KYLIN_FACT`.`ORDER_ID`",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("BIGINT", computedColumns.get(0).getDatatype());
    }

    @Test
    public void testCCOnInnerGroupColLessThanMinCardinality() {
        String[] sqls = new String[] { "select sum(price), sum(item_count)\n" //
                + "from (\n" //
                + "select TRANS_ID + ORDER_ID as NEW_ID, price, item_count\n" //
                + "from test_kylin_fact\n" //
                + ")\n" //
                + "group by NEW_ID" };
        mockTableExtDesc("DEFAULT.TEST_KYLIN_FACT", "newten", new String[] { "TRANS_ID", "ORDER_ID" },
                new int[] { 99, 77 });
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.group-key.minimum-cardinality", "10000");
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);

        val modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = context.getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertEquals(0, computedColumns.size());
    }

    @Test
    public void testNestedUdfRecommendCC() {
        String[] sqls = new String[] {
                "SELECT COUNT(SPLIT_PART(CONCAT(substr(lstg_format_name,1), '-apache-kylin'), '-', 1)) FROM test_kylin_fact",
                "SELECT COUNT(SPLIT_PART(upper(substr(lstg_format_name,1)), 'A', 1)) FROM test_kylin_fact",
                "SELECT sum(length(concat(cast(instr(cast(SELLER_ID as varchar),'0') as varchar),'ll'))) from test_kylin_fact\n"
                        + "where instr(cast(SELLER_ID as varchar),'0') > 1 group by LSTG_FORMAT_NAME" };

        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
        val modelContexts = context.getModelContexts();
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        computedColumns.sort((cc1, cc2) -> StringUtils.compare(cc1.getInnerExpression(), cc2.getInnerExpression()));
        Assert.assertEquals(4, computedColumns.size());
        Assert.assertEquals("INSTR(CAST(`TEST_KYLIN_FACT`.`SELLER_ID` AS VARCHAR), '0')",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("INSTR(CAST(" + SELLER_ID_COLUMN + " AS VARCHAR), '0')",
                computedColumns.get(0).getExpression().trim());
        Assert.assertEquals("INTEGER", computedColumns.get(0).getDatatype());
        Assert.assertEquals(
                "LENGTH(CONCAT(CAST(INSTR(CAST(`TEST_KYLIN_FACT`.`SELLER_ID` AS VARCHAR), '0') AS VARCHAR), 'll'))",
                computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals(
                "LENGTH(CONCAT(CAST(INSTR(CAST(" + SELLER_ID_COLUMN + " AS VARCHAR), '0') AS VARCHAR), 'll'))",
                computedColumns.get(1).getExpression().trim());
        Assert.assertEquals("INTEGER", computedColumns.get(1).getDatatype());
        Assert.assertEquals(
                "SPLIT_PART(CONCAT(SUBSTRING(`TEST_KYLIN_FACT`.`LSTG_FORMAT_NAME`, 1), '-apache-kylin'), '-', 1)",
                computedColumns.get(2).getInnerExpression().trim());
        Assert.assertEquals(
                "SPLIT_PART(CONCAT(SUBSTRING(" + FORMAT_COLUMN_NAME_COLUMN + " FROM 1), '-apache-kylin'), '-', 1)",
                computedColumns.get(2).getExpression().trim());
        Assert.assertEquals("VARCHAR", computedColumns.get(2).getDatatype());
        Assert.assertEquals("SPLIT_PART(UPPER(SUBSTRING(`TEST_KYLIN_FACT`.`LSTG_FORMAT_NAME`, 1)), 'A', 1)",
                computedColumns.get(3).getInnerExpression().trim());
        Assert.assertEquals("SPLIT_PART(UPPER(SUBSTRING(" + FORMAT_COLUMN_NAME_COLUMN + " FROM 1)), 'A', 1)",
                computedColumns.get(3).getExpression().trim());
        Assert.assertEquals("VARCHAR", computedColumns.get(3).getDatatype());
    }

    /**
     * <a href="https://github.com/kyligence/kap/issues/16810">Bug fix</a>
     */
    @Test
    public void testCCContainTypeTransform() {
        String[] sqlArray1 = { "SELECT sum(case when ITEM_COUNT > ' ' then 1 else 0 end) FROM TEST_KYLIN_FACT",
                "SELECT sum(case when ITEM_COUNT > 5  then 1 else 0 end) FROM TEST_KYLIN_FACT",
                "SELECT sum(case when CAL_DT > ' ' then 1 else 0 end) FROM TEST_KYLIN_FACT" };
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqlArray1, true);

        String[] sqlArray2 = { "SELECT sum(case when ITEM_COUNT > ' ' then 1 else 0 end),1 FROM TEST_KYLIN_FACT",
                "SELECT sum(case when ITEM_COUNT > 5  then 1 else 0 end),1 FROM TEST_KYLIN_FACT",
                "SELECT sum(case when CAL_DT > ' ' then 1 else 0 end),1 FROM TEST_KYLIN_FACT" };
        val context2 = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqlArray2, true);
        for (AccelerateInfo accelerateInfo : context2.getAccelerateInfoMap().values()) {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        }
    }

    @Test
    public void testProposeSparkUDFCC() {
        String sql = "select count(CBRT(test_kylin_fact.price)) from test_kylin_fact";

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql }, true);
        Assert.assertEquals(1, context.getModelContexts().size());
        val targetModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, targetModel.getComputedColumnDescs().size());
        val cc = targetModel.getComputedColumnDescs().get(0);
        Assert.assertEquals("CBRT(" + PRICE_COLUMN + ")", cc.getExpression());
    }

    @Test
    public void testNoColIdRepeat() {
        String sql = "select count((test_kylin_fact.price+'')) from test_kylin_fact";
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql }, true);
        Assert.assertEquals(1, context.getModelContexts().size());
        val targetModel = context.getModelContexts().get(0).getTargetModel();

        UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel(targetModel.getId(), model -> {
                    model.getAllNamedColumns().get(model.getAllNamedColumns().size() - 1) //
                            .setStatus(NDataModel.ColumnStatus.TOMB);
                }), getProject());

        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelManager.getDataModelDesc(targetModel.getId());
        val namedColumn = model.getAllNamedColumns().get(model.getAllNamedColumns().size() - 1);
        Assert.assertFalse(namedColumn.isExist());
        val tombId = namedColumn.getId();
        val newTargetModel = context.getModelContexts().get(0).getTargetModel();
        val newNamedColumn = newTargetModel.getAllNamedColumns().get(newTargetModel.getAllNamedColumns().size() - 1);
        Assert.assertEquals(tombId, newNamedColumn.getId());
    }

    private void mockTableExtDesc(String tableIdentity, String proj, String[] colNames, int[] cardinalityList) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), proj);
            TableDesc tableDesc = tableMgr.getTableDesc(tableIdentity);
            TableExtDesc oldExtDesc = tableMgr.getOrCreateTableExt(tableDesc);

            // mock table ext desc
            List<TableExtDesc.ColumnStats> columnStatsList = new LinkedList<>();
            TableExtDesc tableExt = new TableExtDesc(oldExtDesc);
            tableExt.setIdentity(tableIdentity);
            for (int i = 0; i < colNames.length; i++) {
                TableExtDesc.ColumnStats col = new TableExtDesc.ColumnStats();
                col.setCardinality(cardinalityList[i]);
                col.setTableExtDesc(tableExt);
                col.setColumnName(colNames[i]);
                columnStatsList.add(col);
            }
            tableExt.setColumnStats(columnStatsList);
            tableMgr.mergeAndUpdateTableExt(oldExtDesc, tableExt);
            return true;
        }, proj);

        // verify the column stats update successfully
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), proj);
        TableDesc tableDesc = tableMgr.getTableDesc(tableIdentity);
        final TableExtDesc newTableExt = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertEquals(colNames.length, newTableExt.getAllColumnStats().size());
    }
}
