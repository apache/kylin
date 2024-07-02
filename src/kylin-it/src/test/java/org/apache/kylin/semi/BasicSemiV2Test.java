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

package org.apache.kylin.semi;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.collect.ArrayListMultimap;
import org.apache.kylin.guava30.shaded.common.collect.ListMultimap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelReuseContext;
import org.apache.kylin.rec.ProposerJob;
import org.apache.kylin.rec.SmartMaster;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ModelChangeSupporter;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectSmartService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SemiAutoTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicSemiV2Test extends SemiAutoTestBase {
    private static final String PRICE_COLUMN = "\"TEST_KYLIN_FACT\".\"PRICE\"";
    private static final String CC_ITEM_COUNT_MULTIPLY_PRICE = "\"TEST_KYLIN_FACT\".\"ITEM_COUNT\" * \"TEST_KYLIN_FACT\".\"PRICE\"";
    private static final String CC_PRICE_MULTIPLY_ITEM_COUNT = "\"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\"";
    private static final String CC_ORDER_ID_PLUS_TRANS_ID = "\"TEST_KYLIN_FACT\".\"ORDER_ID\" + \"TEST_KYLIN_FACT\".\"TRANS_ID\"";

    private static final long QUERY_TIME = 1595520000000L;

    @InjectMocks
    private final OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    private final RawRecService rawRecService = new RawRecService();
    @InjectMocks
    private final ProjectSmartService projectSmartService = new ProjectSmartService();
    @Spy
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @Spy
    private final List<ModelChangeSupporter> modelChangeSupporters = Mockito
            .spy(Collections.singletonList(rawRecService));

    private JdbcRawRecStore jdbcRawRecStore;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    private TimeZone defaultTimeZone;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        defaultTimeZone = TimeZone.getDefault();
        TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
        TimeZone.setDefault(timeZone);
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        jdbcRawRecStore.deleteAll();
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
    }

    private void prepareACL() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        TimeZone.setDefault(defaultTimeZone);
    }

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testNormalizeForTableDetecting() {
        String sql = "SELECT price * item_count FROM test_kylin_fact FETCH NEXT 1000 ROWS ONLY";
        String normalizedSql = AbstractContext.normalizeForTableDetecting(getProject(), sql);
        Assert.assertEquals(sql, normalizedSql);
    }

    @Test
    public void testBasic() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" }, true);

        MetadataTestUtils.toSemiAutoMode(getProject());

        // test
        String[] sqls = { "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                + "from test_kylin_fact group by lstg_format_name, price *  5" };

        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls);
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        AbstractContext.ModelContext modelContext = modelContexts.get(0);
        val ccRecItemMap = modelContext.getCcRecItemMap();
        val dimensionRecItemMap = modelContext.getDimensionRecItemMap();
        val measureRecItemMap = modelContext.getMeasureRecItemMap();
        val indexRexItemMap = modelContext.getIndexRexItemMap();

        Assert.assertEquals(2, ccRecItemMap.size());
        Assert.assertEquals(2, dimensionRecItemMap.size());
        Assert.assertEquals(2, measureRecItemMap.size());
        Assert.assertEquals(1, indexRexItemMap.size());

        String key = "SUM__TEST_KYLIN_FACT$PRICE";
        measureRecItemMap.forEach((k, item) -> {
            if (item.getUniqueContent().equalsIgnoreCase(key)) {
                Assert.assertEquals("SUM_TEST_KYLIN_FACT_PRICE", item.getMeasure().getName());
            } else {
                String uniqueContent = item.getUniqueContent();
                String ccUUID = uniqueContent.split("__")[1];
                Assert.assertTrue(ccRecItemMap.containsKey(ccUUID));
                CCRecItemV2 ccRecItemV2 = ccRecItemMap.get(ccUUID);
                Assert.assertEquals(CC_ITEM_COUNT_MULTIPLY_PRICE, ccRecItemV2.getCc().getExpression());
            }
        });
    }

    @Test
    public void testNotGenerateSuggestionsWhenExistIndex() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        String query1 = "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by lstg_format_name, price *  5";
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1 }, true);
        MetadataTestUtils.toSemiAutoMode(getProject());

        // collect recommendation
        String[] sqls1 = { "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by lstg_format_name, price *  5" };
        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls1);
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(0, modelContext.getCcRecItemMap().size());
        Assert.assertEquals(0, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(0, modelContext.getMeasureRecItemMap().size());
        Assert.assertEquals(0, modelContext.getIndexRexItemMap().size());
    }

    @Test
    public void testCollectRecItemWithMultipleModels() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare 3 initial model
        String query1 = "select price from test_kylin_fact left join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id";
        String query2 = "select price from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "left join test_country on test_account.account_country = test_country.country";
        String query3 = "select price from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1, query2, query3 }, true);
        MetadataTestUtils.toSemiAutoMode(getProject());

        // collect recommendation
        String[] sqls = {
                "select sum(item_count*price), sum(price), lstg_format_name, price * 5 "
                        + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id "
                        + "group by lstg_format_name, price *  5",
                "select count(item_count*price), sum(price * 5) "
                        + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id ",
                "select max(item_count*price),min(price * 5) from test_kylin_fact inner join test_account "
                        + "on test_kylin_fact.seller_id = test_account.account_id "
                        + "left join test_country on test_account.account_country = test_country.country" };
        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls);
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();

        // validate
        collectCCRecItemWithMultipleModels(modelContexts);
        collectDimensionRecItemWithMultipleModels(modelContexts);
        collectMeasureRecItemWithMultipleModels(modelContexts);
        collectIndexRecItemWithMultipleModels(modelContexts);
    }

    private void collectCCRecItemWithMultipleModels(List<AbstractContext.ModelContext> modelContexts) {
        Map<String, CCRecItemV2> ccRecItemMap1 = modelContexts.get(0).getCcRecItemMap();
        Assert.assertEquals(2, ccRecItemMap1.size());
        ArrayList<CCRecItemV2> ccRecItemList1 = new ArrayList<>(ccRecItemMap1.values());
        ccRecItemList1.sort(Comparator.comparing(o -> o.getCc().getExpression()));
        Assert.assertEquals(PRICE_COLUMN + " * 5", ccRecItemList1.get(1).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList1.get(1).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", ccRecItemList1.get(1).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(21,4)", ccRecItemList1.get(1).getCc().getDatatype());
        Assert.assertEquals(CC_ITEM_COUNT_MULTIPLY_PRICE, ccRecItemList1.get(0).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList1.get(0).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`",
                ccRecItemList1.get(0).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(30,4)", ccRecItemList1.get(0).getCc().getDatatype());

        Map<String, CCRecItemV2> ccRecItemMap2 = modelContexts.get(1).getCcRecItemMap();
        Assert.assertEquals(2, ccRecItemMap2.size());
        ArrayList<CCRecItemV2> ccRecItemList2 = new ArrayList<>(ccRecItemMap2.values());
        ccRecItemList2.sort(Comparator.comparing(o -> o.getCc().getExpression()));
        Assert.assertEquals(PRICE_COLUMN + " * 5", ccRecItemList2.get(1).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList2.get(1).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", ccRecItemList2.get(1).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(21,4)", ccRecItemList2.get(1).getCc().getDatatype());
        Assert.assertEquals(CC_ITEM_COUNT_MULTIPLY_PRICE, ccRecItemList2.get(0).getCc().getExpression());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", ccRecItemList2.get(0).getCc().getTableIdentity());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`",
                ccRecItemList2.get(0).getCc().getInnerExpression());
        Assert.assertEquals("DECIMAL(30,4)", ccRecItemList2.get(0).getCc().getDatatype());
    }

    private void collectDimensionRecItemWithMultipleModels(List<AbstractContext.ModelContext> modelContexts) {
        Map<String, DimensionRecItemV2> dimensionRecItemMap1 = modelContexts.get(0).getDimensionRecItemMap();
        Assert.assertEquals(2, dimensionRecItemMap1.size());
        ArrayList<DimensionRecItemV2> dimensionRecItemList1 = new ArrayList<>(dimensionRecItemMap1.values());
        dimensionRecItemList1.sort(Comparator.comparingInt(o -> o.getColumn().getId()));
        Assert.assertEquals("varchar(4096)", dimensionRecItemList1.get(0).getDataType());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
                dimensionRecItemList1.get(0).getColumn().getAliasDotColumn());
        Assert.assertEquals(4, dimensionRecItemList1.get(0).getColumn().getId());
        Assert.assertEquals("decimal(21,4)", dimensionRecItemList1.get(1).getDataType());
        Assert.assertEquals(17, dimensionRecItemList1.get(1).getColumn().getId());

        Map<String, DimensionRecItemV2> dimensionRecItemMap2 = modelContexts.get(1).getDimensionRecItemMap();
        Assert.assertEquals(0, dimensionRecItemMap2.size());
    }

    private void collectMeasureRecItemWithMultipleModels(List<AbstractContext.ModelContext> modelContexts) {
        Map<String, MeasureRecItemV2> measureRecItemMap1 = modelContexts.get(0).getMeasureRecItemMap();
        Assert.assertEquals(4, measureRecItemMap1.size());
        ArrayList<MeasureRecItemV2> measureRecItemList = new ArrayList<>(measureRecItemMap1.values());
        measureRecItemList.sort(Comparator.comparingInt(o -> o.getMeasure().getId()));
        Assert.assertEquals(100001, measureRecItemList.get(0).getMeasure().getId());
        Assert.assertEquals("decimal(38,4)", measureRecItemList.get(0).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(0).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`", measureRecItemList.get(0)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100002, measureRecItemList.get(1).getMeasure().getId());
        Assert.assertEquals("decimal(29,4)", measureRecItemList.get(1).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(1).getMeasure().getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE",
                measureRecItemList.get(1).getMeasure().getFunction().getParameters().get(0).getColRef().getIdentity());

        Assert.assertEquals(100003, measureRecItemList.get(2).getMeasure().getId());
        Assert.assertEquals("bigint", measureRecItemList.get(2).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("COUNT", measureRecItemList.get(2).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`", measureRecItemList.get(2)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100004, measureRecItemList.get(3).getMeasure().getId());
        Assert.assertEquals("decimal(31,4)", measureRecItemList.get(3).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(3).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", measureRecItemList.get(3).getMeasure().getFunction()
                .getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Map<String, MeasureRecItemV2> measureRecItemMap2 = modelContexts.get(1).getMeasureRecItemMap();
        Assert.assertEquals(2, measureRecItemMap2.size());
        ArrayList<MeasureRecItemV2> measureRecItemList2 = new ArrayList<>(measureRecItemMap2.values());
        measureRecItemList2.sort(Comparator.comparingInt(o -> o.getMeasure().getId()));
        Assert.assertEquals(100001, measureRecItemList2.get(0).getMeasure().getId());
        Assert.assertEquals("decimal(30,4)", measureRecItemList2.get(0).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("MAX", measureRecItemList2.get(0).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`ITEM_COUNT` * `TEST_KYLIN_FACT`.`PRICE`", measureRecItemList2.get(0)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100002, measureRecItemList2.get(1).getMeasure().getId());
        Assert.assertEquals("decimal(21,4)", measureRecItemList2.get(1).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("MIN", measureRecItemList2.get(1).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * 5", measureRecItemList2.get(1).getMeasure().getFunction()
                .getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());
    }

    private void collectIndexRecItemWithMultipleModels(List<AbstractContext.ModelContext> modelContexts) {
        Map<String, LayoutRecItemV2> indexRexItemMap1 = modelContexts.get(0).getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMap1.size());
        ArrayList<LayoutRecItemV2> layoutRecItemList1 = new ArrayList<>(indexRexItemMap1.values());
        layoutRecItemList1.sort(Comparator.comparingLong(o -> o.getLayout().getId()));
        Assert.assertEquals("[17, 4, 100000, 100001, 100002]",
                layoutRecItemList1.get(0).getLayout().getColOrder().toString());
        Assert.assertEquals("[100000, 100003, 100004]", layoutRecItemList1.get(1).getLayout().getColOrder().toString());

        Map<String, LayoutRecItemV2> indexRexItemMap2 = modelContexts.get(1).getIndexRexItemMap();
        Assert.assertEquals(1, indexRexItemMap2.size());
        ArrayList<LayoutRecItemV2> layoutRecItemList2 = new ArrayList<>(indexRexItemMap2.values());
        Assert.assertEquals("[100000, 100001, 100002]", layoutRecItemList2.get(0).getLayout().getColOrder().toString());
    }

    @Test
    public void testCollectCCRecItemWhenCCOnLookupTable() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare origin model
        String query1 = "select cal_dt from test_kylin_fact left join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id";
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1 }, true);
        MetadataTestUtils.toSemiAutoMode(getProject());

        String[] sqls = { "select count(test_order.BUYER_ID + 1) from test_kylin_fact left join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id" };
        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls);
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Map<String, CCRecItemV2> ccRecItemMap = modelContexts.get(0).getCcRecItemMap();
        Assert.assertEquals(1, ccRecItemMap.size());
        Assert.assertEquals("\"TEST_ORDER\".\"BUYER_ID\" + 1",
                Lists.newArrayList(ccRecItemMap.values()).get(0).getCc().getExpression());
        Assert.assertEquals("`TEST_ORDER`.`BUYER_ID` + 1",
                Lists.newArrayList(ccRecItemMap.values()).get(0).getCc().getInnerExpression());
        Assert.assertEquals("BIGINT", Lists.newArrayList(ccRecItemMap.values()).get(0).getCc().getDatatype());
    }

    @Test
    public void testCollectMeasureRecItemForMultiColumn() {
        // prepare origin model
        String query1 = "select cal_dt from test_kylin_fact";
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1 }, true);
        MetadataTestUtils.toSemiAutoMode(getProject());

        // generate raw recommendations
        String[] sqls = { "select count(TRANS_ID + ORDER_ID),avg(ORDER_ID) from test_kylin_fact" };
        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, MeasureRecItemV2> measureRecItemMap = context.getModelContexts().get(0).getMeasureRecItemMap();
        Assert.assertEquals(3, measureRecItemMap.size());
        ArrayList<MeasureRecItemV2> measureRecItemList = Lists.newArrayList(measureRecItemMap.values());
        measureRecItemList.sort(Comparator.comparing(o -> o.getMeasure().getName()));

        Assert.assertEquals(100001, measureRecItemList.get(0).getMeasure().getId());
        Assert.assertEquals("bigint", measureRecItemList.get(0).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("COUNT", measureRecItemList.get(0).getMeasure().getFunction().getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`TRANS_ID` + `TEST_KYLIN_FACT`.`ORDER_ID`", measureRecItemList.get(0)
                .getMeasure().getFunction().getParameters().get(0).getColRef().getColumnDesc().getComputedColumnExpr());

        Assert.assertEquals(100003, measureRecItemList.get(1).getMeasure().getId());
        Assert.assertEquals("bigint", measureRecItemList.get(1).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("COUNT", measureRecItemList.get(1).getMeasure().getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID",
                measureRecItemList.get(1).getMeasure().getFunction().getParameters().get(0).getColRef().getIdentity());

        Assert.assertEquals(100002, measureRecItemList.get(2).getMeasure().getId());
        Assert.assertEquals("bigint", measureRecItemList.get(2).getMeasure().getFunction().getReturnType());
        Assert.assertEquals("SUM", measureRecItemList.get(2).getMeasure().getFunction().getExpression());
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID",
                measureRecItemList.get(2).getMeasure().getFunction().getParameters().get(0).getColRef().getIdentity());
    }

    @Test
    public void testCollectCCRecItemWhenSameCCOnDifferentFactTable() {
        // prepare origin model
        String query1 = "select account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id ";
        String query2 = "select item_count from test_order left join test_kylin_fact "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { query1, query2 }, true);
        MetadataTestUtils.toSemiAutoMode(getProject());

        // generate raw recommendations
        String[] sqls = {
                "select sum(price*item_count), account_id from test_account "
                        + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                        + "group by account_id",
                "select sum(price*item_count) from test_order left join test_kylin_fact on test_kylin_fact.order_id = test_order.order_id " };
        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(2, context.getModelContexts().size());
        Assert.assertEquals(1, context.getModelContexts().get(0).getCcRecItemMap().size());
        Assert.assertEquals(1, context.getModelContexts().get(1).getCcRecItemMap().size());
        Assert.assertEquals(CC_PRICE_MULTIPLY_ITEM_COUNT,
                Lists.newArrayList(context.getModelContexts().get(0).getCcRecItemMap().values()).get(0).getCc()
                        .getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                Lists.newArrayList(context.getModelContexts().get(0).getCcRecItemMap().values()).get(0).getCc()
                        .getInnerExpression());
        Assert.assertEquals(CC_PRICE_MULTIPLY_ITEM_COUNT,
                Lists.newArrayList(context.getModelContexts().get(1).getCcRecItemMap().values()).get(0).getCc()
                        .getExpression());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                Lists.newArrayList(context.getModelContexts().get(1).getCcRecItemMap().values()).get(0).getCc()
                        .getInnerExpression());
    }

    @Test
    public void testBatchGenerateAndUpdateLayoutMetric() {
        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" }, true);

        // prepare [a,b,m1,m2,count(*)]  [a,b,m1,count(*)]
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select count(ORDER_ID),SUM(ORDER_ID) from test_kylin_fact group by price,TRANS_ID");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setDuration(100L);
        queryHistory1.setId(1);
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select SUM(ORDER_ID) from test_kylin_fact group by price,TRANS_ID");
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setDuration(200L);
        queryHistory2.setId(2);
        MetadataTestUtils.toSemiAutoMode(getProject());
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1, queryHistory2));

        // validate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort(Comparator.comparingInt(o -> o.getType().id()));
        Assert.assertEquals(4, rawRecItems.size());
        RawRecItem layoutRawRecItem = rawRecItems.get(3);
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRawRecItem.getType());
        Assert.assertEquals(2,
                Lists.newArrayList(layoutRawRecItem.getLayoutMetric().getFrequencyMap().getDateFrequency().values())
                        .get(0).intValue());
        Assert.assertEquals(300, Lists
                .newArrayList(layoutRawRecItem.getLayoutMetric().getLatencyMap().getMap().values()).get(0).longValue());

        // second batch
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));
        rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort(Comparator.comparingInt(o -> o.getType().id()));
        Assert.assertEquals(4, rawRecItems.size());
        layoutRawRecItem = rawRecItems.get(3);
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRawRecItem.getType());
        Assert.assertEquals(3,
                Lists.newArrayList(layoutRawRecItem.getLayoutMetric().getFrequencyMap().getDateFrequency().values())
                        .get(0).intValue());
        Assert.assertEquals(400, Lists
                .newArrayList(layoutRawRecItem.getLayoutMetric().getLatencyMap().getMap().values()).get(0).longValue());
    }

    @Test
    public void testParallelBatchGenerateAndUpdateLayoutMetric() throws InterruptedException {
        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" }, true);

        // prepare [a,b,m1,m2,count(*)]  [a,b,m1,count(*)]
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select count(ORDER_ID),SUM(ORDER_ID) from test_kylin_fact group by price,TRANS_ID");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setDuration(100L);
        queryHistory1.setId(1);
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select SUM(ORDER_ID) from test_kylin_fact group by price,TRANS_ID");
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setDuration(200L);
        queryHistory2.setId(2);
        MetadataTestUtils.toSemiAutoMode(getProject());

        Runnable runnable = () -> rawRecService.generateRawRecommendations(getProject(),
                Lists.newArrayList(queryHistory1, queryHistory2));
        List<Thread> threads = new ArrayList<>();
        threads.add(new Thread(runnable));
        threads.add(new Thread(runnable));
        threads.add(new Thread(runnable));
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }

        // validate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort(Comparator.comparingInt(o -> o.getType().id()));
        Assert.assertEquals(4, rawRecItems.size());
        RawRecItem layoutRawRecItem = rawRecItems.get(3);
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRawRecItem.getType());
        Assert.assertEquals(2 * 3,
                Lists.newArrayList(layoutRawRecItem.getLayoutMetric().getFrequencyMap().getDateFrequency().values())
                        .get(0).intValue());
        Assert.assertEquals(300 * 3, Lists
                .newArrayList(layoutRawRecItem.getLayoutMetric().getLatencyMap().getMap().values()).get(0).longValue());
    }

    @Test
    public void testGenerateRawRecommendationsLayoutMetric() {
        // prepare two origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact", "select name from test_country" }, true);
        List<NDataModel> originModels = smartContext.getOriginModels();

        // generate raw recommendations for origin model
        rawRecService.generateRawRecommendations(getProject(), queryHistories());

        // check raw recommendations layoutMetric
        List<RawRecItem> recOfCountry = jdbcRawRecStore.listAll(getProject(), originModels.get(0).getUuid(), 0, 10);
        Assert.assertEquals(2, recOfCountry.size());
        RawRecItem dimRecItemOfCountry = jdbcRawRecStore.queryById(1);
        RawRecItem layoutRecItemOfCountry = jdbcRawRecStore.queryById(3);
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, dimRecItemOfCountry.getType());
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRecItemOfCountry.getType());
        Assert.assertEquals(1, layoutRecItemOfCountry.getLayoutMetric().getFrequencyMap().getDateFrequency()
                .get(QUERY_TIME).intValue());

        List<RawRecItem> recOfFact = jdbcRawRecStore.listAll(getProject(), originModels.get(1).getUuid(), 0, 10);
        Assert.assertEquals(2, recOfFact.size());
        RawRecItem dimRecItemOfFact = jdbcRawRecStore.queryById(2);
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, dimRecItemOfFact.getType());
        RawRecItem layoutRecItemOfFact = jdbcRawRecStore.queryById(4);
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, layoutRecItemOfFact.getType());
        Assert.assertEquals(1,
                layoutRecItemOfFact.getLayoutMetric().getFrequencyMap().getDateFrequency().get(QUERY_TIME).intValue());
    }

    @Test
    public void testSemiV2BasicWhenInnerJoinPartialMatch() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");

        // prepare origin model A inner join B inner join C
        String[] sql1 = new String[] { "select TRANS_ID from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sql1, true);

        // model(A inner join B) will reuse model(A inner join B inner join C)
        MetadataTestUtils.toSemiAutoMode(getProject());
        String[] sql2 = new String[] { "select CAL_DT,sum(price) from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID group by CAL_DT" };
        val context1 = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sql2);
        AbstractContext.ModelContext modelContext1 = context1.getModelContexts().get(0);

        // validate
        Map<String, DimensionRecItemV2> dimensionRecItemMap = modelContext1.getDimensionRecItemMap();
        Assert.assertEquals(1, dimensionRecItemMap.size());
        Map.Entry<String, DimensionRecItemV2> dimensionEntry = dimensionRecItemMap.entrySet().iterator().next();
        DimensionRecItemV2 dimensionRecItemV2 = dimensionEntry.getValue();
        Assert.assertEquals("TEST_KYLIN_FACT$CAL_DT", dimensionRecItemV2.getUniqueContent());
        Assert.assertEquals("date", dimensionRecItemV2.getDataType());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", dimensionRecItemV2.getColumn().getAliasDotColumn());

        Map<String, MeasureRecItemV2> measureRecItemMap = modelContext1.getMeasureRecItemMap();
        Assert.assertEquals(1, measureRecItemMap.size());
        Map.Entry<String, MeasureRecItemV2> measureEntry = measureRecItemMap.entrySet().iterator().next();
        MeasureRecItemV2 measureRecItemV2 = measureEntry.getValue();
        Assert.assertEquals("SUM__TEST_KYLIN_FACT$PRICE", measureRecItemV2.getUniqueContent());
        Assert.assertEquals("SUM_TEST_KYLIN_FACT_PRICE", measureRecItemV2.getMeasure().getName());

        Assert.assertEquals(1, modelContext1.getIndexRexItemMap().size());
        Assert.assertEquals("{colOrder=[5, 100000, 100001],sortCols=[],shardCols=[]}",
                Lists.newArrayList(modelContext1.getIndexRexItemMap().keySet()).get(0));
    }

    @Test
    public void testTransferToCCRawRecItemBasic() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " }, true);

        // generate raw recommendations for origin model
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select sum(price+1),count(ORDER_ID+TRANS_ID) from test_kylin_fact group by price * 3");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));

        // validate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort((o1, o2) -> {
            if (o1.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN
                    && o2.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN) {
                return ((CCRecItemV2) o1.getRecEntity()).getCc().getExpression()
                        .compareTo(((CCRecItemV2) o2.getRecEntity()).getCc().getExpression());
            }
            return o1.getType().id() - o2.getType().id();
        });
        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(0).getType());
        Assert.assertEquals(6, rawRecItems.get(0).getDependIDs()[0]);
        Assert.assertEquals(11, rawRecItems.get(0).getDependIDs()[1]);
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(0).getState());
        Assert.assertEquals(CC_ORDER_ID_PLUS_TRANS_ID,
                ((CCRecItemV2) rawRecItems.get(0).getRecEntity()).getCc().getExpression());

        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(1).getType());
        Assert.assertEquals(7, rawRecItems.get(1).getDependIDs()[0]);
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(1).getState());
        Assert.assertEquals(PRICE_COLUMN + " * 3",
                ((CCRecItemV2) rawRecItems.get(1).getRecEntity()).getCc().getExpression());

        Assert.assertEquals(RawRecItem.RawRecType.COMPUTED_COLUMN, rawRecItems.get(2).getType());
        Assert.assertEquals(7, rawRecItems.get(2).getDependIDs()[0]);
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(2).getState());
        Assert.assertEquals(PRICE_COLUMN + " + 1",
                ((CCRecItemV2) rawRecItems.get(2).getRecEntity()).getCc().getExpression());
    }

    @Test
    public void testTransferToDimensionRawRecItemBasic() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" }, true);

        // will not recommend existing dimensions
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select count(*) from test_kylin_fact group by price");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));

        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(1, rawRecItems.size());
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, rawRecItems.get(0).getType());

        // happy pass
        queryHistory1.setSql("select count(*) from test_kylin_fact group by cal_dt");
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));
        rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort(Comparator.comparingInt(o -> o.getType().id()));
        Assert.assertEquals(0, rawRecItems.get(0).getDependIDs()[0]);
        Assert.assertEquals(RawRecItem.RawRecType.DIMENSION, rawRecItems.get(0).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(0).getState());
        Assert.assertEquals("TEST_KYLIN_FACT$CAL_DT", rawRecItems.get(0).getRecEntity().getUniqueContent());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT",
                ((DimensionRecItemV2) rawRecItems.get(0).getRecEntity()).getColumn().getAliasDotColumn());

    }

    @Test
    public void testTransferToMeasureRawRecItemBasic() {
        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " }, true);

        // generate raw recommendations for origin model
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql(
                "select sum(price),count(price),avg(price),min(ORDER_ID)," + "max(ORDER_ID) from test_kylin_fact");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));

        // validate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort((o1, o2) -> {
            if (o1.getType() == RawRecItem.RawRecType.MEASURE && o2.getType() == RawRecItem.RawRecType.MEASURE) {
                return o1.getRecEntity().getUniqueContent().compareTo(o2.getRecEntity().getUniqueContent());
            }
            return o1.getType().id() - o2.getType().id();
        });
        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(0).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(0).getState());
        Assert.assertEquals("COUNT__TEST_KYLIN_FACT$PRICE", rawRecItems.get(0).getRecEntity().getUniqueContent());
        Assert.assertEquals(7, rawRecItems.get(0).getDependIDs()[0]);
        Assert.assertEquals("COUNT_TEST_KYLIN_FACT_PRICE",
                ((MeasureRecItemV2) rawRecItems.get(0).getRecEntity()).getMeasure().getName());

        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(1).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(1).getState());
        Assert.assertEquals("MAX__TEST_KYLIN_FACT$ORDER_ID", rawRecItems.get(1).getRecEntity().getUniqueContent());
        Assert.assertEquals(6, rawRecItems.get(1).getDependIDs()[0]);
        Assert.assertEquals("MAX_TEST_KYLIN_FACT_ORDER_ID",
                ((MeasureRecItemV2) rawRecItems.get(1).getRecEntity()).getMeasure().getName());

        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(2).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(2).getState());
        Assert.assertEquals("MIN__TEST_KYLIN_FACT$ORDER_ID", rawRecItems.get(2).getRecEntity().getUniqueContent());
        Assert.assertEquals(6, rawRecItems.get(2).getDependIDs()[0]);
        Assert.assertEquals("MIN_TEST_KYLIN_FACT_ORDER_ID",
                ((MeasureRecItemV2) rawRecItems.get(2).getRecEntity()).getMeasure().getName());

        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(3).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(3).getState());
        Assert.assertEquals("SUM__TEST_KYLIN_FACT$PRICE", rawRecItems.get(3).getRecEntity().getUniqueContent());
        Assert.assertEquals(7, rawRecItems.get(3).getDependIDs()[0]);
        Assert.assertEquals("SUM_TEST_KYLIN_FACT_PRICE",
                ((MeasureRecItemV2) rawRecItems.get(3).getRecEntity()).getMeasure().getName());
    }

    @Test
    public void testTransferToAvgMeasureRawRecItem() {
        overwriteSystemProp("kylin.query.replace-count-column-with-count-star", "true");

        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select count(*) from test_kylin_fact " }, true);

        // generate raw recommendations for origin model
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select avg(price) from test_kylin_fact");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));

        // count(*) answer avg only work on query
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort((o1, o2) -> {
            if (RawRecItem.RawRecType.MEASURE == o1.getType() && RawRecItem.RawRecType.MEASURE == o2.getType()) {
                return o1.getRecEntity().getUniqueContent().compareTo(o2.getRecEntity().getUniqueContent());
            }
            return o1.getType().id() - o2.getType().id();
        });

        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(0).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(0).getState());
        Assert.assertEquals("COUNT__TEST_KYLIN_FACT$PRICE", rawRecItems.get(0).getRecEntity().getUniqueContent());
        Assert.assertEquals(7, rawRecItems.get(0).getDependIDs()[0]);
        Assert.assertEquals("COUNT_TEST_KYLIN_FACT_PRICE",
                ((MeasureRecItemV2) rawRecItems.get(0).getRecEntity()).getMeasure().getName());

        Assert.assertEquals(RawRecItem.RawRecType.MEASURE, rawRecItems.get(1).getType());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(1).getState());
        Assert.assertEquals("SUM__TEST_KYLIN_FACT$PRICE", rawRecItems.get(1).getRecEntity().getUniqueContent());
        Assert.assertEquals(7, rawRecItems.get(1).getDependIDs()[0]);
        Assert.assertEquals("SUM_TEST_KYLIN_FACT_PRICE",
                ((MeasureRecItemV2) rawRecItems.get(1).getRecEntity()).getMeasure().getName());
    }

    @Test
    public void testTransferToIndexRawRecItemBasic() {
        // prepare origin model
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" }, true);

        // recommend
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select count(*) from test_kylin_fact group by price");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setDuration(30L);
        queryHistory1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1));

        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(0).getState());
        Assert.assertEquals(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, rawRecItems.get(0).getType());
        Assert.assertEquals(7, rawRecItems.get(0).getDependIDs()[0]);
        Assert.assertEquals(1,
                Lists.newArrayList(rawRecItems.get(0).getLayoutMetric().getFrequencyMap().getDateFrequency().values())
                        .get(0).intValue());
        Assert.assertEquals(30, Lists
                .newArrayList(rawRecItems.get(0).getLayoutMetric().getLatencyMap().getTotalLatencyMapPerDay().values())
                .get(0).intValue());
        Assert.assertEquals(1, rawRecItems.get(0).getHitCount());
    }

    @Test
    public void testMarkFailAccelerateMessageToQueryHistory() {
        // prepare query history
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, getProject()));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, getProject()));

        // create a fail accelerate and succeed accelerate, then mark to query history
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select ACCOUNT_SELLER_LEVEL from TEST_ACCOUNT" }, true);

        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select ACCOUNT_BUYER_LEVEL from TEST_ACCOUNT");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setSql("select country from test_country");
        queryHistory2.setId(2);

        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(queryHistory1, queryHistory2));

        // check if mark succeed
        List<QueryHistory> allQueryHistories = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(2, allQueryHistories.size());
        Assert.assertEquals(1, allQueryHistories.get(0).getId());
        Assert.assertNull(allQueryHistories.get(0).getQueryHistoryInfo().getErrorMsg());
        // fail accelerate mark succeed
        Assert.assertEquals(2, allQueryHistories.get(1).getId());
        Assert.assertEquals("There is no compatible model to accelerate this sql.",
                allQueryHistories.get(1).getQueryHistoryInfo().getErrorMsg());
    }

    @Test
    public void testTransferToIndexWithNpeWithNoMsg() {
        // prepare query history
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, getProject()));

        // create a fail accelerate and succeed accelerate, then mark to query history
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select ACCOUNT_SELLER_LEVEL from TEST_ACCOUNT" }, true);

        String sql = "select count(*) from test_kylin_fact group by price";
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql(sql);
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setDuration(30L);
        queryHistory1.setId(1);

        List<String> sqlList = Lists.newArrayList();
        sqlList.add(sql);
        ListMultimap<String, QueryHistory> queryHistoryMap = ArrayListMultimap.create();
        QueryHistorySql queryHistorySql = queryHistory1.getQueryHistorySql();
        String normalizedSql = queryHistorySql.getNormalizedSql();
        sqlList.add(normalizedSql);
        queryHistoryMap.put(normalizedSql, queryHistory1);

        AbstractContext semiContextV2 = ProposerJob
                .propose(new ModelReuseContext(kylinConfig, getProject(), sqlList.toArray(new String[0])));
        Map<String, AccelerateInfo> accelerateInfoMap = semiContextV2.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, accelerateInfo) -> {
            // mock NPE
            accelerateInfo.setFailedCause(new NullPointerException());
            accelerateInfo.getRelatedLayouts().clear();
        });

        rawRecService.markFailAccelerateMessageToQueryHistory(queryHistoryMap, semiContextV2);
        List<QueryHistory> allQueryHistories = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(1, allQueryHistories.size());
        QueryHistory queryHistory = allQueryHistories.get(0);
        Assert.assertNull(queryHistory.getQueryHistoryInfo().getErrorMsg());
    }

    @Test
    public void testTransferToIndexWithNpeWithDesiredMsg() {
        // prepare query history
        queryHistoryDAO.insert(createQueryMetrics(1580311512001L, 2L, false, getProject()));

        // create a fail accelerate and succeed accelerate, then mark to query history
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select ACCOUNT_SELLER_LEVEL from TEST_ACCOUNT" }, true);

        String sql = "select count(*) from test_kylin_fact group by price";
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql(sql);
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setDuration(30L);
        queryHistory1.setId(1);

        List<String> sqlList = Lists.newArrayList();
        sqlList.add(sql);
        ListMultimap<String, QueryHistory> queryHistoryMap = ArrayListMultimap.create();
        QueryHistorySql queryHistorySql = queryHistory1.getQueryHistorySql();
        String normalizedSql = queryHistorySql.getNormalizedSql();
        sqlList.add(normalizedSql);
        queryHistoryMap.put(normalizedSql, queryHistory1);

        AbstractContext semiContextV2 = ProposerJob
                .propose(new ModelReuseContext(kylinConfig, getProject(), sqlList.toArray(new String[0])));
        Map<String, AccelerateInfo> accelerateInfoMap = semiContextV2.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, accelerateInfo) -> {
            // mock NPE
            accelerateInfo.setFailedCause(new NullPointerException("caused by null"));
            accelerateInfo.getRelatedLayouts().clear();
        });

        rawRecService.markFailAccelerateMessageToQueryHistory(queryHistoryMap, semiContextV2);
        List<QueryHistory> allQueryHistories = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(1, allQueryHistories.size());
        QueryHistory queryHistory = allQueryHistories.get(0);
        Assert.assertEquals("caused by null", queryHistory.getQueryHistoryInfo().getErrorMsg());
    }

    @Test
    public void testMergeAggIndexOfSameDimensionForSemiV2() {
        String project = "cc_test";

        // create origin model
        AccelerationUtil.runWithSmartContext(getTestConfig(), project,
                new String[] { "SELECT LO_CUSTKEY FROM SSB.LINEORDER limit 10" }, true);

        // suggest model
        String[] sqls = new String[] { "SELECT min(LO_CUSTKEY) FROM LINEORDER limit 10",
                "SELECT max(LO_CUSTKEY) FROM SSB.LINEORDER limit 10" };
        AbstractContext proposeContext = new ModelReuseContext(getTestConfig(), project, sqls, true);
        val smartMaster = new SmartMaster(proposeContext);
        smartMaster.executePropose();

        // two layout will merge to one layout rec
        Assert.assertEquals(1, proposeContext.getModelContexts().get(0).getIndexRexItemMap().size());
        for (LayoutRecItemV2 layoutRecItem : proposeContext.getModelContexts().get(0).getIndexRexItemMap().values()) {
            Assert.assertEquals(2, layoutRecItem.getLayout().getMeasureIds().size());
        }
    }

    @Test
    public void testExplicitUseCC() {
        String project = "cc_test";
        String[] sqls = new String[] {
                "SELECT min(CC_LTAX) FROM SSB.LINEORDER LINEORDER INNER JOIN SSB.CUSTOMER as CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY" };
        AbstractContext proposeContext = AccelerationUtil.runModelReuseContext(getTestConfig(), project, sqls, true);

        // two layout will merge to one layout rec
        Assert.assertEquals(1, proposeContext.getModelContexts().get(0).getIndexRexItemMap().size());
        for (LayoutRecItemV2 layoutRecItem : proposeContext.getModelContexts().get(0).getIndexRexItemMap().values()) {
            Assert.assertEquals(2, layoutRecItem.getLayout().getMeasureIds().size());
        }
        Assert.assertEquals(0, proposeContext.getModelContexts().get(0).getCcRecItemMap().size());
    }

    @Test
    public void testUpdateCostsAndTopNCandidates() {
        overwriteSystemProp("kylin.smart.update-cost-method", "TIME_DECAY");
        val pair = prepareRecItems();
        List<RawRecItem> rawRecItems = pair.getValue();
        String modelId = pair.getKey();

        Assert.assertEquals(9, rawRecItems.size());
        Assert.assertEquals(18.39, rawRecItems.get(5).getCost(), 0.01);
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(5).getState());
        Assert.assertNotEquals(rawRecItems.get(5).getCreateTime(), rawRecItems.get(5).getUpdateTime());
        Assert.assertEquals(22.07, rawRecItems.get(6).getCost(), 0.01);
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(6).getState());
        Assert.assertNotEquals(rawRecItems.get(6).getCreateTime(), rawRecItems.get(6).getUpdateTime());
        Assert.assertEquals(25.75, rawRecItems.get(7).getCost(), 0.01);
        Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, rawRecItems.get(7).getState());
        Assert.assertNotEquals(rawRecItems.get(7).getCreateTime(), rawRecItems.get(7).getUpdateTime());
        Assert.assertEquals(25.75, rawRecItems.get(8).getCost(), 0.01);
        Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, rawRecItems.get(8).getState());
        Assert.assertNotEquals(rawRecItems.get(8).getCreateTime(), rawRecItems.get(8).getUpdateTime());

        // change to old version RawRecItem with recSource is null then updateCost and validate

        List<RawRecItem> layoutRecItems = jdbcRawRecStore.queryAll();
        layoutRecItems.forEach(recItem -> recItem.setRecSource(null));
        jdbcRawRecStore.batchAddOrUpdate(layoutRecItems);
        for (RawRecItem recItem : jdbcRawRecStore.queryAll()) {
            Assert.assertNull(recItem.getRecSource());
        }
        RawRecManager.getInstance(getProject()).clearExistingCandidates(getProject(), modelId);
        List<RawRecItem> recItemsAfterClear = jdbcRawRecStore.queryAll();
        recItemsAfterClear.forEach(recItem -> Assert.assertEquals(RawRecItem.RawRecState.INITIAL, recItem.getState()));
        recItemsAfterClear.forEach(recItem -> {
            if (recItem.isLayoutRec()) {
                recItem.setRecSource("QUERY_HISTORY");
            }
        });
        jdbcRawRecStore.batchAddOrUpdate(recItemsAfterClear);
        jdbcRawRecStore.queryAll().forEach(recItem -> {
            if (recItem.isLayoutRec()) {
                Assert.assertEquals("QUERY_HISTORY", recItem.getRecSource());
            }
        });
        RawRecManager.getInstance(getProject()).updateRecommendedTopN(getProject(), modelId, 100);
        List<RawRecItem> allRecItems = jdbcRawRecStore.queryAll();
        allRecItems.forEach(recItem -> {
            if (recItem.isLayoutRec()) {
                Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, recItem.getState());
            }
        });

        // reset
        FavoriteRuleManager.getInstance(getProject()).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition(null, "20")), true, FavoriteRule.REC_SELECT_RULE_NAME);
    }

    @Test
    public void testUpdateCostsAndTopNCandidatesByHitCount() {
        overwriteSystemProp("kylin.smart.update-cost-method", "HIT_COUNT");
        List<RawRecItem> rawRecItems = prepareRecItems().getValue();
        Assert.assertEquals(9, rawRecItems.size());
        Assert.assertEquals(1, (int) rawRecItems.get(5).getCost());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(5).getState());
        Assert.assertNotEquals(rawRecItems.get(5).getCreateTime(), rawRecItems.get(5).getUpdateTime());
        Assert.assertEquals(2, (int) rawRecItems.get(8).getCost());
        Assert.assertEquals(RawRecItem.RawRecState.INITIAL, rawRecItems.get(6).getState());
    }

    private Pair<String, List<RawRecItem>> prepareRecItems() {
        long yesterday = System.currentTimeMillis() - MILLIS_PER_DAY;
        // prepare raw recommendation
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact" }, true);

        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelId = modelContexts.get(0).getTargetModel().getUuid();

        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select count(price) from test_kylin_fact group by cal_dt");
        queryHistory1.setQueryTime(yesterday);
        queryHistory1.setDuration(30L);
        queryHistory1.setId(1);
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select sum(price) from test_kylin_fact group by cal_dt");
        queryHistory2.setQueryTime(yesterday);
        queryHistory2.setDuration(40L);
        queryHistory2.setId(2);
        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSql("select CAL_DT from test_kylin_fact");
        queryHistory3.setQueryTime(yesterday);
        queryHistory3.setDuration(50L);
        queryHistory3.setId(3);
        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSql("select ORDER_ID from test_kylin_fact");
        queryHistory4.setQueryTime(yesterday);
        queryHistory4.setDuration(60L);
        queryHistory4.setId(4);
        QueryHistory queryHistory5 = new QueryHistory();
        queryHistory5.setSql("select LSTG_FORMAT_NAME from test_kylin_fact");
        queryHistory5.setQueryTime(yesterday);
        queryHistory5.setDuration(70L);
        queryHistory5.setId(5);

        jdbcRawRecStore.queryAll().forEach(item -> Assert.assertEquals(item.getCreateTime(), item.getUpdateTime()));
        MetadataTestUtils.toSemiAutoMode(getProject());
        rawRecService.generateRawRecommendations(getProject(),
                Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4, queryHistory5));

        // update and select top 2 candidate
        FavoriteRuleManager.getInstance(getProject()).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition(null, "2")), true, FavoriteRule.REC_SELECT_RULE_NAME);
        rawRecService.updateCostsAndTopNCandidates(getProject());

        // validate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.sort((o1, o2) -> {
            if (RawRecItem.RawRecType.ADDITIONAL_LAYOUT == o1.getType()
                    && RawRecItem.RawRecType.ADDITIONAL_LAYOUT == o2.getType()) {
                return (int) (o1.getCost() - o2.getCost());
            }
            return o1.getType().id() - o2.getType().id();
        });
        return Pair.newPair(modelId, rawRecItems);
    }

    private List<QueryHistory> queryHistories() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select CAL_DT from test_kylin_fact");
        queryHistory1.setQueryTime(QUERY_TIME);
        queryHistory1.setId(1);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setQueryTime(QUERY_TIME);
        queryHistory2.setSql("select country from test_country");
        queryHistory2.setId(2);

        return Lists.newArrayList(queryHistory1, queryHistory2);
    }

    private QueryMetrics createQueryMetrics(long queryTime, long duration, boolean indexHit, String project) {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(duration);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(indexHit);
        queryMetrics.setQueryTime(queryTime);
        queryMetrics.setQueryFirstDayOfMonth(TimeUtil.getMonthStart(queryTime));
        queryMetrics.setQueryFirstDayOfWeek(TimeUtil.getWeekStart(queryTime));
        queryMetrics.setQueryDay(TimeUtil.getDayStart(queryTime));
        queryMetrics.setProjectName(project);
        queryMetrics.setQueryStatus("SUCCEEDED");
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName(project);

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        queryHistoryInfo.setRealizationMetrics(realizationMetricsList);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
        return queryMetrics;
    }
}
