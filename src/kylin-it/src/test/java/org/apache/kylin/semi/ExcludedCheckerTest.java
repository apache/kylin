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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.index.IndexSuggester;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SemiAutoTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

public class ExcludedCheckerTest extends SemiAutoTestBase {

    private RawRecService rawRecService;
    private NDataModelManager modelManager;
    private ProjectService projectService;

    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    ModelService modelService = Mockito.spy(ModelService.class);
    @Mock
    ModelSmartService modelSmartService = Mockito.spy(ModelSmartService.class);
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Before
    public void setUp() throws Exception {
        super.setUp();
        rawRecService = new RawRecService();
        projectService = new ProjectService();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        prepareACL();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelSmartService, "modelService", modelService);
        ReflectionTestUtils.setField(modelSmartService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelSmartService, "rawRecService", rawRecService);
        ReflectionTestUtils.setField(modelSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    private String prepareInitialModel() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        return modelID;
    }

    @Test
    public void testBasicExcludeTable() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludeFactTable = "DEFAULT.TEST_KYLIN_FACT";
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        String[] sqls = { "select buyer_id, sum(price) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        // exclude fact table then validate the result => fact table not affect by the excluded table
        {
            MetadataTestUtils.mockExcludedTable(getProject(), excludeFactTable);
            Set<String> tables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertEquals(1, tables.size());
            Assert.assertTrue(tables.contains(excludeFactTable));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[12, 100000, 100001],sortCols=[],shardCols=[]}", key);
            NDataModel targetModel = modelContext.getTargetModel();
            NDataModel.NamedColumn namedColumn = targetModel.getAllNamedColumns().get(12);
            Assert.assertEquals("TEST_ORDER.BUYER_ID", namedColumn.getAliasDotColumn());
            Assert.assertTrue(namedColumn.isDimension());
        }

        // exclude the lookup table then validate  => foreign key in the index
        {
            MetadataTestUtils.mockExcludedTable(getProject(), excludedLookupTable);
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertEquals(2, lookupTables.size());
            Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[6, 100000, 100001],sortCols=[],shardCols=[]}", key);
            NDataModel targetModel = modelContext.getTargetModel();
            NDataModel.NamedColumn namedColumn = targetModel.getAllNamedColumns().get(6);
            Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", namedColumn.getAliasDotColumn());
            Assert.assertTrue(namedColumn.isDimension());
        }
    }

    @Test
    public void testBasicExcludedColumns() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        String[] sqls = { "select buyer_id, sum(price) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };
        // exclude the deliberated lookup column  => foreign key in the index
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[6, 100000, 100001],sortCols=[],shardCols=[]}", key);
            NDataModel targetModel = modelContext.getTargetModel();
            NDataModel.NamedColumn namedColumn = targetModel.getAllNamedColumns().get(6);
            Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", namedColumn.getAliasDotColumn());
            Assert.assertTrue(namedColumn.isDimension());
        }
    }

    @Test
    public void testProposedLayoutNotIncludeAllFks() {
        prepareInitialModel();

        // disable propose all fks
        overwriteSystemProp("kylin.smart.conf.propose-all-foreign-keys", "false");

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        String[] sqls = { "select sum(price) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id " };
        // exclude the deliberated lookup column  => foreign key in the index
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(0, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[100000, 100001],sortCols=[],shardCols=[]}", key);
        }
    }

    @Test
    public void testProposeCCOnlyDependOnFactTable() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludeFactTable = "DEFAULT.TEST_KYLIN_FACT";
        String[] sqls = { "select buyer_id, sum(price * item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        // exclude fact table column then validate the result
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludeFactTable, Sets.newHashSet("item_count"));
            Set<String> tables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(tables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludeFactTable);
            Assert.assertEquals(1, excludedColumns.size());
            Assert.assertTrue(excludedColumns.contains("ITEM_COUNT"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getCcRecItemMap().size());
            CCRecItemV2 ccRec = modelContext.getCcRecItemMap().entrySet().iterator().next().getValue();
            Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`_DEFAULT.TEST_KYLIN_FACT",
                    ccRec.getUniqueContent());
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[12, 100000, 100001],sortCols=[],shardCols=[]}", key);
            NDataModel targetModel = modelContext.getTargetModel();
            NDataModel.NamedColumn namedColumn = targetModel.getAllNamedColumns().get(12);
            Assert.assertEquals("TEST_ORDER.BUYER_ID", namedColumn.getAliasDotColumn());
            Assert.assertTrue(namedColumn.isDimension());
        }
    }

    @Test
    public void testProposeCCOnlyDependOnLookupTable() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        String[] sqls = {
                "select sum(buyer_id+ 1) from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id ",
                "select buyer_id+ 1 from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id+ 1",
                "select max(buyer_id + 1) from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id " };

        // case 1: exclude the lookup table then validate
        {
            MetadataTestUtils.mockExcludedTable(getProject(), excludedLookupTable);
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertEquals(1, lookupTables.size());
            Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
            Assert.assertTrue(accelerateInfo.isPending());

            AbstractContext anotherContext = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[1] });
            AbstractContext.ModelContext modelContext = anotherContext.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            DimensionRecItemV2 dimRec = modelContext.getDimensionRecItemMap().entrySet().iterator().next().getValue();
            Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", dimRec.getColumn().getAliasDotColumn());
        }

        // case 2: exclude the lookup table column then validate the result
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
            Assert.assertTrue(accelerateInfo.isPending());

            AbstractContext anotherContext = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[1] });
            AbstractContext.ModelContext modelContext = anotherContext.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            DimensionRecItemV2 dimRec = modelContext.getDimensionRecItemMap().entrySet().iterator().next().getValue();
            Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", dimRec.getColumn().getAliasDotColumn());
        }

        // case 3: exclude the lookup table column but the measure function is max
        {
            // this case reuses the excluded columns of case 2
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(),
                    new String[] { sqls[0], sqls[2] });
            context.getAccelerateInfoMap().forEach((k, accelerateInfo) -> {
                String pendingMsg = accelerateInfo.getPendingMsg();
                Assert.assertTrue(pendingMsg.startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
                Assert.assertTrue(accelerateInfo.isPending());
            });
        }
    }

    /**
     * If excluded p_size, can not query by ke, what can we do?
     * select lo_commitdate, sum(p_size)
     * from ssb.lineorder inner join ssb.part on lineorder.lo_partkey=part.p_partkey
     * group by lo_commitdate
     */
    @Test
    public void testProposeSomeCCOnLookupTableOtherOnFactTable() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        // The cardinality of TEST_ORDER.TEST_EXTENDED_COLUMN is 957, set the following config to 900 for cc proposing
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.group-key.minimum-cardinality", "900");
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String[] sqls = {
                "select sum(price * item_count), sum(buyer_id+ 1) from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id ",
                "select sum(price * item_count), sum(case when buyer_id > 0 then 1 else 0 end) "
                        + "from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id ",
                "select case when test_extended_column = 'a' then 1 else 0 end, sum(price * item_count) "
                        + "from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id "
                        + "group by case when test_extended_column = 'a' then 1 else 0 end" };

        // exclude the deliberated lookup column, 'sum' example
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
            Assert.assertTrue(accelerateInfo.isPending());
        }

        // exclude the deliberated lookup column then validate, 'sum case when' example
        {
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[1] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[1]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
        }

        // not exclude column, 'group by case when' example
        {

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[2] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(2, modelContext.getCcRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            NDataModel.NamedColumn column = modelContext.getDimensionRecItemMap().entrySet().iterator().next()
                    .getValue().getColumn();
            String dimCCInnerExp = "CASE WHEN `TEST_ORDER`.`TEST_EXTENDED_COLUMN` = 'a' THEN 1 ELSE 0 END";
            String measureCCInnerExp = "`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`";
            List<ComputedColumnDesc> ccList = modelContext.getTargetModel().getComputedColumnDescs();
            ccList.forEach(cc -> {
                if (cc.getColumnName().equals(column.getName())) {
                    Assert.assertEquals(dimCCInnerExp, cc.getInnerExpression());
                } else {
                    Assert.assertEquals(measureCCInnerExp, cc.getInnerExpression());
                }
            });
        }

        // exclude the deliberated lookup column, 'group by case when' example
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable,
                    Sets.newHashSet("test_extended_column"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("TEST_EXTENDED_COLUMN"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[2] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            DimensionRecItemV2 dimRec = modelContext.getDimensionRecItemMap().entrySet().iterator().next().getValue();
            Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", dimRec.getColumn().getAliasDotColumn());
        }
    }

    @Test
    public void testOneSqlWithCCUseColumnsOfFactTableAndLookupTable() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String[] sqls = {
                "select test_order.order_id, sum(item_count + test_order.order_id) from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by test_order.order_id",
                "select price, sum(item_count + test_order.buyer_id) from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by price" };

        // create new cc & propose fk index
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getCcRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            DimensionRecItemV2 dimRec = modelContext.getDimensionRecItemMap().entrySet().iterator().next().getValue();
            Assert.assertEquals("TEST_ORDER.ORDER_ID", dimRec.getColumn().getAliasDotColumn());
            CCRecItemV2 ccRec = modelContext.getCcRecItemMap().entrySet().iterator().next().getValue();
            String expectedInnerExp = "`TEST_KYLIN_FACT`.`ITEM_COUNT` + `TEST_ORDER`.`ORDER_ID`";
            Assert.assertEquals(expectedInnerExp, ccRec.getCc().getInnerExpression());
        }

        // can not create new cc
        {
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[1] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[1]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
        }
    }

    @Test
    public void testProposeMeasureOnLookupTable() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String[] sqls = { "select price, sum(buyer_id) "
                + "from test_kylin_fact inner join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by price",
                "select price, count(test_extended_column) "
                        + "from test_kylin_fact inner join test_order on test_kylin_fact.order_id = test_order.order_id "
                        + "group by price" };

        // exclude the deliberated lookup table column
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
            Assert.assertTrue(accelerateInfo.isPending());
        }

        // not exclude column, 'count' example
        {
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[1] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(0, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(0, modelContext.getCcRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            NDataModel.Measure measure = modelContext.getMeasureRecItemMap().entrySet().iterator().next().getValue()
                    .getMeasure();
            List<ParameterDesc> parameters = measure.getFunction().getParameters();
            Assert.assertEquals(1, parameters.size());
            Assert.assertEquals("TEST_ORDER.TEST_EXTENDED_COLUMN", parameters.get(0).getValue());
        }
    }

    @Test
    public void testReuseExistedCC() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(buyer_id+ 1), sum(price * item_count) "
                        + "from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(19, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(3, modelBeforeGenerateRecItems.getAllMeasures().size());
        List<ComputedColumnDesc> ccList = modelBeforeGenerateRecItems.getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getInnerExpression));
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                ccList.get(0).getInnerExpression());
        Assert.assertEquals("`TEST_ORDER`.`BUYER_ID` + 1", ccList.get(1).getInnerExpression());
        String ccName = ccList.get(0).getColumnName();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String[] sqls = {
                "select lstg_format_name, buyer_id+ 1 from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name, buyer_id + 1",
                "select lstg_format_name, price * item_count from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id "
                        + "group by lstg_format_name, price * item_count",
                "select lstg_format_name, sum(buyer_id+ 1) from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name, buyer_id + 1" };

        // exclude the lookup table then validate
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            // ignore excluded cc
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
            Assert.assertTrue(modelContext.getCcRecItemMap().isEmpty());
            Assert.assertTrue(modelContext.getMeasureRecItemMap().isEmpty());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[6, 8, 100000],sortCols=[],shardCols=[]}", key);
            NDataModel targetModel = modelContext.getTargetModel();
            NDataModel.NamedColumn namedColumn = targetModel.getAllNamedColumns().get(6);
            Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", namedColumn.getAliasDotColumn());
            NDataModel.NamedColumn namedColumn2 = targetModel.getAllNamedColumns().get(8);
            Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", namedColumn2.getAliasDotColumn());
        }

        // reuse fact table cc
        {
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[1] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            Assert.assertTrue(modelContext.getCcRecItemMap().isEmpty());
            modelContext.getDimensionRecItemMap().forEach((k, dimRec) -> {
                NDataModel.NamedColumn column = dimRec.getColumn();
                if (column.getName().startsWith(ComputedColumnUtil.CC_NAME_PREFIX)) {
                    Assert.assertEquals(ccName, column.getName());
                } else {
                    Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", column.getAliasDotColumn());
                }
            });
        }

        // can not reuse existed cc for agg
        {
            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[2] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[2]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
        }
    }

    @Test
    public void testMeasureAlreadyExist() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(buyer_id) from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        String[] sqls = { "select lstg_format_name, sum(buyer_id) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name" };

        {
            // exclude the lookup table then validate
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS));
        }
    }

    @Test
    public void testDimensionAlreadyExist() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select buyer_id from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id group by buyer_id" },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        modelBeforeGenerateRecItems.getAllNamedColumns().stream() //
                .filter(col -> col.getAliasDotColumn().equalsIgnoreCase("test_order.buyer_id")) //
                .forEach(col -> Assert.assertTrue(col.isDimension()));

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        String[] sqls = { "select lstg_format_name, buyer_id from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name, buyer_id" };

        // exclude the lookup table then validate  => foreign key in the index
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[4, 6, 100000],sortCols=[],shardCols=[]}", key);
        }
    }

    @Test
    public void testTableIndex() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price, sum(buyer_id + 1) from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id group by price" },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(18, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getComputedColumnDescs().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        String[] sqls = {
                "select lstg_format_name, buyer_id from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id",
                "select price, buyer_id + 1 from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id" };

        // exclude the lookup table then validate
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(2, modelContext.getIndexRexItemMap().size());

            IndexPlan targetIndexPlan = modelContext.getTargetIndexPlan();
            long layoutId1 = context.getAccelerateInfoMap().get(sqls[0]).getRelatedLayouts() //
                    .iterator().next().getLayoutId();
            long layoutId2 = context.getAccelerateInfoMap().get(sqls[1]).getRelatedLayouts() //
                    .iterator().next().getLayoutId();

            Assert.assertEquals("[5, 7]", targetIndexPlan.getLayoutEntity(layoutId1).getColOrder().toString());
            Assert.assertEquals("[7, 8]", targetIndexPlan.getLayoutEntity(layoutId2).getColOrder().toString());
        }
    }

    @Test
    public void createEmptyModel() {

        MetadataTestUtils.toSemiAutoMode(getProject());
        // exclude the lookup table then validate
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        MetadataTestUtils.mockExcludedTable(getProject(), excludedLookupTable);
        Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        String[] sqls = { "select lstg_format_name, sum(buyer_id + 1) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name" };

        AbstractContext context = modelSmartService.suggestModel(getProject(), Arrays.asList(sqls.clone()), true, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel modelBeforeGenerateRecItems = modelContexts.get(0).getTargetModel();
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertEquals(0, modelBeforeGenerateRecItems.getComputedColumnDescs().size());
    }

    @Test
    public void testExcludeStarModel() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact "
                        + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                        + "inner join test_order on test_kylin_fact.order_id = test_order.order_id" },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(22, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String excludedLookupTable = "DEFAULT.TEST_ACCOUNT";
        String[] sqls = { "select account_buyer_level, sum(price) from test_kylin_fact "
                + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by account_buyer_level" };

        // exclude the lookup table then validate  => foreign key in the index
        MetadataTestUtils.mockExcludedTable(getProject(), excludedLookupTable);
        Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
        Assert.assertEquals(1, context.getModelContexts().size());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
        String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[11, 13, 100000, 100001],sortCols=[],shardCols=[]}", key);
        NDataModel targetModel = modelContext.getTargetModel();
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", targetModel.getAllNamedColumns().get(11).getAliasDotColumn());
        Assert.assertEquals("TEST_KYLIN_FACT.SELLER_ID", targetModel.getAllNamedColumns().get(13).getAliasDotColumn());
    }

    @Test
    public void testExcludeSnowModel() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact "
                        + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                        + "inner join test_country on test_account.account_country = test_country.country" },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(21, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        String excludedLookupTable = "DEFAULT.TEST_ACCOUNT";
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());

        String[] sqls = { "select account_buyer_level, sum(price) from test_kylin_fact "
                + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country "
                + "group by account_buyer_level" };

        // exclude the lookup table then validate
        {
            MetadataTestUtils.mockExcludedTable(getProject(), excludedLookupTable);
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertEquals(1, lookupTables.size());
            Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.FK_OF_EXCLUDED_COLUMNS));
        }

        // exclude the non join key
        {
            MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable,
                    Sets.newHashSet("account_buyer_level"));
            Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
            Assert.assertTrue(lookupTables.isEmpty());
            Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
            Assert.assertTrue(excludedColumns.contains("ACCOUNT_BUYER_LEVEL"));

            AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), sqls);
            Assert.assertEquals(1, context.getModelContexts().size());
            AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
            Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
            Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
            Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
            String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
            Assert.assertEquals("{colOrder=[2, 17, 100000, 100001],sortCols=[],shardCols=[]}", key);
            NDataModel targetModel = modelContext.getTargetModel();
            NDataModel.NamedColumn namedColumn = targetModel.getAllNamedColumns().get(2);
            Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_COUNTRY", namedColumn.getAliasDotColumn());
            Assert.assertTrue(namedColumn.isDimension());
            NDataModel.NamedColumn namedColumn2 = targetModel.getAllNamedColumns().get(17);
            Assert.assertEquals("TEST_KYLIN_FACT.SELLER_ID", namedColumn2.getAliasDotColumn());
            Assert.assertTrue(namedColumn2.isDimension());
        }
    }

    @Test
    public void testTurnOnOnlyReuseUserDefinedCCWithoutProposingAnyCC() {
        prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.setOnlyReuseUseDefinedCC(config);
        String[] sqls = { "select buyer_id, sum(price * item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
        AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.OTHER_UNSUPPORTED_MEASURE));
    }

    @Test
    public void testTurnOnOnlyReuseUserDefinedCCWithRejectReuseAutoCC() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(buyer_id+ 1), sum(price * item_count) "
                        + "from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(19, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(3, modelBeforeGenerateRecItems.getAllMeasures().size());
        List<ComputedColumnDesc> ccList = modelBeforeGenerateRecItems.getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getInnerExpression));
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                ccList.get(0).getInnerExpression());
        Assert.assertTrue(ccList.get(0).isAutoCC());
        Assert.assertEquals("`TEST_ORDER`.`BUYER_ID` + 1", ccList.get(1).getInnerExpression());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());

        MetadataTestUtils.setOnlyReuseUseDefinedCC(config);
        String excludedLookupTable = "DEFAULT.test_order";
        String[] sqls = { "select buyer_id, sum(price * item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        // reject reuse auto cc
        MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
        Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
        Assert.assertTrue(lookupTables.isEmpty());
        Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
        Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

        AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
        AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.OTHER_UNSUPPORTED_MEASURE));
    }

    @Test
    public void testTurnOnOnlyReuseUserDefinedCCWithCCReused() {
        String modelID = prepareInitialModel();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());
        KylinConfig config = MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        String factTable = "DEFAULT.TEST_KYLIN_FACT";

        MetadataTestUtils.setOnlyReuseUseDefinedCC(config);
        String excludedLookupTable = "DEFAULT.test_order";
        String[] sqls = { "select buyer_id, sum(price + item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        // add a user defined cc to model and propose
        MetadataTestUtils.mockExcludedCols(getProject(), excludedLookupTable, Sets.newHashSet("buyer_id"));
        Set<String> lookupTables = MetadataTestUtils.getExcludedTables(getProject());
        Assert.assertTrue(lookupTables.isEmpty());
        Set<String> excludedColumns = MetadataTestUtils.getExcludedColumns(getProject(), excludedLookupTable);
        Assert.assertTrue(excludedColumns.contains("BUYER_ID"));

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, getProject());
            modelMgr.updateDataModel(modelID, copyForWrite -> {
                ComputedColumnDesc cc = new ComputedColumnDesc();
                cc.setColumnName("CC1");
                cc.setDatatype("int");
                cc.setExpression("\"TEST_KYLIN_FACT\".\"PRICE\" + \"TEST_KYLIN_FACT\".\"ITEM_COUNT\"");
                cc.setInnerExpression("`TEST_KYLIN_FACT`.`PRICE` + `TEST_KYLIN_FACT`.`ITEM_COUNT`");
                cc.setTableAlias("TEST_KYLIN_FACT");
                cc.setTableIdentity(factTable);
                copyForWrite.getComputedColumnDescs().add(cc);
                List<NDataModel.NamedColumn> columns = copyForWrite.getAllNamedColumns();
                int id = columns.size();
                NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
                namedColumn.setName("CC1");
                namedColumn.setId(id);
                namedColumn.setAliasDotColumn("TEST_KYLIN_FACT.CC1");
                columns.add(namedColumn);
                copyForWrite.setAllNamedColumns(columns);
            });
            return null;
        }, getProject());

        List<ComputedColumnDesc> ccList = modelManager.getDataModelDesc(modelID).getComputedColumnDescs();
        Assert.assertFalse(ccList.get(0).isAutoCC());

        AbstractContext context = AccelerationUtil.genOptRec(config, getProject(), new String[] { sqls[0] });
        Assert.assertEquals(1, context.getModelContexts().size());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Map<String, DimensionRecItemV2> dimensionRecItemMap = modelContext.getDimensionRecItemMap();
        Map<String, MeasureRecItemV2> measureRecItemMap = modelContext.getMeasureRecItemMap();
        Map<String, LayoutRecItemV2> indexRexItemMap = modelContext.getIndexRexItemMap();
        Assert.assertEquals(1, dimensionRecItemMap.size());
        Assert.assertEquals(1, measureRecItemMap.size());
        Assert.assertEquals(1, indexRexItemMap.size());
        Assert.assertTrue(modelContext.getCcRecItemMap().isEmpty());

        NDataModel.NamedColumn column = dimensionRecItemMap.entrySet().iterator().next().getValue().getColumn();
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", column.getAliasDotColumn());
        NDataModel.Measure measure = measureRecItemMap.entrySet().iterator().next().getValue().getMeasure();
        Assert.assertEquals("SUM_TEST_KYLIN_FACT_CC1", measure.getName());
        Assert.assertEquals("{colOrder=[6, 100000, 100001],sortCols=[],shardCols=[]}",
                indexRexItemMap.entrySet().iterator().next().getKey());
    }
}
