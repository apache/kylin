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

import static org.apache.kylin.metadata.model.util.ComputedColumnUtil.CC_NAME_PREFIX;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnManager;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModel.NamedColumn;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelSelectProposer;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.response.LayoutRecDetailResponse;
import org.apache.kylin.rest.response.OptRecDepResponse;
import org.apache.kylin.rest.response.OptRecDetailResponse;
import org.apache.kylin.rest.response.OptRecResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.ModelChangeSupporter;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.OptRecApproveService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.ProjectSmartService;
import org.apache.kylin.rest.service.QueryHistoryAccelerateScheduler;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SCD2SimplificationConvertUtil;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SemiAutoTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SemiV2CITest extends SemiAutoTestBase {
    private static final long QUERY_TIME = 1595520000000L;

    private JdbcRawRecStore jdbcRawRecStore;
    private NDataModelManager modelManager;
    private NIndexPlanManager indexPlanManager;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @InjectMocks
    private final RawRecService rawRecService = Mockito.spy(new RawRecService());
    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());
    @InjectMocks
    private final ProjectSmartService projectSmartService = Mockito.spy(new ProjectSmartService());
    @InjectMocks
    private final OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    private final OptRecApproveService optRecApproveService = Mockito.spy(new OptRecApproveService());
    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);
    @InjectMocks
    private final ModelSmartService modelSmartService = Mockito.spy(ModelSmartService.class);
    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());
    @InjectMocks
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Spy
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @Spy
    private final List<ModelChangeSupporter> modelChangeSupporters = Mockito.spy(Arrays.asList(rawRecService));

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        jdbcRawRecStore.deleteAll();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
        QueryHistoryAccelerateScheduler queryHistoryAccelerateScheduler = QueryHistoryAccelerateScheduler.getInstance();
        ReflectionTestUtils.setField(queryHistoryAccelerateScheduler, "querySmartSupporter", rawRecService);
        ReflectionTestUtils.setField(queryHistoryAccelerateScheduler, "userGroupService", userGroupService);
        queryHistoryAccelerateScheduler.init();

        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
    }

    @After
    public void teardown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
        QueryHistoryAccelerateScheduler.shutdown();
    }

    private void prepareACL() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Test
    public void testAccelerateImmediately() throws IOException {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        FavoriteRuleManager.getInstance(getProject()).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition("0", "180")), true, FavoriteRule.DURATION_RULE_NAME);

        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeGenerateRecItems.getComputedColumnDescs().isEmpty());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        List<QueryMetrics> queryMetrics = loadQueryHistoryList(
                "../kylin-it/src/test/resources/ut_meta/newten_query_history");
        queryHistoryDAO.insert(queryMetrics);

        // before accelerate
        List<RawRecItem> rawRecItemBeforeAccelerate = jdbcRawRecStore.queryAll();
        Assert.assertTrue(rawRecItemBeforeAccelerate.isEmpty());

        // accelerate
        projectSmartService.accelerateImmediately(getProject());

        // after accelerate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(6, rawRecItems.size());
        Assert.assertEquals(1, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.COMPUTED_COLUMN));
        Assert.assertEquals(2, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.DIMENSION));
        Assert.assertEquals(1, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.MEASURE));
        Assert.assertEquals(2, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.ADDITIONAL_LAYOUT));
    }

    @Test
    public void testAccelerateManually() throws IOException {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        FavoriteRuleManager.getInstance(getProject()).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition("0", "180")), true, FavoriteRule.DURATION_RULE_NAME);

        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeGenerateRecItems.getComputedColumnDescs().isEmpty());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        List<QueryMetrics> queryMetrics = loadQueryHistoryList(
                "../kylin-it/src/test/resources/ut_meta/newten_query_history");
        queryHistoryDAO.insert(queryMetrics);

        // before accelerate
        List<RawRecItem> rawRecItemBeforeAccelerate = jdbcRawRecStore.queryAll();
        Assert.assertTrue(rawRecItemBeforeAccelerate.isEmpty());

        // accelerate
        projectSmartService.accelerateManually(getProject());

        // after accelerate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(6, rawRecItems.size());
        Assert.assertEquals(1, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.COMPUTED_COLUMN));
        Assert.assertEquals(2, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.DIMENSION));
        Assert.assertEquals(1, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.MEASURE));
        Assert.assertEquals(2, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.ADDITIONAL_LAYOUT));
    }

    @Test
    public void testDeleteOutDatedRecommendations() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        FavoriteRuleManager.getInstance(getProject()).updateRule(
                Lists.newArrayList(new FavoriteRule.Condition("0", "180")), true, FavoriteRule.DURATION_RULE_NAME);

        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeGenerateRecItems.getComputedColumnDescs().isEmpty());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        List<QueryMetrics> queryMetrics = loadQueryHistoryList(
                "../kylin-it/src/test/resources/ut_meta/newten_query_history");
        queryHistoryDAO.insert(queryMetrics);

        // before accelerate
        List<RawRecItem> rawRecItemBeforeAccelerate = jdbcRawRecStore.queryAll();
        Assert.assertTrue(rawRecItemBeforeAccelerate.isEmpty());

        // accelerate
        projectSmartService.accelerateManually(getProject());

        // after accelerate
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(6, rawRecItems.size());
        Assert.assertEquals(1, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.COMPUTED_COLUMN));
        Assert.assertEquals(2, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.DIMENSION));
        Assert.assertEquals(1, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.MEASURE));
        Assert.assertEquals(2, getFilterRecCount(rawRecItems, RawRecItem.RawRecType.ADDITIONAL_LAYOUT));

        // set to outdated
        rawRecItems.forEach(recItem -> recItem.setSemanticVersion(recItem.getSemanticVersion() - 2));
        jdbcRawRecStore.batchAddOrUpdate(rawRecItems);
        Assert.assertEquals(6, rawRecItems.size());

        // mock model broken and delete outdated recommendations
        NDataModel backupModel = modelManager.copyBySerialization(modelManager.getDataModelDesc(modelID));

        modelManager.updateDataModel(modelID, copyForWrite -> {
            copyForWrite.setBrokenReason(NDataModel.BrokenReason.EVENT);
            copyForWrite.setBroken(true);
        });

        Assert.assertTrue(modelManager.getDataModelDesc(modelID).isBroken());
        jdbcRawRecStore.deleteOutdated();
        Assert.assertEquals(6, jdbcRawRecStore.queryAll().size());

        // mock not broken and delete outdated recommendations
        backupModel.setMvcc(-1);
        modelManager.dropModel(backupModel.getId());
        modelManager.createDataModelDesc(backupModel, backupModel.getOwner());
        Assert.assertFalse(modelManager.getDataModelDesc(modelID).isBroken());
        jdbcRawRecStore.deleteOutdated();
        Assert.assertEquals(0, jdbcRawRecStore.queryAll().size());
    }

    @Test
    public void testCCAsDimensionWithoutRename() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact " }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeGenerateRecItems.getComputedColumnDescs().isEmpty());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // generate raw recommendations for origin model
        QueryHistory qh1 = new QueryHistory();
        qh1.setSql("select price+1, sum(price+1) from test_kylin_fact group by price+1");
        qh1.setQueryTime(QUERY_TIME);
        qh1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(qh1));

        // assert before apply recommendations
        NDataModel modelBeforeApplyRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeApplyRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApplyRecItems.getAllMeasures().size());
        Assert.assertTrue(modelBeforeApplyRecItems.getComputedColumnDescs().isEmpty());
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(4, rawRecItems.size());

        // get layout recommendation and change state to RECOMMENDED
        RawRecItem layoutRecItem = rawRecItems.stream().filter(RawRecItem::isAddLayoutRec).findFirst().orElse(null);
        Assert.assertNotNull(layoutRecItem);
        changeRecItemState(Lists.newArrayList(layoutRecItem), RawRecItem.RawRecState.RECOMMENDED);

        // validateSelectedRecItems
        OptRecDetailResponse optRecDetailResponse = optRecService.validateSelectedRecItems(getProject(), modelID,
                Lists.newArrayList(layoutRecItem.getId()), Lists.newArrayList());
        Assert.assertEquals(1, optRecDetailResponse.getDimensionItems().size());
        Assert.assertEquals(2, optRecDetailResponse.getMeasureItems().size());
        Assert.assertEquals(1, optRecDetailResponse.getCcItems().size());
        final OptRecDepResponse optCCRecDepResponse = optRecDetailResponse.getCcItems().get(0);
        Assert.assertEquals("\"TEST_KYLIN_FACT\".\"PRICE\" + 1", optCCRecDepResponse.getContent());
        final OptRecDepResponse optDimRecDepResponse = optRecDetailResponse.getDimensionItems().get(0);
        Assert.assertEquals(optCCRecDepResponse.getName().replace(CC_NAME_PREFIX, "DIMENSION_AUTO_"),
                optDimRecDepResponse.getName());

        // mock optRecRequest() and apply recommendations
        OptRecRequest recRequest = mockOptRecRequest(modelID, optRecDetailResponse);
        OptRecResponse optRecResponse = optRecApproveService.approve(getProject(), recRequest);
        Assert.assertEquals(1, optRecResponse.getAddedLayouts().size());
        Assert.assertEquals(0, optRecResponse.getRemovedLayouts().size());

        // assert after apply recommendations
        NDataModel modelAfterApplyRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(13, modelAfterApplyRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelAfterApplyRecItems.getAllMeasures().size());
        Assert.assertEquals(1, modelAfterApplyRecItems.getComputedColumnDescs().size());
    }

    @Test
    public void testRebuildManualCCProposeRecommendationNormally() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(price+1) from test_kylin_fact " }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(13, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertFalse(modelBeforeGenerateRecItems.getComputedColumnDescs().isEmpty());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getComputedColumnDescs().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // remove measure of sum(price + 1)
        ComputedColumnManager ccManager = ComputedColumnManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(modelID, copyForWrite -> {
            List<NDataModel.Measure> allMeasures = copyForWrite.getAllMeasures();
            allMeasures.removeIf(measure -> !measure.getFunction().isCountConstant());
            val cc = ccManager.copy(copyForWrite.getComputedColumnDescs().get(0));
            cc.setColumnName("cc1");
            copyForWrite.getComputedColumnDescs().set(0, cc);
            for (NamedColumn col : copyForWrite.getAllNamedColumns()) {
                if (col.getName().contains(CC_NAME_PREFIX)) {
                    col.setAliasDotColumn("TEST_KYLIN_FACT.cc1");
                    col.setName("cc1");
                }
            }
            copyForWrite.setAllMeasures(allMeasures);
        });

        // generate raw recommendations for origin model
        QueryHistory qh1 = new QueryHistory();
        qh1.setSql("select lstg_format_name, sum(price+1) from test_kylin_fact group by lstg_format_name");
        qh1.setQueryTime(QUERY_TIME);
        qh1.setId(1);
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(qh1));

        // assert before apply recommendations
        NDataModel modelBeforeApplyRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(13, modelBeforeApplyRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApplyRecItems.getAllMeasures().size());
        Assert.assertFalse(modelBeforeApplyRecItems.getComputedColumnDescs().isEmpty());
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(3, rawRecItems.size());
        rawRecItems.stream().filter(recItem -> recItem.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT
                || recItem.getType() == RawRecItem.RawRecType.MEASURE).forEach(recItem -> {
                    recItem.setState(RawRecItem.RawRecState.BROKEN);
                    recItem.setUniqueFlag(null);
                });
        jdbcRawRecStore.batchAddOrUpdate(rawRecItems);

        // mock delete cc then rebuild a new same cc
        modelManager.updateDataModel(modelID, copyForWrite -> {
            List<ComputedColumnDesc> ccList = copyForWrite.getComputedColumnDescs();
            String fullName = ccList.get(0).getFullName();
            List<NDataModel.NamedColumn> allNamedColumns = copyForWrite.getAllNamedColumns();
            NDataModel.NamedColumn column = allNamedColumns.stream()
                    .filter(col -> col.getAliasDotColumn().equalsIgnoreCase(fullName)).findAny().orElse(null);
            Preconditions.checkNotNull(column);
            column.setStatus(NDataModel.ColumnStatus.TOMB);
            int maxColumnId = copyForWrite.getMaxColumnId();
            NDataModel.NamedColumn newCol;
            try {
                newCol = JsonUtil.deepCopy(column, NDataModel.NamedColumn.class);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            newCol.setId(maxColumnId + 1);
            newCol.setStatus(NDataModel.ColumnStatus.EXIST);
            allNamedColumns.add(newCol);
            copyForWrite.setAllNamedColumns(allNamedColumns);
        });
        OptRecManagerV2.getInstance(getProject()).loadOptRecV2(modelID);
        // generate raw recommendations for origin model again
        rawRecService.generateRawRecommendations(getProject(), Lists.newArrayList(qh1));
        NDataModel modelBeforeApplyRecItemsAgain = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(14, modelBeforeApplyRecItemsAgain.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApplyRecItemsAgain.getAllMeasures().size());
        Assert.assertFalse(modelBeforeApplyRecItemsAgain.getComputedColumnDescs().isEmpty());
        List<RawRecItem> rawRecItemsAgain = jdbcRawRecStore.queryAll();
        Assert.assertEquals(5, rawRecItemsAgain.size());
    }

    @Test
    public void testBatchCreateModelWithProposingNewJoinRelation() {
        String project = "newten";

        // prepare initial model
        String sql = "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name";
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, project, new String[] { sql }, true);
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();

        // assert initial result
        NDataModel dataModel = modelManager.getDataModelDesc(targetModel.getUuid());
        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        long dimensionCount = allNamedColumns.stream().filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(12, allNamedColumns.size());
        Assert.assertEquals(1L, dimensionCount);
        Assert.assertEquals(2, dataModel.getAllMeasures().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(dataModel.getUuid()).getAllLayouts().size());
        Assert.assertTrue(dataModel.getJoinTables().isEmpty());

        // transfer auto model to semi-auto
        // make model online
        MetadataTestUtils.toSemiAutoMode(getProject());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            dfManager.updateDataflowStatus(targetModel.getId(), RealizationStatusEnum.ONLINE);
            return true;
        }, project);
        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");

        // optimize with a batch of sql list
        List<String> li = Lists.newArrayList();
        li.add("select test_kylin_fact.order_id, lstg_format_name\n"
                + "from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id\n");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, li, true, false);

        List<AbstractContext.ModelContext> modelContextList = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextList.size());
        SuggestionResponse suggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResponse.getReusedModels();
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResponse.getNewModels();
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        changeTheIndexRecOrder(reusedModelRequests);
        modelService.batchCreateModel(getProject(), newModelRequests, reusedModelRequests);

        // assert result after apply recommendations
        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCountRefreshed);
        List<NDataModel.Measure> allMeasures = modelAfterSuggestModel.getAllMeasures();
        Assert.assertEquals(2, allMeasures.size());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelAfterSuggestModel.getUuid());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());
        Assert.assertEquals(1, modelAfterSuggestModel.getJoinTables().size());
        Assert.assertEquals(17, modelAfterSuggestModel.getAllNamedColumns().size());
    }

    @Test
    public void testSuggestModelKeepColumnAndMeasureOrder() {
        String project = "newten";

        // prepare initial model
        String sql = "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { sql }, true);
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();

        // assert initial result
        NDataModel dataModel = modelManager.getDataModelDesc(targetModel.getUuid());
        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        long dimensionCount = allNamedColumns.stream().filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(1L, dimensionCount);
        Assert.assertEquals(2, dataModel.getAllMeasures().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(dataModel.getUuid()).getAllLayouts().size());

        // transfer auto model to semi-auto
        // make model online
        MetadataTestUtils.toSemiAutoMode(getProject());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            dfManager.updateDataflowStatus(targetModel.getId(), RealizationStatusEnum.ONLINE);
            return true;
        }, project);

        // optimize with a batch of sql list
        List<String> li = Lists.newArrayList();
        li.add("select lstg_format_name, trans_id, count(item_count) from test_kylin_fact group by lstg_format_name, trans_id");
        li.add("select leaf_categ_id, count(seller_id) from test_kylin_fact group by leaf_categ_id");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, li, true, false);

        List<AbstractContext.ModelContext> modelContextList = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextList.size());
        SuggestionResponse suggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResponse.getReusedModels();
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResponse.getNewModels();
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        changeTheIndexRecOrder(reusedModelRequests);
        modelService.batchCreateModel(getProject(), newModelRequests, reusedModelRequests);

        // assert result after apply recommendations
        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(3L, dimensionCountRefreshed);
        List<NDataModel.Measure> allMeasures = modelAfterSuggestModel.getAllMeasures();
        Assert.assertEquals(4, allMeasures.size());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelAfterSuggestModel.getUuid());
        Assert.assertEquals(3, indexPlan.getAllLayouts().size());
        List<Integer> measureIds = allMeasures.stream().map(NDataModel.Measure::getId).collect(Collectors.toList());
        Assert.assertEquals("[100000, 100001, 100002, 100003]", measureIds.toString());

        // suggest again and assert result again
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select order_id, count(seller_id) from test_kylin_fact group by order_id");
        AbstractContext proposeContextSecond = modelSmartService.suggestModel(project, sqlList, true, true);
        List<AbstractContext.ModelContext> modelContextsTwice = proposeContextSecond.getModelContexts();
        Assert.assertEquals(1, modelContextsTwice.size());
        AbstractContext.ModelContext modelContextTwice = modelContextsTwice.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMapTwice = modelContextTwice.getIndexRexItemMap();
        Assert.assertEquals(1, indexRexItemMapTwice.size());
    }

    /**
     * https://olapio.atlassian.net/browse/KE-23783
     * layout1 depends on m2, layout2 depends on m1, m2.id > m1.id, layout2.id > layout1.id
     */
    private void changeTheIndexRecOrder(List<ModelRequest> reusedModelRequests) {
        ModelRequest modelRequest = reusedModelRequests.get(0);
        List<LayoutRecDetailResponse> recItems = modelRequest.getRecItems();
        recItems.sort((rec1, rec2) -> {
            List<Integer> measureList1 = rec1.getMeasures().stream().map(recMeasure -> recMeasure.getMeasure().getId())
                    .sorted().collect(Collectors.toList());
            List<Integer> measureList2 = rec2.getMeasures().stream().map(recMeasure -> recMeasure.getMeasure().getId())
                    .sorted().collect(Collectors.toList());

            return measureList2.get(measureList2.size() - 1) - measureList1.get(measureList1.size() - 1);
        });
        modelRequest.setRecItems(recItems);
    }

    @Test
    public void testTransferAndSaveRecommendations() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        String query1 = "select sum(item_count*price) from test_kylin_fact";
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { query1 }, true);

        // assertion of the model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        AbstractContext.ModelContext modelContext = modelContexts.get(0);
        NDataModel targetModel = modelContext.getTargetModel();
        List<RawRecItem> rawRecItemsBefore = jdbcRawRecStore.queryAll();
        Assert.assertTrue(rawRecItemsBefore.isEmpty());

        // mock propose with suggest model with saving recommendation to raw-rec-table
        MetadataTestUtils.toSemiAutoMode(getProject());
        String query2 = "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name";
        AbstractContext semiContextV2 = AccelerationUtil.genOptRec(getTestConfig(), getProject(),
                new String[] { query2 });
        rawRecService.transferAndSaveRecommendations(semiContextV2);

        // assert result
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(3, rawRecItems.size());
        List<RawRecItem> layoutRecs = rawRecItems.stream()
                .filter(item -> item.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT).collect(Collectors.toList());
        Assert.assertEquals(1, layoutRecs.size());
        Assert.assertEquals(RawRecItem.IMPORTED, layoutRecs.get(0).getRecSource());

        // assert the method of `queryImportedRawRecItems`
        List<RawRecItem> recItems = RawRecManager.getInstance(getProject()).queryImportedRawRecItems(getProject(),
                targetModel.getUuid());
        Assert.assertEquals(1, recItems.size());
        Assert.assertEquals(RawRecItem.IMPORTED, layoutRecs.get(0).getRecSource());
    }

    @Test
    public void testParallelTransferAndSaveRecommendations() throws InterruptedException {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        String query1 = "select sum(item_count*price) from test_kylin_fact";
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { query1 }, true);

        // assertion of the model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        AbstractContext.ModelContext modelContext = modelContexts.get(0);
        NDataModel targetModel = modelContext.getTargetModel();
        List<RawRecItem> rawRecItemsBefore = jdbcRawRecStore.queryAll();
        Assert.assertTrue(rawRecItemsBefore.isEmpty());

        // mock propose with suggest model with saving recommendation to raw-rec-table
        MetadataTestUtils.toSemiAutoMode(getProject());

        Runnable runnable = () -> {
            String query2 = "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name";
            AbstractContext semiContextV2 = AccelerationUtil.genOptRec(getTestConfig(), getProject(),
                    new String[] { query2 });
            rawRecService.transferAndSaveRecommendations(semiContextV2);
        };
        List<Thread> threads = new ArrayList<>();
        threads.add(new Thread(runnable));
        threads.add(new Thread(runnable));
        threads.add(new Thread(runnable));
        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            thread.join();
        }

        // assert result
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        System.out.println(StringUtils.join(rawRecItems));
        Assert.assertEquals(3, rawRecItems.size());
        List<RawRecItem> layoutRecs = rawRecItems.stream()
                .filter(item -> item.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT).collect(Collectors.toList());
        Assert.assertEquals(1, layoutRecs.size());
        Assert.assertEquals(RawRecItem.IMPORTED, layoutRecs.get(0).getRecSource());

        // assert the method of `queryImportedRawRecItems`
        List<RawRecItem> recItems = RawRecManager.getInstance(getProject()).queryImportedRawRecItems(getProject(),
                targetModel.getUuid());
        Assert.assertEquals(1, recItems.size());
        Assert.assertEquals(RawRecItem.IMPORTED, layoutRecs.get(0).getRecSource());
    }

    @Test
    public void testSuggestModelWithoutCreateNewModel() {
        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name" },
                true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeOptimization = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(12, modelBeforeOptimization.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeOptimization.getAllMeasures().size());
        IndexPlan indexPlanBeforeOptimization = modelContexts.get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlanBeforeOptimization.getAllLayouts().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // suggest model without create new model
        List<String> sqlList = ImmutableList.of(
                "select price, item_count from test_kylin_fact join edw.test_cal_dt "
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt group by price, item_count",
                "select lstg_format_name, item_count, count(item_count), sum(price) "
                        + "from test_kylin_fact group by lstg_format_name, item_count");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqlList, true, false);
        AccelerateInfo failedInfo = proposeContext.getAccelerateInfoMap().get(sqlList.get(0));
        Assert.assertTrue(failedInfo.isNotSucceed());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, failedInfo.getPendingMsg());

        rawRecService.transferAndSaveRecommendations(proposeContext);

        // assert result
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(3, rawRecItems.size());
        List<RawRecItem> layoutRecs = rawRecItems.stream()
                .filter(item -> item.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT).collect(Collectors.toList());
        Assert.assertEquals(1, layoutRecs.size());
        Assert.assertEquals(RawRecItem.IMPORTED, layoutRecs.get(0).getRecSource());
    }

    @Test
    public void testOptimizeNonEquivJoinModel() {
        overwriteSystemProp("kylin.query.print-logical-plan", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        String joinExpr = "\"TEST_ORDER\".\"ORDER_ID\" = \"TEST_KYLIN_FACT\".\"ORDER_ID\" "
                + "AND \"TEST_ORDER\".\"BUYER_ID\" >= \"TEST_KYLIN_FACT\".\"SELLER_ID\" "
                + "AND \"TEST_ORDER\".\"BUYER_ID\" < \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"";
        String sql = "select test_order.order_id,buyer_id from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "and buyer_id>=seller_id and buyer_id<leaf_categ_id " //
                + "group by test_order.order_id,buyer_id";
        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { sql }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeOptimization = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeOptimization.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeOptimization.getAllMeasures().size());
        IndexPlan indexPlanBeforeOptimization = modelContexts.get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlanBeforeOptimization.getAllLayouts().size());
        Assert.assertEquals(1, modelBeforeOptimization.getJoinTables().size());
        JoinTableDesc joinTable = modelBeforeOptimization.getJoinTables().get(0);
        NonEquiJoinCondition nonEquiJoinCondition = joinTable.getJoin().getNonEquiJoinCondition();
        Assert.assertEquals(joinExpr, nonEquiJoinCondition.getExpr());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // suggest model without create new model
        List<String> sqlList = ImmutableList.of("select test_order.order_id,test_date_enc from test_order "
                + "left join test_kylin_fact on test_order.order_id = test_kylin_fact.order_id "
                + "and buyer_id >= seller_id and buyer_id < leaf_categ_id " //
                + "group by test_order.order_id,test_date_enc");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqlList, true, false);
        rawRecService.transferAndSaveRecommendations(proposeContext);

        // assert result
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(2, rawRecItems.size());
        List<RawRecItem> layoutRecs = rawRecItems.stream()
                .filter(item -> item.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT).collect(Collectors.toList());
        Assert.assertEquals(1, layoutRecs.size());
        Assert.assertEquals(RawRecItem.IMPORTED, layoutRecs.get(0).getRecSource());
    }

    @Test
    public void testSuggestModel() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select price, sum(price+1) from test_kylin_fact group by price" }, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeOptimization = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(13, modelBeforeOptimization.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeOptimization.getAllMeasures().size());
        Assert.assertEquals(1, modelBeforeOptimization.getComputedColumnDescs().size());
        IndexPlan indexPlanBeforeOptimization = modelContexts.get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlanBeforeOptimization.getAllLayouts().size());

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // suggest model and verify result
        List<String> sqlList = ImmutableList.of(
                "select price, item_count, sum(price+1) from \"DEFAULT\".test_kylin_fact inner join edw.test_cal_dt "
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt group by price, item_count",
                "select lstg_format_name, item_count, sum(price+1), sum(price+2) "
                        + "from test_kylin_fact group by lstg_format_name, item_count");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqlList, true, true);
        SuggestionResponse suggestionResp = modelSmartService.buildModelSuggestionResponse(proposeContext);
        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResp.getReusedModels();
        Assert.assertEquals(1, reusedModels.size());
        SuggestionResponse.ModelRecResponse recommendedModelResponse = reusedModels.get(0);
        List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
        Assert.assertEquals(1, indexes.size());
        LayoutRecDetailResponse layoutRecResp1 = indexes.get(0);
        Assert.assertEquals(1, layoutRecResp1.getSqlList().size());
        Assert.assertTrue(layoutRecResp1.getSqlList().get(0).equalsIgnoreCase(sqlList.get(1)));
        Assert.assertEquals(2, layoutRecResp1.getDimensions().size());
        Assert.assertEquals(3, layoutRecResp1.getMeasures().size());
        Assert.assertEquals(1, layoutRecResp1.getComputedColumns().size());
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResp.getNewModels();
        Assert.assertEquals(1, newModels.size());
        SuggestionResponse.ModelRecResponse newModelResponse = newModels.get(0);
        List<LayoutRecDetailResponse> newModelIndexes = newModelResponse.getIndexes();
        Assert.assertEquals(1, newModelIndexes.size());
        LayoutRecDetailResponse layoutRecResp2 = newModelIndexes.get(0);
        Assert.assertEquals(1, layoutRecResp2.getSqlList().size());
        Assert.assertTrue(layoutRecResp2.getSqlList().get(0).equalsIgnoreCase(sqlList.get(0)));
        Assert.assertEquals(2, layoutRecResp2.getDimensions().size());
        Assert.assertEquals(2, layoutRecResp2.getMeasures().size());
        Assert.assertEquals(1, layoutRecResp2.getComputedColumns().size());

        // Mock modelRequest and save
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        modelService.batchCreateModel(getProject(), newModelRequests, reusedModelRequests);

        // assert model optimization result
        assertAfterResult(modelID, newModelResponse);
    }

    private void assertAfterResult(String modelID, SuggestionResponse.ModelRecResponse newModelResponse) {
        NDataModel reusedModelAfter = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(2, reusedModelAfter.getComputedColumnDescs().size());
        Assert.assertEquals(14, reusedModelAfter.getAllNamedColumns().size());
        Assert.assertEquals(3, reusedModelAfter.getAllMeasures().size());
        Assert.assertEquals(3, reusedModelAfter.getAllNamedColumns().stream()//
                .filter(NamedColumn::isDimension).count());
        IndexPlan indexPlanAfter = indexPlanManager.getIndexPlan(modelID);
        Assert.assertEquals(2, indexPlanAfter.getAllLayouts().size());
        String newModelID = newModelResponse.getIndexPlan().getUuid();
        NDataModel newModel = modelManager.getDataModelDesc(newModelID);
        Assert.assertEquals(1, newModel.getComputedColumnDescs().size());
        Assert.assertEquals(113, newModel.getAllNamedColumns().size());
        Assert.assertEquals(2, newModel.getAllMeasures().size());
        Assert.assertEquals(2, newModel.getAllNamedColumns().stream()//
                .filter(NamedColumn::isDimension).count());
        IndexPlan newIndexPlan = indexPlanManager.getIndexPlan(newModelID);
        Assert.assertEquals(1, newIndexPlan.getAllLayouts().size());
    }

    @Test
    public void testSuggestModelAddSameIndexDiffLayout() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "select count(*) from test_kylin_fact where price = 1 group by lstg_format_name" },
                true);

        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // suggest model, add same index but not same layout
        List<String> sqlList = ImmutableList
                .of("select count(*) from test_kylin_fact where lstg_format_name = '1' group by price");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqlList, true, true);
        SuggestionResponse suggestionResp = modelSmartService.buildModelSuggestionResponse(proposeContext);

        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResp.getReusedModels();
        Assert.assertEquals(1, reusedModels.size());
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResp.getNewModels();
        Assert.assertEquals(0, newModels.size());
        // Mock modelRequest and save
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        modelService.batchCreateModel(getProject(), newModelRequests, reusedModelRequests);

        IndexPlan newIndexPlan = indexPlanManager.getIndexPlan(modelID);
        Assert.assertEquals(2, newIndexPlan.getAllLayouts().size());
    }

    private List<ModelRequest> mockModelRequest(List<SuggestionResponse.ModelRecResponse> modelResponses) {
        List<ModelRequest> modelRequestList = Lists.newArrayList();
        modelResponses.forEach(model -> {
            ModelRequest modelRequest = new ModelRequest();
            modelRequest.setUuid(model.getUuid());
            modelRequest.setJoinTables(model.getJoinTables());
            modelRequest.setJoinsGraph(model.getJoinsGraph());
            modelRequest.setFactTableRefs(model.getFactTableRefs());
            modelRequest.setAllTableRefs(model.getAllTableRefs());
            modelRequest.setLookupTableRefs(model.getLookupTableRefs());
            modelRequest.setTableNameMap(model.getTableNameMap());
            modelRequest.setRootFactTableName(model.getRootFactTableName());
            modelRequest.setRootFactTableAlias(model.getRootFactTableAlias());
            modelRequest.setRootFactTableRef(model.getRootFactTableRef());
            modelRequest.setModelType(model.getModelType());

            modelRequest.setIndexPlan(model.getIndexPlan());
            modelRequest.setAllNamedColumns(model.getAllNamedColumns());
            modelRequest.setAllMeasures(model.getAllMeasures());
            modelRequest.setComputedColumnDescs(model.getComputedColumnDescs());
            modelRequest.setRecItems(model.getIndexes());

            modelRequest.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                    .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList()));
            modelRequest.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                    .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
            modelRequest.setSimplifiedJoinTableDescs(
                    SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(model.getJoinTables()));
            modelRequest.setAlias(model.getAlias());
            modelRequest.setManagementType(model.getManagementType());
            modelRequestList.add(modelRequest);
        });
        return modelRequestList;
    }

    private void changeRecItemState(List<RawRecItem> recItems, RawRecItem.RawRecState state) {
        recItems.forEach(recItem -> recItem.setState(state));
        jdbcRawRecStore.batchAddOrUpdate(recItems);
    }

    private long getFilterRecCount(List<RawRecItem> rawRecItems, RawRecItem.RawRecType type) {
        return rawRecItems.stream().filter(item -> item.getType() == type).count();
    }

    private OptRecRequest mockOptRecRequest(String modelID, OptRecDetailResponse optRecDetailResponse) {
        Map<Integer, String> userDefinedNameMap = Maps.newHashMap();
        optRecDetailResponse.getCcItems().stream().filter(OptRecDepResponse::isAdd).forEach(item -> {
            int itemId = (int) item.getItemId();
            String name = item.getName();
            userDefinedNameMap.put(itemId, name.substring(name.indexOf('.') + 1));
        });
        optRecDetailResponse.getDimensionItems().stream().filter(OptRecDepResponse::isAdd).forEach(item -> {
            int itemId = (int) item.getItemId();
            String name = item.getName();
            userDefinedNameMap.put(itemId, name.substring(name.indexOf('.') + 1));
        });
        optRecDetailResponse.getMeasureItems().stream().filter(OptRecDepResponse::isAdd).forEach(item -> {
            int itemId = (int) item.getItemId();
            userDefinedNameMap.put(itemId, item.getName());
        });
        OptRecRequest recRequest = new OptRecRequest();
        recRequest.setProject(getProject());
        recRequest.setModelId(modelID);
        recRequest.setRecItemsToAddLayout(optRecDetailResponse.getRecItemsToAddLayout());
        recRequest.setRecItemsToRemoveLayout(optRecDetailResponse.getRecItemsToRemoveLayout());
        recRequest.setNames(userDefinedNameMap);
        return recRequest;
    }

    private static List<QueryMetrics> loadQueryHistoryList(String queryHistoryJsonFilePath) throws IOException {
        List<QueryMetrics> allQueryMetrics = Lists.newArrayList();
        File directory = new File(queryHistoryJsonFilePath);
        File[] files = directory.listFiles();
        for (File file : Objects.requireNonNull(files)) {
            String recItemContent = FileUtils.readFileToString(file, Charset.defaultCharset());
            allQueryMetrics.addAll(parseQueryMetrics(recItemContent));
        }
        return allQueryMetrics;
    }

    private static List<QueryMetrics> parseQueryMetrics(String recItemContent) throws IOException {
        List<QueryMetrics> recItems = Lists.newArrayList();
        JsonNode jsonNode = JsonUtil.readValueAsTree(recItemContent);
        final Iterator<JsonNode> elements = jsonNode.elements();
        while (elements.hasNext()) {
            JsonNode recItemNode = elements.next();
            QueryMetrics item = parseQueryMetrics(recItemNode);
            recItems.add(item);
        }
        return recItems;
    }

    private static QueryMetrics parseQueryMetrics(JsonNode recItemNode) throws IOException {
        String queryId = recItemNode.get("query_id").asText();
        String server = recItemNode.get("server").asText();
        QueryMetrics queryMetrics = new QueryMetrics(queryId, server);
        queryMetrics.setId(recItemNode.get("id").asInt());
        queryMetrics.setSql(recItemNode.get("sql_text").asText());
        queryMetrics.setSqlPattern(recItemNode.get("sql_pattern").asText());
        queryMetrics.setQueryDuration(recItemNode.get("duration").asInt());
        queryMetrics.setTotalScanBytes(recItemNode.get("total_scan_bytes").asInt());
        queryMetrics.setTotalScanCount(recItemNode.get("total_scan_count").asInt());
        queryMetrics.setResultRowCount(recItemNode.get("result_row_count").asInt());
        queryMetrics.setSubmitter(recItemNode.get("submitter").asText());
        queryMetrics.setServer(recItemNode.get("server").asText());
        queryMetrics.setErrorType(recItemNode.get("error_type").asText());
        queryMetrics.setEngineType(recItemNode.get("engine_type").asText());
        queryMetrics.setCacheHit(recItemNode.get("cache_hit").asBoolean());
        queryMetrics.setQueryStatus(recItemNode.get("query_status").asText());
        queryMetrics.setIndexHit(recItemNode.get("index_hit").asBoolean());
        queryMetrics.setQueryTime(recItemNode.get("query_time").asLong());
        queryMetrics.setMonth(recItemNode.get("month").asText());
        queryMetrics.setQueryFirstDayOfMonth(recItemNode.get("query_first_day_of_month").asLong());
        queryMetrics.setQueryFirstDayOfWeek(recItemNode.get("query_first_day_of_week").asLong());
        queryMetrics.setQueryDay(recItemNode.get("query_day").asLong());
        queryMetrics.setTableIndexUsed(recItemNode.get("is_table_index_used").asBoolean());
        queryMetrics.setAggIndexUsed(recItemNode.get("is_agg_index_used").asBoolean());
        queryMetrics.setTableSnapshotUsed(recItemNode.get("is_table_snapshot_used").asBoolean());
        queryMetrics.setProjectName(recItemNode.get("project_name").asText());
        String queryHistoryInfoStr = recItemNode.get("reserved_field_3").asText();
        QueryHistoryInfo queryHistoryInfo = JsonUtil.readValue(queryHistoryInfoStr, QueryHistoryInfo.class);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
        return queryMetrics;
    }

    @Test
    public void testSuggestStreamingModel() {
        String project = "streaming_test";

        // optimize with a batch of sql list
        List<String> li = Lists.newArrayList();
        li.add("SELECT sum(LO_CUSTKEY) from SSB.P_LINEORDER_STR group by LO_CUSTKEY");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, li, false, true);
        List<AbstractContext.ModelContext> modelContextList = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextList.size());
        Assert.assertEquals(NDataModel.ModelType.STREAMING, modelContextList.get(0).getTargetModel().getModelType());

        String modelID = modelContextList.get(0).getTargetModel().getUuid();
        SuggestionResponse suggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResponse.getReusedModels();
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResponse.getNewModels();
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        modelService.batchCreateModel(project, newModelRequests, reusedModelRequests);

        val jobManager = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = jobManager.getStreamingJobByUuid(modelID + "_build");
        Assert.assertNotNull(job);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelID);
        Assert.assertEquals(0, dataflow.getSegments().size());

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelMgr.getDataModelDesc(modelID);
        Assert.assertEquals(2, model.getAllMeasures().size());
        Assert.assertEquals(19, model.getAllNamedColumns().size());
        Assert.assertEquals(NDataModel.ColumnStatus.DIMENSION, model.getAllNamedColumns().get(1).getStatus());
    }

    @Test
    public void testSuggestStreamingModelDisableBaseIndex() {
        String project = "streaming_test";
        // optimize with a batch of sql list
        List<String> li = Lists.newArrayList();
        li.add("SELECT sum(LO_CUSTKEY) from SSB.P_LINEORDER_STR group by LO_CUSTKEY");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, li, false, true);
        List<AbstractContext.ModelContext> modelContextList = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextList.size());
        Assert.assertEquals(NDataModel.ModelType.STREAMING, modelContextList.get(0).getTargetModel().getModelType());

        String modelID = modelContextList.get(0).getTargetModel().getUuid();
        SuggestionResponse suggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResponse.getReusedModels();
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResponse.getNewModels();
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        newModelRequests.forEach(request -> request.setWithBaseIndex(true));
        modelService.batchCreateModel(project, newModelRequests, reusedModelRequests);
        val indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getIndexPlan(modelID);
        Assert.assertFalse(indexPlan.containBaseTableLayout());
        Assert.assertFalse(indexPlan.containBaseAggLayout());
    }

    @Test
    public void testRecommendationWhenTableIndexAnswerSelectStarIsTrue() throws InterruptedException {
        overwriteSystemProp("kylin.query.cache-enabled", "false");
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] {
                "select LEAF_CATEG_ID,LSTG_SITE_ID,ITEM_COUNT,ORDER_ID,CAL_DT,LSTG_FORMAT_NAME from test_kylin_fact" },
                true);
        buildAllModels(getTestConfig(), getProject());

        overwriteSystemProp("kylin.query.use-tableindex-answer-select-star.enabled", "true");

        MetadataTestUtils.toSemiAutoMode(getProject());
        String[] sqls = { "select price from test_kylin_fact group by price" };
        val context = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sqls);
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        AbstractContext.ModelContext context1 = modelContexts.get(0);
        Assert.assertEquals("AUTO_MODEL_TEST_KYLIN_FACT_1", context1.getTargetModel().getAlias());
        Assert.assertEquals(1, context1.getDimensionRecItemMap().size());
        Assert.assertEquals(1, context1.getIndexRexItemMap().size());
    }
}
