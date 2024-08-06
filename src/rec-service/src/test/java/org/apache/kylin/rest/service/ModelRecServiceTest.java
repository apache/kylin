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

package org.apache.kylin.rest.service;

import static org.awaitility.Awaitility.await;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.service.JobInfoService;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.model.util.scd2.SCD2CondChecker;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.QueryTimesResponse;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ProposerJob;
import org.apache.kylin.rec.SmartMaster;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.feign.MetadataInvoker;
import org.apache.kylin.rest.feign.SmartInvoker;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.LayoutRecDetailResponse;
import org.apache.kylin.rest.response.OpenAccSqlResponse;
import org.apache.kylin.rest.response.OpenModelRecResponse;
import org.apache.kylin.rest.response.OpenSuggestionResponse;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class ModelRecServiceTest extends SourceTestCase {
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());
    @InjectMocks
    private final ModelSmartService modelSmartService = Mockito.spy(new ModelSmartService());
    @InjectMocks
    private final ProjectSmartService projectSmartService = Mockito.spy(new ProjectSmartService());
    @InjectMocks
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());
    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());
    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());
    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());
    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());
    @InjectMocks
    private final ModelRecService modelRecService = Mockito.spy(new ModelRecService());
    @InjectMocks
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @InjectMocks
    private final JobInfoService jobInfoService = Mockito.spy(JobInfoService.class);
    @InjectMocks
    private final JobInfoDao jobInfoDao = Mockito.spy(JobInfoDao.class);
    @InjectMocks
    private final JobInfoMapper jobInfoMapper = Mockito.spy(JobInfoMapper.class);
    @InjectMocks
    private final RawRecService rawRecService = Mockito.spy(new RawRecService());
    @InjectMocks
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    MetadataInvoker metadataInvoker = Mockito.spy(new MetadataInvoker());
    @Spy
    private final AclTCRServiceSupporter aclTCRService = Mockito.spy(AclTCRServiceSupporter.class);
    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Spy
    private final AccessService accessService = Mockito.spy(AccessService.class);
    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();
    @Spy
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private final StreamingJobListener eventListener = new StreamingJobListener();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = PushDownUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.createAclInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));
        SmartInvoker.getInstance().setDelegate(modelSmartService);

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
        EventBusFactory.getInstance().register(modelBrokenListener, false);
    }

    @Override
    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testOptimizeModelNeedMergeIndex() {
        String project = "newten";

        // prepare initial model
        String sql = "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact "
                + "where cal_dt = '2012-01-02' group by lstg_format_name, cal_dt";
        AbstractContext smartContext = ProposerJob.proposeForAutoMode(getTestConfig(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();

        // assert initial result
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(targetModel.getUuid());
        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        long dimensionCount = allNamedColumns.stream().filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCount);
        Assert.assertEquals(2, dataModel.getAllMeasures().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(dataModel.getUuid()).getAllLayouts().size());

        // transfer auto model to semi-auto
        // make model online
        transferProjectToSemiAutoMode(getTestConfig(), project);
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        dfManager.updateDataflowStatus(targetModel.getId(), RealizationStatusEnum.ONLINE);

        // optimize with a batch of sql list
        List<String> sqlList = Lists.newArrayList();
        sqlList.add(sql);
        sqlList.add("select lstg_format_name, cal_dt, sum(price) from test_kylin_fact "
                + "where lstg_format_name = 'USA' group by lstg_format_name, cal_dt");
        sqlList.add("select lstg_format_name, cal_dt, count(item_count) from test_kylin_fact "
                + "where cal_dt = '2012-01-02' group by lstg_format_name, cal_dt");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, true, true);

        // assert optimization result
        List<AbstractContext.ModelContext> modelContextsAfterOptimization = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContextsAfterOptimization.size());
        AbstractContext.ModelContext modelContextAfterOptimization = modelContextsAfterOptimization.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMap = modelContextAfterOptimization.getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMap.size()); // if no merge, the result will be 3.

        // apply recommendations
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        modelRecService.saveRecResult(modelSuggestionResponse, project, null);

        // assert result after apply recommendations
        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCountRefreshed);
        Assert.assertEquals(3, modelAfterSuggestModel.getAllMeasures().size());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelAfterSuggestModel.getUuid());
        Assert.assertEquals(3, indexPlan.getAllLayouts().size());

        // remove proposed indexes
        indexPlan.getAllLayouts().forEach(l -> indexPlanService.removeIndex(project, targetModel.getUuid(), l.getId()));
        IndexPlan indexPlanRefreshed = indexPlanManager.getIndexPlan(targetModel.getUuid());
        Assert.assertTrue(indexPlanRefreshed.getAllLayouts().isEmpty());

        // suggest again and assert result again
        AbstractContext proposeContextSecond = modelSmartService.suggestModel(project, sqlList, true, true);
        List<AbstractContext.ModelContext> modelContextsTwice = proposeContextSecond.getModelContexts();
        Assert.assertEquals(1, modelContextsTwice.size());
        AbstractContext.ModelContext modelContextTwice = modelContextsTwice.get(0);
        Map<String, LayoutRecItemV2> indexRexItemMapTwice = modelContextTwice.getIndexRexItemMap();
        Assert.assertEquals(2, indexRexItemMapTwice.size());
    }

    @Test
    public void testSuggestModelWithSimpleQuery() {
        String project = "newten";
        transferProjectToSemiAutoMode(getTestConfig(), project);

        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select floor(date'2020-11-17' TO day), ceil(date'2020-11-17' TO day) from test_kylin_fact");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, false, true);
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        modelRecService.saveRecResult(modelSuggestionResponse, project, null);

        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();
        long dimensionCountRefreshed = targetModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(1L, dimensionCountRefreshed);
        Assert.assertEquals(1, targetModel.getAllMeasures().size());
    }

    @Test
    public void testSuggestOrOptimizeModels() {
        String project = "newten";
        // prepare initial model
        AbstractContext smartContext = ProposerJob.proposeForAutoMode(getTestConfig(), project,
                new String[] { "select price from test_kylin_fact" });
        UnitOfWork.doInTransactionWithRetry(() -> {
            smartContext.saveMetadata();
            return true;
        }, project);
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel targetModel = modelContexts.get(0).getTargetModel();

        UnitOfWork.doInTransactionWithRetry(() -> {
            transferProjectToSemiAutoMode(getTestConfig(), project);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            dfManager.updateDataflowStatus(targetModel.getId(), RealizationStatusEnum.ONLINE);
            return true;
        }, project);

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(targetModel.getUuid());
        List<NDataModel.NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
        long dimensionCount = allNamedColumns.stream().filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(1L, dimensionCount);
        Assert.assertEquals(1, dataModel.getAllMeasures().size());

        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, true, true);
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        modelRecService.saveRecResult(modelSuggestionResponse, project, null);

        NDataModel modelAfterSuggestModel = modelManager.getDataModelDesc(targetModel.getUuid());
        long dimensionCountRefreshed = modelAfterSuggestModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).count();
        Assert.assertEquals(2L, dimensionCountRefreshed);
        Assert.assertEquals(2, modelAfterSuggestModel.getAllMeasures().size());
    }

    @Test
    public void testAccSqlWithStrategy() {
        String project = "newten";
        String strategy = "single_dim_and_reduce_hc";
        String sql1 = "select trans_id, order_id, cal_dt, seller_id, sum(price) "
                + "from test_kylin_fact group by trans_id, order_id, cal_dt, seller_id";
        String sql2 = "select trans_id, order_id, cal_dt, lstg_format_name, max(price) "
                + "from test_kylin_fact group by trans_id, order_id, cal_dt, lstg_format_name";
        val request = smartRequest(project, Lists.newArrayList(sql1, sql2));
        request.setAcceptRecommendationStrategy(strategy);
        request.setForce2CreateNewModel(false);
        request.setWithBaseIndex(true);

        //create new model
        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        List<IndexEntity> indexes = NIndexPlanManager.getInstance(getTestConfig(), project)
                .getIndexPlan(normalResponse.getCreatedModels().get(0).getUuid()).getIndexes();
        Assert.assertEquals(10, indexes.size());
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
    }

    @Test
    public void testAccSqlWithStrategyReduceHc() {
        String project = "newten";

        // set table_ext
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NTableMetadataManager manager = NTableMetadataManager.getInstance(config, project);
            TableExtDesc ext = manager.getOrCreateTableExt("DEFAULT.TEST_ORDER");
            ext.setTotalRows(5000L);
            setColumnStats(ext, "ORDER_ID", 4671L);
            setColumnStats(ext, "BUYER_ID", 946L);
            setColumnStats(ext, "TEST_DATE_ENC", 1810L);
            setColumnStats(ext, "TEST_TIME_ENC", 4830L);
            setColumnStats(ext, "TEST_EXTENDED_COLUMN", 957L);
            manager.saveOrUpdateTableExt(false, ext);
            return null;
        }, project);

        String strategy = "single_dim_and_reduce_hc";
        String sql1 = "SELECT TEST_TIME_ENC, TEST_DATE_ENC, BUYER_ID, MAX(ORDER_ID) "
                + "FROM TEST_ORDER GROUP BY TEST_TIME_ENC, TEST_DATE_ENC, BUYER_ID";
        String sql2 = "SELECT TEST_TIME_ENC, BUYER_ID, MIN(ORDER_ID) "
                + "FROM TEST_ORDER GROUP BY TEST_TIME_ENC, BUYER_ID";
        val request = smartRequest(project, Lists.newArrayList(sql1, sql2));
        request.setAcceptRecommendationStrategy(strategy);
        request.setForce2CreateNewModel(false);
        request.setWithBaseIndex(true);

        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        List<IndexEntity> indexes = NIndexPlanManager.getInstance(getTestConfig(), project)
                .getIndexPlan(normalResponse.getCreatedModels().get(0).getUuid()).getIndexes();
        Assert.assertEquals(4, indexes.size());

        boolean isOnlyOneDim = indexes.stream().filter(index -> !index.isTableIndex())
                .allMatch(index -> ((index.getDimensions().size() == 1) && (index.getDimensions().get(0) == 0)));
        Assert.assertTrue(isOnlyOneDim);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
    }

    private void setColumnStats(TableExtDesc ext, String colName, long cardinality) {
        TableExtDesc.ColumnStats stats = new TableExtDesc.ColumnStats();
        stats.setColumnName(colName);
        stats.setCardinality(cardinality);
        ext.getAllColumnStats().add(stats);
    }

    public static void transferProjectToSemiAutoMode(KylinConfig kylinConfig, String project) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(project, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    @Test
    public void testOptimizeModel_Twice() {
        String project = "newten";
        val indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);

        Function<OpenSqlAccelerateRequest, OpenSqlAccelerateRequest> rewriteReq = req -> {
            req.setForce2CreateNewModel(false);
            return req;
        };
        String normSql = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        OpenSuggestionResponse normalResponse = modelRecService.suggestOrOptimizeModels(smartRequest(project, normSql));
        Assert.assertEquals(1, normalResponse.getModels().size());

        normSql = "select test_order.order_id,sum(price) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id";
        normalResponse = modelRecService.suggestOrOptimizeModels(rewriteReq.apply(smartRequest(project, normSql)));
        Assert.assertEquals(1, normalResponse.getModels().size());

        normSql = "select test_order.order_id,buyer_id,max(price) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id,LSTG_FORMAT_NAME";
        normalResponse = modelRecService.suggestOrOptimizeModels(rewriteReq.apply(smartRequest(project, normSql)));

        Assert.assertEquals(3,
                indexMgr.getIndexPlan(normalResponse.getModels().get(0).getUuid()).getAllLayouts().size());
    }

    @Test
    public void testAccSql() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        //create new model
        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
        Assert.assertEquals(0, normalResponse.getOptimizedModels().size());

        //create new model and add advice for model
        String sql2 = "select max(buyer_id) from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id";
        val request2 = smartRequest(project, sql2);
        String sql3 = "select max(order_id) from test_order";
        request2.getSqls().add(sql3);
        request2.setForce2CreateNewModel(false);
        normalResponse = modelRecService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
        Assert.assertEquals(1, normalResponse.getOptimizedModels().size());

        //acc again, due to model online, so have no impact
        normalResponse = modelRecService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(0, normalResponse.getCreatedModels().size());
        Assert.assertEquals(0, normalResponse.getOptimizedModels().size());
        Assert.assertEquals(2, normalResponse.getErrorSqlList().size());

        // acc again, due to model online and withOptimalModel=true, so have optimalModels and no error sql
        request2.setWithOptimalModel(true);
        normalResponse = modelRecService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(0, normalResponse.getCreatedModels().size());
        Assert.assertEquals(0, normalResponse.getErrorSqlList().size());
        Assert.assertEquals(2, normalResponse.getOptimalModels().size());
    }

    @Test
    public void testErrorAndConstantOptimalModelResponse() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        String sql2 = "select 1";
        String sql3 = "select select";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        request.setWithOptimalModel(true);
        request.getSqls().add(sql2);
        request.getSqls().add(sql3);
        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());
        Assert.assertEquals(1, normalResponse.getErrorSqlList().size());
        Assert.assertEquals(1, normalResponse.getOptimalModels().size());
        OpenModelRecResponse openModelRecResponse = normalResponse.getOptimalModels().get(0);
        Assert.assertEquals("CONSTANT", openModelRecResponse.getAlias());
    }

    @Test
    public void testOptimizeModelWithOptimalSql() {
        String project = "newten";
        String sql1 = "select order_id,buyer_id from test_order group by order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setWithOptimalModel(true);
        OpenSuggestionResponse openSuggestionResponse = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertEquals(1, openSuggestionResponse.getModels().size());
        Assert.assertEquals(0, openSuggestionResponse.getOptimalModels().size());
        String sql2 = "select 1";
        String sql3 = "select select";
        String sql4 = "select order_id, count(1) from test_order group by order_id";
        request.setForce2CreateNewModel(false);
        request.getSqls().add(sql2);
        request.getSqls().add(sql3);
        request.getSqls().add(sql4);
        OpenSuggestionResponse openSuggestionResponse1 = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertEquals(1, openSuggestionResponse1.getErrorSqlList().size());
        Assert.assertEquals(2, openSuggestionResponse1.getOptimalModels().size());
        Assert.assertEquals(1, openSuggestionResponse1.getModels().size());
        Assert.assertEquals("CONSTANT", openSuggestionResponse1.getOptimalModels().get(0).getAlias());
        Assert.assertEquals(sql3, openSuggestionResponse1.getErrorSqlList().get(0));
        Assert.assertEquals(openSuggestionResponse.getModels().get(0).getAlias(),
                openSuggestionResponse1.getModels().get(0).getAlias());
    }

    @Test
    public void testOptimizeModelWithProposingJoin() {
        String project = "newten";
        NProjectManager projectMgr = NProjectManager.getInstance(getTestConfig());
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);

        // create a base model
        String normSql = "select test_order.order_id,buyer_id from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        OpenSuggestionResponse normalResponse = modelRecService.suggestOrOptimizeModels(smartRequest(project, normSql));
        Assert.assertEquals(1, normalResponse.getModels().size());
        String modelId = normalResponse.getModels().get(0).getUuid();
        final NDataModel model1 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(1, model1.getJoinTables().size());
        Assert.assertEquals(17, model1.getAllNamedColumns().size());

        // without proposing new join, accelerate failed
        normSql = "select test_order.order_id,sum(price) from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "left join edw.test_cal_dt on test_kylin_fact.cal_dt=test_cal_dt.cal_dt "
                + "group by test_order.order_id";
        OpenSqlAccelerateRequest request1 = smartRequest(project, normSql);
        request1.setForce2CreateNewModel(false);
        normalResponse = modelRecService.suggestOrOptimizeModels(request1);
        Assert.assertEquals(0, normalResponse.getModels().size());
        Assert.assertEquals(1, normalResponse.getErrorSqlList().size());

        // with proposing new join, accelerate success
        projectMgr.updateProject(project, copyForWrite -> {
            copyForWrite.getConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        });
        OpenSqlAccelerateRequest request2 = smartRequest(project, normSql);
        request2.setForce2CreateNewModel(false);
        normalResponse = modelRecService.suggestOrOptimizeModels(request2);
        Assert.assertEquals(1, normalResponse.getModels().size());
        NDataModel model2 = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(2, model2.getJoinTables().size());
        Assert.assertEquals(117, model2.getAllNamedColumns().size());

        // proposing new index
        normSql = "select test_order.order_id,buyer_id,max(price) from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id,LSTG_FORMAT_NAME";
        OpenSqlAccelerateRequest request3 = smartRequest(project, normSql);
        request3.setForce2CreateNewModel(false);
        normalResponse = modelRecService.suggestOrOptimizeModels(request3);
        Assert.assertEquals(1, normalResponse.getModels().size());
        Assert.assertEquals(3, indexMgr.getIndexPlan(modelId).getAllLayouts().size());

        OpenSqlAccelerateRequest request4 = smartRequest(project, normSql);
        request4.setForce2CreateNewModel(true);
        SuggestionResponse suggestionResponse = modelRecService.suggestOptimizeModels(request4, true);
        Assert.assertEquals(1, suggestionResponse.getNewModels().size());
        Assert.assertEquals(0, suggestionResponse.getOptimalModels().size());
    }

    @Test
    public void testModelNonEquiJoinBrokenRepair() {
        /* 1.create scd2 model
         * 2.turn off scd2 configuration
         * 3.unload fact table , model become broken
         * 4.reload the fact table, model should be offline when model is scd2 and scd2 is turned off
         */
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "true");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        String project = "newten";
        transferProjectToSemiAutoMode(getTestConfig(), project);
        String scd2Sql = "select test_order.order_id,buyer_id from test_order "
                + "left join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "and buyer_id>=seller_id and buyer_id<leaf_categ_id " //
                + "group by test_order.order_id,buyer_id";
        val scd2Response = modelRecService.suggestOrOptimizeModels(smartRequest(project, scd2Sql));

        String normSql = "select test_order.order_id,buyer_id from test_order "
                + " join test_kylin_fact on test_order.order_id=test_kylin_fact.order_id "
                + "group by test_order.order_id,buyer_id";
        val normalResponse = modelRecService.suggestOrOptimizeModels(smartRequest(project, normSql));

        String nonEquivModelId = scd2Response.getModels().get(0).getUuid();
        String normalModelId = normalResponse.getModels().get(0).getUuid();

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel scd2Model = modelManager.getDataModelDesc(nonEquivModelId);
        NDataModel normalModel = modelManager.getDataModelDesc(normalModelId);
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(scd2Model, project));
        Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(normalModel, project));
        Assert.assertTrue(SCD2CondChecker.INSTANCE.isScd2Model(scd2Model));

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), project);
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        tableDesc.setMvcc(-1);

        // online -> broken
        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);
        NDataModel nonEquivOnline2Broken = modelManager.getDataModelDesc(nonEquivModelId);
        NDataModel normalOnline2Broken = modelManager.getDataModelDesc(normalModelId);
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, convertModelStatus(nonEquivOnline2Broken, project));
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, convertModelStatus(normalOnline2Broken, project));

        // broken -> repair
        TableExtDesc orCreateTableExt = tableMetadataManager.getOrCreateTableExt(tableDesc);
        tableExtService.loadTable(tableDesc, orCreateTableExt, project);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            NDataModel nonEquivBroken2Repair = modelManager.getDataModelDesc(nonEquivModelId);
            NDataModel normalBroken2Repair = modelManager.getDataModelDesc(nonEquivModelId);
            Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(nonEquivBroken2Repair, project));
            Assert.assertEquals(ModelStatusToDisplayEnum.WARNING, convertModelStatus(normalBroken2Repair, project));
        });
    }

    private OpenSqlAccelerateRequest smartRequest(String project, String sql) {
        return smartRequest(project, Lists.newArrayList(sql));
    }

    private OpenSqlAccelerateRequest smartRequest(String project, List<String> sqls) {
        OpenSqlAccelerateRequest scd2Request = new OpenSqlAccelerateRequest();
        scd2Request.setProject(project);
        scd2Request.setSqls(sqls);
        scd2Request.setAcceptRecommendation(true);
        scd2Request.setForce2CreateNewModel(true);
        scd2Request.setWithEmptySegment(true);
        scd2Request.setWithModelOnline(true);
        return scd2Request;
    }

    private ModelStatusToDisplayEnum convertModelStatus(NDataModel model, String project) {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        long inconsistentSegmentCount = dataflowManager.getDataflow(model.getUuid())
                .getSegments(SegmentStatusEnum.WARNING).size();
        return modelService.convertModelStatusToDisplay(model, model.getProject(), inconsistentSegmentCount);
    }

    @Test
    public void testOptimizedModelWithModelViewSql() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order group by test_order.order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());

        // use model view sql, can suggest optimized model
        getTestConfig().setProperty("kylin.query.auto-model-view-enabled", "true");
        String sql2 = String.format("select order_id from %s.%s group by order_id", project,
                normalResponse.getCreatedModels().get(0).getAlias());
        val request2 = smartRequest(project, sql2);
        request2.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse1 = modelRecService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(1, normalResponse1.getOptimizedModels().size());
        Assert.assertEquals(normalResponse.getCreatedModels().get(0).getAlias(),
                normalResponse1.getOptimizedModels().get(0).getAlias());
    }

    @Test
    public void testOptimizedModelWithJoinViewModel() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order group by test_order.order_id,buyer_id";
        String sql2 = "select order_id,seller_id from test_kylin_fact group by order_id,seller_id";
        val request = smartRequest(project, sql1);
        request.getSqls().add(sql2);
        request.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        Assert.assertEquals(2, normalResponse.getCreatedModels().size());

        // use two view model join sql, can suggest two optimized model
        getTestConfig().setProperty("kylin.query.auto-model-view-enabled", "true");
        String sql3 = String.format(
                "select * from (select order_id as a from %s.%s group by order_id ) t1 join"
                        + " (select order_id as b from %s.%s group by order_id) t2 on t1.a = t2.b",
                project, normalResponse.getCreatedModels().get(0).getAlias(), project,
                normalResponse.getCreatedModels().get(1).getAlias());
        val request2 = smartRequest(project, sql3);
        request2.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse1 = modelRecService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(2, normalResponse1.getOptimizedModels().size());
        Assert.assertEquals(normalResponse1.getOptimizedModels().get(0).getAlias(),
                normalResponse.getCreatedModels().get(0).getAlias());
        Assert.assertEquals(normalResponse1.getOptimizedModels().get(1).getAlias(),
                normalResponse.getCreatedModels().get(1).getAlias());
    }

    @Test
    public void testOptimizedModelWithCloneViewModel() {
        String project = "newten";
        String sql1 = "select test_order.order_id,buyer_id from test_order group by test_order.order_id,buyer_id";
        val request = smartRequest(project, sql1);
        request.setForce2CreateNewModel(false);
        OpenAccSqlResponse normalResponse = modelRecService.suggestAndOptimizeModels(request);
        Assert.assertEquals(1, normalResponse.getCreatedModels().size());

        String sql2 = "select order_id,seller_id from test_kylin_fact group by order_id,seller_id";
        request.getSqls().add(sql2);
        request.setForce2CreateNewModel(true);
        OpenAccSqlResponse normalResponse1 = modelRecService.suggestAndOptimizeModels(request);
        Assert.assertEquals(2, normalResponse1.getCreatedModels().size());
        Optional<OpenModelRecResponse> test_order = normalResponse1.getCreatedModels().stream()
                .filter(e -> e.getAlias().contains("TEST_ORDER")).findAny();
        Assert.assertTrue(test_order.isPresent());

        getTestConfig().setProperty("kylin.query.auto-model-view-enabled", "true");
        String sql3 = String.format(
                "select * from (select order_id as a from %s.%s group by order_id ) t1 join"
                        + " (select order_id as b from %s.%s group by order_id) t2 on t1.a = t2.b",
                project, normalResponse.getCreatedModels().get(0).getAlias(), project, test_order.get().getAlias());
        String sql4 = "select seller_id from test_kylin_fact group by seller_id";
        String sql5 = "select test_order.order_id from test_order group by test_order.order_id";
        val request2 = smartRequest(project, sql3);
        request2.setForce2CreateNewModel(false);
        request2.getSqls().add(sql4);
        request2.getSqls().add(sql5);
        OpenAccSqlResponse normalResponse2 = modelRecService.suggestAndOptimizeModels(request2);
        Assert.assertEquals(3, normalResponse2.getOptimizedModels().size());
        Assert.assertEquals(normalResponse2.getOptimizedModels().get(0).getAlias(),
                normalResponse1.getCreatedModels().get(0).getAlias());
        Assert.assertEquals(normalResponse2.getOptimizedModels().get(1).getAlias(),
                normalResponse1.getCreatedModels().get(1).getAlias());
        Assert.assertEquals(normalResponse2.getOptimizedModels().get(2).getAlias(),
                normalResponse.getCreatedModels().get(0).getAlias());
    }

    @Test
    public void testBaseIndexWithTableIndexDisabled() {
        String project = "default";
        String sql1 = "select max(LO_LINENUMBER) from LINEORDER";
        String sql2 = "select LO_ORDTOTALPRICE, LO_ORDERPRIOTITY, LO_EXTENDEDPRICE, LO_TAX from LINEORDER";
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());
        Assert.assertEquals(2, indexPlan.getAllLayouts().stream().filter(LayoutEntity::isBase).count());

        OpenSqlAccelerateRequest request = new OpenSqlAccelerateRequest();
        request.setProject(project);
        request.setSqls(Arrays.asList(sql1, sql2));
        request.setForce2CreateNewModel(false);
        request.setAcceptRecommendation(true);
        request.setDiscardTableIndex(true);
        OpenSuggestionResponse openSuggestionResponse = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertEquals(1, openSuggestionResponse.getModels().size());

        IndexPlan indexPlan1 = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(5, indexPlan1.getAllLayouts().size());
        Assert.assertEquals(4, indexPlan1.getAllLayouts().stream().filter(LayoutEntity::isBase).count());
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel nDataModel = dataModelManager.getDataModelDesc(modelId);
        Set<String> dimensions = nDataModel.getAllNamedColumns().stream()
                .filter(t -> t.getStatus() == NDataModel.ColumnStatus.DIMENSION)
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toSet());
        List<String> columns = Arrays.asList("LINEORDER.LO_LINENUMBER", "LINEORDER.LO_ORDTOTALPRICE",
                "LINEORDER.LO_ORDERPRIOTITY", "LINEORDER.LO_EXTENDEDPRICE", "LINEORDER.LO_TAX");
        Assert.assertTrue(columns.stream().noneMatch(dimensions::contains));
    }

    @Test
    public void testOptimizeModelWithTableIndexDisabled() {
        Function<List<OpenModelRecResponse>, Long> discardCount = list -> list.stream()
                .map(OpenModelRecResponse::getIndexes).flatMap(List::stream)
                .filter(LayoutRecDetailResponse::isDiscarded).count();

        String project = "newten";
        String sql1 = "select order_id,buyer_id from test_order group by order_id,buyer_id";
        OpenSqlAccelerateRequest request = new OpenSqlAccelerateRequest();
        request.setProject(project);
        request.setSqls(Collections.singletonList(sql1));
        request.setAcceptRecommendation(true);
        request.setWithEmptySegment(true);
        request.setWithModelOnline(true);
        request.setWithOptimalModel(true);
        request.setForce2CreateNewModel(true);
        OpenSuggestionResponse openSuggestionResponse = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertEquals(1, openSuggestionResponse.getModels().size());
        Assert.assertEquals(0, openSuggestionResponse.getOptimalModels().size());
        Assert.assertEquals(0, discardCount.apply(openSuggestionResponse.getModels())
                + discardCount.apply(openSuggestionResponse.getOptimalModels()));

        request.setForce2CreateNewModel(false);
        request.setAcceptRecommendation(false);
        request.setDiscardTableIndex(true);
        String sql2 = " select buyer_id from test_order";
        String sql3 = " select max(buyer_id) from test_order";
        request.setSqls(Arrays.asList(sql1, sql2, sql3));
        OpenSuggestionResponse openSuggestionResponse3 = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertEquals(1, openSuggestionResponse3.getOptimalModels().size());
        Assert.assertEquals(1, discardCount.apply(openSuggestionResponse3.getModels())
                + discardCount.apply(openSuggestionResponse3.getOptimalModels()));
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(openSuggestionResponse3.getModels().get(0).getUuid());
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());

        request.setAcceptRecommendation(true);
        String sql4 = " select order_id from test_order";
        String sql5 = " select min(buyer_id) from test_order";
        request.setSqls(Arrays.asList(sql1, sql4, sql5));
        OpenSuggestionResponse openSuggestionResponse2 = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertEquals(1, openSuggestionResponse2.getOptimalModels().size());
        Assert.assertEquals(1, discardCount.apply(openSuggestionResponse2.getModels())
                + discardCount.apply(openSuggestionResponse2.getOptimalModels()));
        IndexPlan indexPlan1 = indexPlanManager.getIndexPlan(openSuggestionResponse2.getModels().get(0).getUuid());
        Assert.assertEquals(2, indexPlan1.getAllLayouts().size());
    }

    @Test
    public void testIndexDisabledWhenRecIndexDuplicate() {
        String project = "default";
        String sql1 = "SELECT c.* FROM (SELECT a.*, b.* FROM\n"
                + "     (SELECT ID1 AS id FROM TEST_MEASURE) a RIGHT JOIN (SELECT *\n"
                + "      FROM (SELECT ID1 AS id FROM TEST_MEASURE) d) b ON a.id=b.id) c";
        String sql2 = "SELECT c.* FROM (SELECT a.*, b.* FROM\n"
                + "        (SELECT ID1 AS id, NAME1 AS aName FROM TEST_MEASURE) a RIGHT JOIN (SELECT *\n"
                + "         FROM (SELECT ID1 AS id, NAME1 AS dName FROM TEST_MEASURE) d) b ON a.id=b.id) c";
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertEquals(0, indexPlan.getAllLayouts().stream().filter(LayoutEntity::isBase).count());

        OpenSqlAccelerateRequest request = new OpenSqlAccelerateRequest();
        request.setProject(project);
        request.setSqls(Arrays.asList(sql1, sql2));
        request.setForce2CreateNewModel(false);
        request.setAcceptRecommendation(false);
        request.setDiscardTableIndex(true);
        OpenSuggestionResponse openSuggestionResponse = modelRecService.suggestOrOptimizeModels(request);
        Assert.assertTrue(openSuggestionResponse.getErrorSqlList().isEmpty());
        Assert.assertEquals(1, openSuggestionResponse.getModels().size());
        Assert.assertFalse(
                openSuggestionResponse.getModels().get(0).getIndexes().get(0).getDimensions().get(0).isNew());
    }
}
