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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.model.util.scd2.SCD2SqlConverter;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.QueryTimesResponse;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.LayoutRecDetailResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class ModelSmartServiceTest extends SourceTestCase {
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
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Spy
    private final AclTCRServiceSupporter aclTCRService = Mockito.spy(AclTCRServiceSupporter.class);

    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Spy
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @InjectMocks
    private UserService userService = Mockito.spy(UserService.class);

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Spy
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private final StreamingJobListener eventListener = new StreamingJobListener();

    private static SparkConf sparkConf;
    private static SparkSession ss;

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

        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testSuggestModel() {
        List<String> sqls = Lists.newArrayList();
        Mockito.doReturn(false).when(modelService).isProjectNotExist(getProject());
        val result = modelSmartService.couldAnsweredByExistedModel(getProject(), sqls);
        Assert.assertTrue(result);
    }

    @Test
    public void testSuggestModelWithModelName() {
        String project = "newten";
        transferProjectToSemiAutoMode(getTestConfig(), project);

        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select floor(date'2020-11-17' TO day), ceil(date'2020-11-17' TO day) from test_kylin_fact");
        AbstractContext proposeContext = modelSmartService.suggestModel(project, sqlList, false, true);
        Assert.assertNull(proposeContext.getModelName());
        SuggestionResponse modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, modelSuggestionResponse.getNewModels().size());
        Assert.assertEquals("AUTO_MODEL_TEST_KYLIN_FACT_1", modelSuggestionResponse.getNewModels().get(0).getAlias());

        proposeContext = modelSmartService.suggestModel(project, sqlList, false, true, "test_model");
        modelSuggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, modelSuggestionResponse.getNewModels().size());
        Assert.assertEquals("test_model_1", modelSuggestionResponse.getNewModels().get(0).getAlias());
    }

    @Test
    public void testAnswerBySnapshot() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val tableManager = NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject());
        val table = tableManager.copyForWrite(tableManager.getTableDesc("DEFAULT.TEST_ORDER"));
        table.setLastSnapshotPath("default/table_snapshot/DEFAULT.TEST_ORDER/fb283efd-36fb-43de-86dc-40cf39054f59");
        tableManager.updateTableDesc(table);

        List<String> sqls = Lists.newArrayList("select order_id, count(*) from test_order group by order_id limit 1");
        Mockito.doReturn(false).when(modelService).isProjectNotExist(getProject());
        val result = modelSmartService.couldAnsweredByExistedModel(getProject(), sqls);
        Assert.assertTrue(result);
    }

    @Test
    public void testMultipleModelContextSelectedTheSameModel() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(dataflow.getConfig(),
                dataflow.getProject());
        val table1 = tableMetadataManager.copyForWrite(tableMetadataManager.getTableDesc("EDW.TEST_CAL_DT"));
        table1.setLastSnapshotPath("default/table_snapshot/EDW.TEST_CAL_DT/a27a7f08-792a-4514-a5ec-3182ea5474cc");
        tableMetadataManager.updateTableDesc(table1);

        val table2 = tableMetadataManager.copyForWrite(tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER"));
        table2.setLastSnapshotPath("default/table_snapshot/DEFAULT.TEST_ORDER/fb283efd-36fb-43de-86dc-40cf39054f59");
        tableMetadataManager.updateTableDesc(table2);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(getProject(), copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });

        val sqls = Lists.newArrayList("select order_id, count(*) from test_order group by order_id limit 1",
                "select cal_dt, count(*) from edw.test_cal_dt group by cal_dt limit 1",
                "SELECT count(*) \n" + "FROM \n" + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                        + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                        + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                        + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n"
                        + "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n"
                        + "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                        + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                        + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                        + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\" group by test_kylin_fact.cal_dt");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, true, true);
        Assert.assertEquals(3, proposeContext.getModelContexts().size());
        Assert.assertEquals(1, proposeContext.getModelContexts().stream()
                .filter(modelContext -> !modelContext.isSnapshotSelected()).count());
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, response.getReusedModels().size());
        Assert.assertEquals(0, response.getNewModels().size());
        response.getReusedModels().forEach(recommendedModelResponse -> {
            List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
            Assert.assertTrue(indexes.isEmpty());
        });

        AbstractContext proposeContext2 = modelSmartService.suggestModel(getProject(), sqls.subList(0, 2), true, true);
        Assert.assertEquals(2, proposeContext2.getModelContexts().size());
        Assert.assertEquals(0, proposeContext2.getModelContexts().stream()
                .filter(modelContext -> !modelContext.isSnapshotSelected()).count());
        val response2 = modelSmartService.buildModelSuggestionResponse(proposeContext2);
        Assert.assertEquals(0, response2.getReusedModels().size());
        Assert.assertEquals(0, response2.getNewModels().size());
        response2.getReusedModels().forEach(recommendedModelResponse -> {
            List<LayoutRecDetailResponse> indexes = recommendedModelResponse.getIndexes();
            Assert.assertTrue(indexes.isEmpty());
        });
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
    public void testProposeDimAsMeasureToAnswerCountDistinctExpr() {
        overwriteSystemProp("kylin.query.implicit-computed-column-convert", "FALSE");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "TRUE");
        val sqls = Lists.newArrayList("select \n"
                + "count(distinct (case when ORDER_ID > 0  then price when ORDER_ID > 10 then SELLER_ID  else null end))  \n"
                + "FROM TEST_KYLIN_FACT ");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, response.getNewModels().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().size());
        Assert.assertEquals(3, response.getNewModels().get(0).getIndexes().get(0).getDimensions().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().get(0).getMeasures().size());

        Assert.assertEquals("ORDER_ID",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(0).getDimension().getName());
        Assert.assertEquals("PRICE",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(1).getDimension().getName());
        Assert.assertEquals("SELLER_ID",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(2).getDimension().getName());
        Assert.assertEquals("COUNT_ALL",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(0).getMeasure().getName());
    }

    @Test
    public void testProposeMeasureWhenSubQueryOnFilter() {
        val sqls = Lists.newArrayList("select LO_ORDERDATE,sum(LO_TAX) from SSB.LINEORDER "
                + "where LO_ORDERDATE = (select max(LO_ORDERDATE) from SSB.LINEORDER) group by LO_ORDERDATE ;");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(1, response.getNewModels().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().size());
        Assert.assertEquals(1, response.getNewModels().get(0).getIndexes().get(0).getDimensions().size());
        Assert.assertEquals(3, response.getNewModels().get(0).getIndexes().get(0).getMeasures().size());

        Assert.assertEquals("LO_ORDERDATE",
                response.getNewModels().get(0).getIndexes().get(0).getDimensions().get(0).getDimension().getName());
        Assert.assertEquals("COUNT_ALL",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(0).getMeasure().getName());
        Assert.assertEquals("SUM_LINEORDER_LO_TAX",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(1).getMeasure().getName());
        Assert.assertEquals("MAX_LINEORDER_LO_ORDERDATE",
                response.getNewModels().get(0).getIndexes().get(0).getMeasures().get(2).getMeasure().getName());
    }

    @Test
    public void testCouldAnsweredByExistedModels() {
        val project = "streaming_test";
        val proposeContext0 = modelSmartService.probeRecommendation(project, Collections.emptyList());
        Assert.assertTrue(proposeContext0.getProposedModels().isEmpty());

        val sqlList = Collections.singletonList("SELECT * FROM SSB.P_LINEORDER_STR");
        val proposeContext1 = modelSmartService.probeRecommendation(project, sqlList);
        Assert.assertTrue(proposeContext1.getProposedModels().isEmpty());

        val sqls = Lists.newArrayList("select * from SSB.LINEORDER");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        val response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertTrue(response.getReusedModels().isEmpty());

        thrown.expect(KylinException.class);
        modelSmartService.probeRecommendation("not_existed_project", sqlList);
    }

    @Test
    public void testProposeWhenAggPushdown() {
        // before agg push down, propose table index and agg index
        overwriteSystemProp("kylin.query.calcite.aggregate-pushdown-enabled", "FALSE");
        val sqls = Lists.newArrayList("SELECT \"自定义 SQL 查询\".\"CAL_DT\" ,\n"
                + "       SUM (\"自定义 SQL 查询\".\"SELLER_ID\") AS \"TEMP_Calculation_54915774428294\",\n"
                + "               COUNT (DISTINCT \"自定义 SQL 查询\".\"CAL_DT\") AS \"TEMP_Calculation_97108873613918\",\n"
                + "                     COUNT (DISTINCT (CASE\n"
                + "                                          WHEN (\"t0\".\"x_measure__0\" > 0) THEN \"t0\".\"LSTG_FORMAT_NAME\"\n"
                + "                                          ELSE CAST (NULL AS VARCHAR (1))\n"
                + "                                      END)) AS \"TEMP_Calculation_97108873613911\"\n" + "FROM\n"
                + "  (SELECT *\n" + "   FROM TEST_KYLIN_FACT) \"自定义 SQL 查询\"\n" + "INNER JOIN\n"
                + "     (SELECT LSTG_FORMAT_NAME, ORDER_ID, SUM (\"PRICE\") AS \"X_measure__0\"\n"
                + "      FROM TEST_KYLIN_FACT  GROUP  BY LSTG_FORMAT_NAME, ORDER_ID) \"t0\" ON \"自定义 SQL 查询\".\"ORDER_ID\" = \"t0\".\"ORDER_ID\"\n"
                + "GROUP  BY \"自定义 SQL 查询\".\"CAL_DT\"");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        SuggestionResponse response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(2, response.getNewModels().get(0).getIndexPlan().getIndexes().size());
        Assert.assertTrue(response.getNewModels().get(0).getIndexPlan().getIndexes().get(0).isTableIndex());
        Assert.assertTrue(
                IndexEntity.isAggIndex(response.getNewModels().get(0).getIndexPlan().getIndexes().get(1).getId()));

        // after agg push down, propose two agg index
        overwriteSystemProp("kylin.query.calcite.aggregate-pushdown-enabled", "TRUE");
        proposeContext = modelSmartService.suggestModel(getProject(), sqls, false, true);
        response = modelSmartService.buildModelSuggestionResponse(proposeContext);
        Assert.assertEquals(2, response.getNewModels().get(0).getIndexPlan().getIndexes().size());
        Assert.assertTrue(
                IndexEntity.isAggIndex(response.getNewModels().get(0).getIndexPlan().getIndexes().get(0).getId()));
        Assert.assertTrue(
                IndexEntity.isAggIndex(response.getNewModels().get(0).getIndexPlan().getIndexes().get(1).getId()));
    }

    @Test
    public void testOnlineSCD2Model() throws Exception {
        final String randomUser = RandomStringUtils.randomAlphabetic(5);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(randomUser, "123456", Constant.ROLE_ADMIN));
        String projectName = "default";
        val scd2Model = createNonEquiJoinModel(projectName, "scd2_non_equi");

        //turn off scd2 model
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject("default", copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "false");
        });
        thrown.expect(KylinException.class);
        thrown.expectMessage("This model can’t go online as it includes non-equal join conditions");
        modelService.updateDataModelStatus(scd2Model.getId(), "default", "ONLINE");
    }

    private NDataModel createNonEquiJoinModel(String projectName, String modelName) {
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        NDataModel model = modelManager.getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        model.setPartitionDesc(null);
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject(projectName);
        modelRequest.setAlias(modelName);
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        modelRequest.getSimplifiedJoinTableDescs().get(0).getSimplifiedJoinDesc()
                .setSimplifiedNonEquiJoinConditions(genNonEquiJoinCond());

        return modelService.createModel(modelRequest.getProject(), modelRequest);
    }

    private List<NonEquiJoinCondition.SimplifiedJoinCondition> genNonEquiJoinCond() {
        NonEquiJoinCondition.SimplifiedJoinCondition join1 = new NonEquiJoinCondition.SimplifiedJoinCondition(
                "TEST_KYLIN_FACT.SELLER_ID", "TEST_ORDER.TEST_EXTENDED_COLUMN", SqlKind.GREATER_THAN_OR_EQUAL);
        NonEquiJoinCondition.SimplifiedJoinCondition join2 = new NonEquiJoinCondition.SimplifiedJoinCondition(
                "TEST_KYLIN_FACT.SELLER_ID", "TEST_ORDER.BUYER_ID", SqlKind.LESS_THAN);
        return Arrays.asList(join1, join2);
    }

    @Test
    public void testCloneSCD2Model() {
        final String randomUser = RandomStringUtils.randomAlphabetic(5);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(randomUser, "123456", Constant.ROLE_ADMIN));

        String projectName = "default";

        val scd2Model = createNonEquiJoinModel(projectName, "scd2_non_equi");

        //turn off scd2 model
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject("default", copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "false");
        });

        modelService.cloneModel(scd2Model.getId(), "clone_scd2_non_equi", projectName);

        List<NDataModelResponse> newModels = modelService.getModels("clone_scd2_non_equi", projectName, true, "", null,
                "last_modify", true);

        Assert.assertEquals(1, newModels.size());

        Assert.assertEquals(ModelStatusToDisplayEnum.OFFLINE, newModels.get(0).getStatus());
    }

    @Test
    public void testCreateModelNonEquiJoin() {

        val newModel = createNonEquiJoinModel("default", "non_equi_join");
        String sql = SCD2SqlConverter.INSTANCE.genSCD2SqlStr(newModel.getJoinTables().get(0).getJoin(),
                genNonEquiJoinCond());
        Assert.assertEquals(
                "select * from  \"DEFAULT\".\"TEST_KYLIN_FACT\" AS \"TEST_KYLIN_FACT\" LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" AS \"TEST_ORDER\" ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\" AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\">=\"TEST_ORDER\".\"TEST_EXTENDED_COLUMN\") AND (\"TEST_KYLIN_FACT\".\"SELLER_ID\"<\"TEST_ORDER\".\"BUYER_ID\")",
                sql);

        Assert.assertTrue(newModel.getJoinTables().get(0).getJoin().isNonEquiJoin());
    }

    @Test
    public void testProbeRecommendation_throwsException() {
        when(modelService.isProjectNotExist(Mockito.anyString())).thenReturn(true);
        try {
            modelSmartService.probeRecommendation("SOME_PROJECT", null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(PROJECT_NOT_EXIST.getCodeMsg("SOME_PROJECT"), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckBatchSqlSize() {
        List<String> list = Collections.nCopies(201, "sql");
        Assert.assertTrue(list.size() > 200);
        KylinConfig config = getTestConfig();
        Assert.assertThrows(KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(modelSmartService, "checkBatchSqlSize", config, list));
    }
}
