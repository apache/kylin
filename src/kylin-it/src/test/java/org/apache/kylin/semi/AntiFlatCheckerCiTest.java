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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.index.IndexSuggester;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ModelChangeSupporter;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.OptRecApproveService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SemiAutoTestBase;
import org.apache.spark.sql.SparderEnv;
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

public class AntiFlatCheckerCiTest extends SemiAutoTestBase {

    private NDataModelManager modelManager;
    private JdbcRawRecStore jdbcRawRecStore;
    private NIndexPlanManager indexPlanManager;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @InjectMocks
    private final RawRecService rawRecService = Mockito.spy(new RawRecService());
    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());
    @InjectMocks
    private final OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    private final OptRecApproveService optRecApproveService = Mockito.spy(new OptRecApproveService());
    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);
    @InjectMocks
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());
    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Spy
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @Spy
    private final List<ModelChangeSupporter> modelChangeSupporters = Mockito.spy(Arrays.asList(rawRecService));

    @Override
    public String getProject() {
        return "ssb";
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        prepareData();
        jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
    }

    private void prepareACL() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void teardown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
    }

    /**
     * precondition:
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      push-down result is the same as index result
     */
    @Test
    public void basicTest() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "SELECT DATES.D_DATE FROM SSB.P_LINEORDER AS LINEORDER \n"
                        + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                        + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n" },
                true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setFlattenable(JoinTableDesc.NORMALIZED);
                    join.setKind(NDataModel.TableKind.LOOKUP);
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // change to semi-auto
        MetadataTestUtils.toSemiAutoMode(getProject());

        // accelerate & validate recommendations
        String sql = "SELECT C_REGION, DATES.D_DATEKEY, SUM(LINEORDER.LO_EXTENDEDPRICE*LINEORDER.LO_DISCOUNT)\n"
                + "FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY C_REGION, DATES.D_DATEKEY\n" //
                + "ORDER BY C_REGION, DATES.D_DATEKEY";
        AbstractContext context2 = AccelerationUtil.genOptRec(getTestConfig(), getProject(), new String[] { sql });
        rawRecService.transferAndSaveRecommendations(context2);
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(5, rawRecItems.size());

        // approve recommendations & validate layouts
        List<String> modelIds = Lists.newArrayList();
        modelIds.add(modelId);
        optRecApproveService.batchApprove(getProject(), modelIds, "all", true, false);
        List<LayoutEntity> allLayouts = indexPlanManager.getIndexPlan(modelId).getAllLayouts();
        Assert.assertEquals(2, allLayouts.size());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        dumpMetadata();

        // compare sql
        List<Pair<String, String>> queryList = readSQL();
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      ssb.p_lineorder === ssb.customer
     *      ssb.p_lineorder --- ssb.dates
     *      turn on patial match join & ssb.customer without pre-calculation
     * assertion:
     *      1) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      2) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         INNER JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      query result are the same
     */
    @Test
    public void partialJoinTest() throws InterruptedException {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "TRUE");
        String sql1 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";
        String sql2 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  INNER JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";

        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql2 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel(modelId, copyForWrite -> {
                    final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                    joinTables.forEach(join -> {
                        if (join.getTable().equals("SSB.CUSTOMER")) {
                            join.setFlattenable(JoinTableDesc.NORMALIZED);
                            join.setKind(NDataModel.TableKind.LOOKUP);
                            join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                        }
                    });
                }), getProject());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql1", sql1));
        queryList.add(Pair.newPair("sql2", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      ssb.p_lineorder === ssb.customer
     *      ssb.p_lineorder --- ssb.dates
     *      turn on patial match join & ssb.customer without pre-calculation
     * assertion:
     *      1) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      2) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      query result are the same
     */
    @Test
    public void leftJoinRelationTest() throws InterruptedException {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "TRUE");
        String sql1 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";
        String sql2 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";
        String sql3 = "SELECT SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n";

        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql2 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel(modelId, copyForWrite -> {
                    final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                    joinTables.forEach(join -> {
                        if (join.getTable().equals("SSB.CUSTOMER")) {
                            join.setFlattenable(JoinTableDesc.NORMALIZED);
                            join.setKind(NDataModel.TableKind.LOOKUP);
                            join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                        }
                    });
                }), getProject());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql1", sql1));
        queryList.add(Pair.newPair("sql2", sql2));
        queryList.add(Pair.newPair("sql3", sql3));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    @Test
    public void testSnowModelCanNotPropose() {
        String[] sqls = {
                "select p_lineorder.lo_custkey, sum(lineorder.lo_extendedprice) \n"
                        + "from ssb.p_lineorder as p_lineorder\n"
                        + "  left join ssb.dates on p_lineorder.lo_orderdate = dates.d_datekey\n"
                        + "  left join ssb.customer on p_lineorder.lo_custkey = customer.c_custkey\n"
                        + "  left join ssb.lineorder as lineorder on dates.d_datekey = lineorder.lo_orderdate\n"
                        + "  group by p_lineorder.lo_custkey order by p_lineorder.lo_custkey",
                "select p_lineorder.lo_custkey, lineorder.lo_extendedprice \n" //
                        + "from ssb.p_lineorder as p_lineorder\n"
                        + "  left join ssb.dates on p_lineorder.lo_orderdate = dates.d_datekey\n"
                        + "  left join ssb.customer on p_lineorder.lo_custkey = customer.c_custkey\n"
                        + "  left join ssb.lineorder as lineorder on dates.d_datekey = lineorder.lo_orderdate\n"
                        + "  group by p_lineorder.lo_custkey, lineorder.lo_extendedprice\n"
                        + "  order by p_lineorder.lo_custkey, lineorder.lo_extendedprice" };

        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sqls[0] }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            modelManager.updateDataModel(modelId, copyForWrite -> {
                final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                joinTables.forEach(join -> {
                    join.setKind(NDataModel.TableKind.LOOKUP);
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                    if (join.getTable().equals("SSB.DATES")) {
                        join.setFlattenable(JoinTableDesc.NORMALIZED);

                    } else if (join.getTable().equals("SSB.LINEORDER")) {
                        join.setFlattenable(JoinTableDesc.FLATTEN);
                        join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
                    }
                });
            });
            return null;
        }, getProject());

        // with measure can not propose
        {
            AbstractContext context = AccelerationUtil.genOptRec(kylinConfig, getProject(), new String[] { sqls[0] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[0]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.MEASURE_ON_ANTI_FLATTEN_LOOKUP));
        }

        // with dimension can not derive
        {
            AbstractContext context = AccelerationUtil.genOptRec(kylinConfig, getProject(), new String[] { sqls[1] });
            AccelerateInfo accelerateInfo = context.getAccelerateInfoMap().get(sqls[1]);
            Assert.assertTrue(accelerateInfo.isPending());
            Assert.assertTrue(accelerateInfo.getPendingMsg().startsWith(IndexSuggester.FK_ON_ANTI_FLATTEN_LOOKUP));
        }
    }

    @Test
    public void snowModelTest() throws InterruptedException {
        String sql2 = "SELECT LINEORDER.LO_CUSTKEY, SUM(LINEORDER.LO_EXTENDEDPRICE) \n"
                + "FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER AS LR ON DATES.D_DATEKEY = LR.LO_ORDERDATE\n"
                + "  GROUP BY LINEORDER.LO_CUSTKEY ORDER BY LINEORDER.LO_CUSTKEY";

        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql2 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                if (join.getTable().equals("SSB.DATES")) {
                    join.setFlattenable(JoinTableDesc.NORMALIZED);

                } else if (join.getTable().equals("SSB.LINEORDER")) {
                    join.setFlattenable(JoinTableDesc.FLATTEN);
                    //join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY)
                }
            });
        });

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql2", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        try {
            ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not match"));
        }
    }

    private List<Pair<String, String>> readSQL() throws IOException {
        String folder = "src/test/resources/anti_flatten/to_many/sql/";
        return ExecAndCompExt.fetchPartialQueries(folder, 0, 100);
    }

    private void prepareData() throws IOException {
        String srcTableDir = "/derived_query/anti_flatten/tables/";
        MetadataTestUtils.replaceTable(getProject(), getClass(), srcTableDir, "SSB.CUSTOMER");
        MetadataTestUtils.replaceTable(getProject(), getClass(), srcTableDir, "SSB.DATES");
        MetadataTestUtils.replaceTable(getProject(), getClass(), srcTableDir, "SSB.P_LINEORDER");
        MetadataTestUtils.createTable(getProject(), getClass(), srcTableDir, "SSB.LINEORDER");

        String srcDataDir = "/derived_query/anti_flatten/data/";
        MetadataTestUtils.putTableCSVData(srcDataDir, getClass(), "SSB.CUSTOMER");
        MetadataTestUtils.putTableCSVData(srcDataDir, getClass(), "SSB.DATES");
        MetadataTestUtils.putTableCSVData(srcDataDir, getClass(), "SSB.P_LINEORDER");
        MetadataTestUtils.putTableCSVData(srcDataDir, getClass(), "SSB.LINEORDER");
    }
}
