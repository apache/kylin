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

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ModelChangeSupporter;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.NUserGroupService;
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

public class ExcludedCheckerBasicCiTest extends SemiAutoTestBase {
    private NDataModelManager modelManager;
    private NIndexPlanManager indexPlanManager;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @InjectMocks
    private final RawRecService rawRecService = new RawRecService();
    @InjectMocks
    private final ProjectService projectService = new ProjectService();
    @InjectMocks
    private final OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);
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
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
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
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      index independent on customer builds successfully
     */
    @Test
    public void testStarModel() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE, LINEORDER.LO_CUSTKEY  FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY  DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n" //
                + "ORDER BY DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n";
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // change to semi-auto
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.toSemiAutoMode(getProject());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      the index query result equals to the result of push-down
     */
    @Test
    public void testStarModelValidateAggIndexResult() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE, LINEORDER.LO_CUSTKEY, sum(LINEORDER.LO_EXTENDEDPRICE)  FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY  DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n" //
                + "ORDER BY DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n";
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // change to semi-auto
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.toSemiAutoMode(getProject());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      the index query result equals to the result of push-down
     */
    @Test
    public void testStarModelValidateTableIndexResult() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE, LINEORDER.LO_CUSTKEY  FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "ORDER BY DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n";
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // change to semi-auto
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.toSemiAutoMode(getProject());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     *      SSB.CUSTOMER      ====  SSB.LINEORDER
     * assertion:
     *      index independent on customer & lineorder builds successfully
     */
    @Test
    public void testSnowModel() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE FROM SSB.P_LINEORDER AS P_LINEORDER \n"
                + "INNER JOIN SSB.DATES ON P_LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON P_LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "INNER JOIN SSB.LINEORDER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY  DATES.D_DATE \n" //
                + "ORDER BY DATES.D_DATE \n";
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // change to semi-auto
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.toSemiAutoMode(getProject());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
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
