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
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.SmartMaster;
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
import org.apache.kylin.util.ExecAndComp;
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

public class ExcludedCheckerExtraCiTest extends SemiAutoTestBase {

    private static final String SNAPSHOT_PREFERRED_PROP = "kylin.query.snapshot-preferred-for-table-exclusion";

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

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        prepareData();
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

    @Test
    public void testPkToFkDerivedInfo() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT LINEORDER.LO_TAX, LINEORDER.LO_CUSTKEY, LINEORDER.LO_SHIPMODE \n" //
                + "   FROM SSB.P_LINEORDER AS LINEORDER\n" //
                + "   INNER JOIN SSB.CUSTOMER AS CUSTOMER ON LINEORDER.LO_CUSTKEY=CUSTOMER.C_CUSTKEY\n" //
                + "   LEFT JOIN SSB.CUSTOMER AS CUSTOMER_1 ON LINEORDER.LO_SHIPMODE=CUSTOMER_1.C_CUSTKEY\n" //
                + "   GROUP BY LINEORDER.LO_TAX, LINEORDER.LO_CUSTKEY, LINEORDER.LO_SHIPMODE\n"
                + "   ORDER BY LINEORDER.LO_TAX, LINEORDER.LO_CUSTKEY, LINEORDER.LO_SHIPMODE";
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

        UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel(modelId, copyForWrite -> {
                    List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                    joinTables.forEach(join -> join.setKind(NDataModel.TableKind.LOOKUP));
                }), getProject());

        // change to semi-auto and set excluded table
        MetadataTestUtils.toSemiAutoMode(getProject());
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        sql = "SELECT A.C_ADDRESS FROM\n" //
                + "  (SELECT LINEORDER.LO_TAX, CUSTOMER_1.C_ADDRESS\n" //
                + "   FROM SSB.P_LINEORDER AS LINEORDER\n" //
                + "   INNER JOIN SSB.CUSTOMER AS CUSTOMER ON LINEORDER.LO_CUSTKEY=CUSTOMER.C_CUSTKEY\n" //
                + "   LEFT JOIN SSB.CUSTOMER AS CUSTOMER_1 ON LINEORDER.LO_SHIPMODE=CUSTOMER_1.C_CUSTKEY) A\n" //
                + "WHERE A.LO_TAX >0\n" //
                + "GROUP BY A.C_ADDRESS ORDER BY A.C_ADDRESS";

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    @Test
    public void testRollBackToAggIndexMatch() throws InterruptedException {
        // non raw sql
        String sql = "select lineorder.lo_tax, lineorder.lo_custkey, customer.c_address \n" //
                + "   from ssb.p_lineorder as lineorder\n" //
                + "   inner join ssb.customer as customer on lineorder.lo_custkey=customer.c_custkey\n" //
                + "   group by lineorder.lo_tax, lineorder.lo_custkey, customer.c_address\n"
                + "   order by lineorder.lo_tax, lineorder.lo_custkey, customer.c_address";

        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();
        {
            NDataModel originModel = modelManager.getDataModelDesc(modelId);
            originModel.getJoinTables().forEach(join -> {
                Assert.assertTrue(join.isFlattenable());
                Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
            });
            IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
            Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());
            UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                    .updateDataModel(modelId, copyForWrite -> {
                        List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                        joinTables.forEach(join -> join.setKind(NDataModel.TableKind.LOOKUP));
                    }), getProject());
        }

        // set excluded table
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");

        // run propose again
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        {
            IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
            Assert.assertEquals(2, originIndexPlan.getAllLayouts().size());
            UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                    .updateDataModel(modelId, copyForWrite -> {
                        List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                        joinTables.forEach(join -> join.setKind(NDataModel.TableKind.LOOKUP));
                    }), getProject());
        }

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // turn off snapshot preferred
        {
            MetadataTestUtils.updateProjectConfig(getProject(), SNAPSHOT_PREFERRED_PROP, "false");
            ExecAndComp.EnhancedQueryResult rst = ExecAndComp.queryModelWithOlapContext(getProject(), "default", sql);
            OlapContext context = rst.getOlapContexts().iterator().next();
            long layoutId = context.getStorageContext().getBatchCandidate().getLayoutId();
            Assert.assertEquals(1L, layoutId);
        }

        // turn on snapshot preferred
        {
            MetadataTestUtils.updateProjectConfig(getProject(), SNAPSHOT_PREFERRED_PROP, "true");
            ExecAndComp.EnhancedQueryResult rst = ExecAndComp.queryModelWithOlapContext(getProject(), "default", sql);
            OlapContext context = rst.getOlapContexts().iterator().next();
            long layoutId = context.getStorageContext().getBatchCandidate().getLayoutId();
            Assert.assertEquals(10001L, layoutId);
        }

        // delete the index need to derive
        {
            UnitOfWork.doInTransactionWithRetry(() -> {
                NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
                indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
                    copyForWrite.getAllLayouts().removeIf(layout -> layout.getColOrder().size() == 3);
                    copyForWrite.getIndexes().removeIf(index -> index.getDimensions().size() == 2);
                });
                return true;
            }, getProject());
            ExecAndComp.EnhancedQueryResult rst = ExecAndComp.queryModelWithOlapContext(getProject(), "default", sql);
            OlapContext context = rst.getOlapContexts().iterator().next();
            long layoutId = context.getStorageContext().getBatchCandidate().getLayoutId();
            Assert.assertEquals(1L, layoutId);
        }
    }

    @Test
    public void testRollBackToTableIndexMatch() throws InterruptedException {
        // raw sql
        String sql = "select lineorder.lo_tax, lineorder.lo_custkey, customer.c_address \n" //
                + "   from ssb.p_lineorder as lineorder\n" //
                + "   inner join ssb.customer as customer on lineorder.lo_custkey=customer.c_custkey\n" //
                + "   order by lineorder.lo_tax, lineorder.lo_custkey, customer.c_address";

        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();
        {
            NDataModel originModel = modelManager.getDataModelDesc(modelId);
            originModel.getJoinTables().forEach(join -> {
                Assert.assertTrue(join.isFlattenable());
                Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
            });
            IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
            Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());
            UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                    .updateDataModel(modelId, copyForWrite -> {
                        List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                        joinTables.forEach(join -> join.setKind(NDataModel.TableKind.LOOKUP));
                    }), getProject());
        }

        // set excluded table
        MetadataTestUtils.turnOnExcludedTable(getTestConfig());
        MetadataTestUtils.mockExcludedTable(getProject(), "SSB.CUSTOMER");

        // run propose again
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        {
            IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
            Assert.assertEquals(2, originIndexPlan.getAllLayouts().size());
            UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                    .updateDataModel(modelId, copyForWrite -> {
                        List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                        joinTables.forEach(join -> join.setKind(NDataModel.TableKind.LOOKUP));
                    }), getProject());
        }

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // turn off snapshot preferred
        {
            MetadataTestUtils.updateProjectConfig(getProject(), SNAPSHOT_PREFERRED_PROP, "false");
            ExecAndComp.EnhancedQueryResult rst = ExecAndComp.queryModelWithOlapContext(getProject(), "default", sql);
            OlapContext context = rst.getOlapContexts().iterator().next();
            long layoutId = context.getStorageContext().getBatchCandidate().getLayoutId();
            Assert.assertEquals(1L + IndexEntity.TABLE_INDEX_START_ID, layoutId);
        }

        // turn on snapshot preferred
        {
            MetadataTestUtils.updateProjectConfig(getProject(), SNAPSHOT_PREFERRED_PROP, "true");
            ExecAndComp.EnhancedQueryResult rst = ExecAndComp.queryModelWithOlapContext(getProject(), "default", sql);
            OlapContext context = rst.getOlapContexts().iterator().next();
            long layoutId = context.getStorageContext().getBatchCandidate().getLayoutId();
            Assert.assertEquals(10001L + IndexEntity.TABLE_INDEX_START_ID, layoutId);
        }

        // delete the index need to derive
        {
            UnitOfWork.doInTransactionWithRetry(() -> {
                NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
                indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
                    copyForWrite.getAllLayouts().removeIf(layout -> layout.getColOrder().size() == 3);
                    copyForWrite.getIndexes().removeIf(index -> index.getDimensions().size() == 2);
                });
                return true;
            }, getProject());
            ExecAndComp.EnhancedQueryResult rst = ExecAndComp.queryModelWithOlapContext(getProject(), "default", sql);
            OlapContext context = rst.getOlapContexts().iterator().next();
            long layoutId = context.getStorageContext().getBatchCandidate().getLayoutId();
            Assert.assertEquals(1L + IndexEntity.TABLE_INDEX_START_ID, layoutId);
        }

    }

    private void prepareData() throws IOException {
        String srcTableDir = "/derived_query/excluded_table/tables/";
        MetadataTestUtils.replaceTable(getProject(), getClass(), srcTableDir, "SSB.CUSTOMER");
        MetadataTestUtils.replaceTable(getProject(), getClass(), srcTableDir, "SSB.P_LINEORDER");

        String srcDataDir = "/derived_query/excluded_table/data/";
        MetadataTestUtils.putTableCSVData(srcDataDir, getClass(), "SSB.CUSTOMER");
        MetadataTestUtils.putTableCSVData(srcDataDir, getClass(), "SSB.P_LINEORDER");
    }
}
