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

package org.apache.kylin.newten;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.merger.AfterBuildResourceMerger;
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.IndexesToSegmentsRequest;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.QueryHistoryAccelerateScheduler;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.service.params.IndexBuildParams;
import org.apache.kylin.rest.service.params.RefreshSegmentParams;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.SemiAutoTestBase;
import org.apache.spark.sql.SparderEnv;
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

import lombok.val;

public class PartialBuildJobTest extends SemiAutoTestBase {
    private NIndexPlanManager indexPlanManager;

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    private final ModelBuildService modelBuildService = Mockito.spy(new ModelBuildService());
    @InjectMocks
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @InjectMocks
    public AccessService accessService;
    @Spy
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

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
        overwriteSystemProp("HADOOP_USER_NAME", "root");

        RawRecService rawRecService = new RawRecService();
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
        QueryHistoryAccelerateScheduler queryHistoryAccelerateScheduler = QueryHistoryAccelerateScheduler.getInstance();
        ReflectionTestUtils.setField(queryHistoryAccelerateScheduler, "querySmartSupporter", rawRecService);
        queryHistoryAccelerateScheduler.init();
        getTestConfig().setMetadataUrl("test" + 777777
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=");
    }

    /**
     * A - B - D - C(c2)
     *   \
     *    C
     * <p>
     * index agg [C2.C_CUSTKEY, SUM(C2.C_CUSTKEY)]  ==> index based on A B D C
     * index table [C.*] ==> A C
     * index agg C ==> A C
     */
    @Test
    public void getRelatedTablesSnowModelTest() {
        String sql = "SELECT C2.C_CUSTKEY, SUM(C2.C_CUSTKEY) \n" + "FROM SSB.P_LINEORDER A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C2.C_CUSTKEY ORDER BY C2.C_CUSTKEY";
        String sql2 = "SELECT C.* \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n";

        String sql3 = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*2) \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2, sql3 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);

        indexPlanManager.updateIndexPlan(originModel.getId(), copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Lists.newArrayList(1));
            NAggregationGroup group1 = null;
            try {
                group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1],\n" //
                        + "        \"measures\": [100000,100001],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": []\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
            } catch (IOException e) {
                // do nothing
            }
            newRule.setAggregationGroups(Lists.newArrayList(group1));
            copyForWrite.setRuleBasedIndex(newRule);
        });
        IndexDependencyParser parser = new IndexDependencyParser(originModel);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(originModel.getId());
        Assert.assertEquals(4, indexPlan.getAllLayouts().size());

        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            List<String> relatedTables = parser.getRelatedTables(layoutEntity);
            if (IndexEntity.isAggIndex(layoutEntity.getIndexId())) {
                if (relatedTables.size() == 2) {
                    Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
                } else {
                    Assert.assertEquals(4, relatedTables.size());
                    Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
                }

            } else {
                Assert.assertEquals(2, relatedTables.size());
                Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
            }
        });
    }

    /**
     * A - B
     *  \
     *   C
     * index agg [C.C_CUSTKEY, SUM(C.C_CUSTKEY*10)] ==> A  C
     * index table [C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY]==> A B C
     */
    @Test
    public void getRelatedTablesFilterStarModelTest() {

        String sql = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*10) \n" //
                + "FROM SSB.P_LINEORDER A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";

        String sql2 = "SELECT C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY \n" //
                + "FROM SSB.P_LINEORDER  A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n";
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModelManager.getInstance(getTestConfig(), getProject()).updateDataModel(modelId, //
                copyForWrite -> copyForWrite.setFilterCondition("CUSTOMER.C_CUSTKEY<999 and CUSTOMER.C_CUSTKEY > 0"));

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);
        IndexDependencyParser parser = new IndexDependencyParser(originModel);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(originModel.getId());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());

        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            List<String> relatedTables = parser.getRelatedTables(layoutEntity);
            if (IndexEntity.isAggIndex(layoutEntity.getIndexId())) {
                Assert.assertEquals(2, relatedTables.size());
            } else {
                Assert.assertEquals(3, relatedTables.size());
            }
            Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
        });
    }

    /**
     * A - B
     *  \
     *   C
     * index agg [C.C_CUSTKEY, SUM(C.C_CUSTKEY*10)] ==> A  C
     * index table [C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY]==> A B C
     */
    @Test
    public void getRelatedTablesPartitionDescStarModelTest() {
        String sql = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*10) \n" //
                + "FROM SSB.P_LINEORDER A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";

        String sql2 = "SELECT C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY \n" //
                + "FROM SSB.P_LINEORDER  A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n";
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        NDataModelManager.getInstance(getTestConfig(), getProject()).updateDataModel(modelId, //
                copyForWrite -> {
                    copyForWrite.setPartitionDesc(new PartitionDesc());
                    copyForWrite.getPartitionDesc().setPartitionDateColumn("P_LINEORDER.LO_ORDERDATE");
                });

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);
        IndexDependencyParser parser = new IndexDependencyParser(originModel);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(originModel.getId());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());

        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            List<String> relatedTables = parser.getRelatedTables(layoutEntity);
            if (IndexEntity.isAggIndex(layoutEntity.getIndexId())) {
                Assert.assertEquals(2, relatedTables.size());
            } else {
                Assert.assertEquals(3, relatedTables.size());
            }
            Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
        });
    }

    /**
     * A - B - D - C(c2)
     *   \
     *    C
     * <p>
     * index agg [C2.C_CUSTKEY, SUM(C2.C_CUSTKEY)]  ==> index based on A B D C
     * index table [C.*] ==> A C
     * index agg C ==> A C
     */
    @Test
    public void partialBuildTest() {
        String sql = "SELECT C2.C_CUSTKEY, SUM(C2.C_CUSTKEY) \n" + "FROM SSB.P_LINEORDER A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C2.C_CUSTKEY ORDER BY C2.C_CUSTKEY";
        String sql2 = "SELECT C.* \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n";

        String sql3 = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*2) \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";
        // prepare an origin model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2, sql3 }, true);
        String modelId = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        UnitOfWork.doInTransactionWithRetry(() -> NIndexPlanManager.getInstance(getTestConfig(), getProject())
                .updateIndexPlan(modelId, copyForWrite -> {
                    val newRule = new RuleBasedIndex();
                    newRule.setIndexPlan(copyForWrite);
                    newRule.setDimensions(Lists.newArrayList(1));
                    NAggregationGroup group1 = null;
                    try {
                        group1 = JsonUtil.readValue("{\n" //
                                + "        \"includes\": [1],\n" //
                                + "        \"measures\": [100000,100001],\n" //
                                + "        \"select_rule\": {\n" //
                                + "          \"hierarchy_dims\": [],\n" //
                                + "          \"mandatory_dims\": [],\n" //
                                + "          \"joint_dims\": []\n" //
                                + "        }\n" + "}", NAggregationGroup.class);
                    } catch (IOException e) {
                        // do nothing
                    }
                    newRule.setAggregationGroups(Lists.newArrayList(group1));
                    copyForWrite.setRuleBasedIndex(newRule);
                }), getProject());

        testPartialBuildSegments(modelId);
        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(queryList, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    private void testPartialBuildSegments(String modelId) {
        val range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        indexDataConstructor.cleanSegments(modelId);
        NDataModel modelDesc = UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager
                .getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataModel(modelId, copyForWrite -> {
                    copyForWrite.setPartitionDesc(new PartitionDesc());
                    copyForWrite.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
                }), getProject());
        val layouts = dsMgr.getDataflow(modelId).getIndexPlan().getAllLayouts();

        val layoutIds = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toList());
        long layoutId = layoutIds.remove(0);
        JobInfoResponse jobInfo = null;
        try {
            List<String[]> multiValues = Lists.newArrayList();
            if (modelDesc.isMultiPartitionModel()) {
                multiValues.add(new String[] { "123" });
            }
            jobInfo = modelBuildService.buildSegmentsManually(getProject(), modelId, "" + range.getStart(),
                    "" + range.getEnd(), true, null, multiValues, 3, false, layoutIds, true, null, null);
        } catch (Exception e) {
            // do nothing
        }
        assert jobInfo != null;
        Assert.assertEquals(1, jobInfo.getJobs().size());
        val execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        NSparkCubingJob job = (NSparkCubingJob) execMgr.getJob(jobInfo.getJobs().get(0).getJobId());
        ExecutableState status = null;
        try {
            status = IndexDataConstructor.wait(job);
        } catch (InterruptedException e) {
            // do nothing
        }
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
        UnitOfWork.doInTransactionWithRetry(() -> {
            AfterBuildResourceMerger merger = new AfterBuildResourceMerger(getTestConfig(), getProject());
            NDataSegment oneSeg = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId)
                    .getSegments().get(0);
            merger.mergeAfterIncrement(modelId, oneSeg.getId(), Sets.newHashSet(layoutIds), buildStore);
            return true;
        }, getProject());

        testPartialCompleteSegments(modelId, layoutId);
    }

    private void testPartialCompleteSegments(String modelId, long layoutId) {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataSegment oneSeg = dsMgr.getDataflow(modelId).getSegments().get(0);
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(getProject());
        req.setParallelBuildBySegment(false);
        req.setSegmentIds(Lists.newArrayList(oneSeg.getId()));
        req.setPartialBuild(true);
        req.setIndexIds(Lists.newArrayList(layoutId));
        val rs = modelBuildService.addIndexesToSegments(
                IndexBuildParams.builder().project(req.getProject()).modelId(modelId).segmentIds(req.getSegmentIds())
                        .layoutIds(req.getIndexIds()).parallelBuildBySegment(req.isParallelBuildBySegment())
                        .priority(req.getPriority()).partialBuild(req.isPartialBuild()).build());
        Assert.assertEquals(1, rs.getJobs().size());
        val execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        NSparkCubingJob job = (NSparkCubingJob) execMgr.getJob(rs.getJobs().get(0).getJobId());
        ExecutableState status = null;
        try {
            status = IndexDataConstructor.wait(job);
        } catch (InterruptedException e) {
            // do nothing
        }
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        testPartialRefreshSegment(modelId);
    }

    private void testPartialRefreshSegment(String modelId) {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val layouts = dsMgr.getDataflow(modelId).getIndexPlan().getAllLayouts();
        val layoutIds = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toList());
        NDataSegment oneSeg = dsMgr.getDataflow(modelId).getSegments().get(0);
        String segmentUuid = oneSeg.getUuid();
        val rs = UnitOfWork.doInTransactionWithRetry(() -> modelBuildService
                .refreshSegmentById(new RefreshSegmentParams(getProject(), modelId, new String[] { segmentUuid }, true)
                        .withPartialBuild(true) //
                        .withBatchIndexIds(layoutIds)),
                getProject());
        Assert.assertEquals(1, rs.size());
        val execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        NSparkCubingJob job = (NSparkCubingJob) execMgr.getJob(rs.get(0).getJobId());
        ExecutableState status = null;
        try {
            status = IndexDataConstructor.wait(job);
        } catch (InterruptedException e) {
            // do nothing
        }
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig conf = getTestConfig();
            AfterBuildResourceMerger merger = new AfterBuildResourceMerger(conf, getProject());
            NDataSegment segment = NDataflowManager.getInstance(conf, getProject()).getDataflow(modelId).getSegments()
                    .get(0);
            merger.mergeAfterIncrement(modelId, segment.getUuid(), Sets.newHashSet(layoutIds), buildStore);
            return true;
        }, getProject());
    }

    private void prepareData() throws IOException {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        TableDesc tableDesc = tableMgr.getTableDesc("SSB.P_LINEORDER");
        String s = JsonUtil.writeValueAsIndentString(tableDesc);

        TableDesc newTable = JsonUtil.readValue(s, TableDesc.class);
        newTable.setUuid(RandomUtil.randomUUIDStr());
        newTable.setName("SSB.LINEORDER".split("\\.")[1]);
        newTable.setMvcc(-1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager.getInstance(getTestConfig(), getProject()).createTableDesc(newTable);
            return true;
        }, getProject());
    }
}
