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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.OpenRecApproveResponse;
import org.apache.kylin.rest.response.OptRecLayoutsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

public class OptRecServiceIllegalMeasureTest extends OptRecV2TestBase {
    @InjectMocks
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    OptRecApproveService optRecApproveService = Mockito.spy(new OptRecApproveService());

    @Spy
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Spy
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        prepareACL();
        QueryHistoryAccelerateScheduler.getInstance().init();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        QueryHistoryAccelerateScheduler.shutdown();
    }

    private void prepareACL() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    public OptRecServiceIllegalMeasureTest() {
        super("../rec-service/src/test/resources/ut_rec_v2/illegal_measure",
                new String[] { "13aceef1-d9a1-4ec0-bb8e-b52f7bd3b99b" });
    }

    /**
     * 1. Following query statements were used to prepare recommendations.
     * [a]  select lo_orderkey, lo_extendedprice from ssb.p_lineorder group by lo_extendedprice, lo_orderkey
     * [b]  select lo_orderkey, lo_extendedprice * lo_quantity from ssb.p_lineorder
     * [c]  select lo_orderkey, lo_extendedprice * lo_quantity, lo_quantity from ssb.p_lineorder
     * [d]  select lo_orderkey, lo_tax, lo_quantity from ssb.p_lineorder
     * [e]  select lo_orderkey, sum(lo_extendedprice) from ssb.p_lineorder group by lo_orderkey
     * [f]  select lo_orderkey, lo_commitdate, sum(lo_extendedprice * lo_quantity), count(lo_quantity)
     *          from ssb.p_lineorder group by lo_orderkey, lo_commitdate
     * [g]  select lo_orderkey, lo_linenumber, sum(lo_extendedprice * lo_quantity), count(lo_shippriotity)
     *          from ssb.p_lineorder group by lo_orderkey, lo_linenumber
     * [h]  select lo_orderkey, lo_shipmode, sum(lo_extendedprice * lo_quantity), count(lo_tax)
     *          from ssb.p_lineorder group by lo_orderkey, lo_shipmode
     *
     * 2. Then put the column `lo_extendedprice` after `lo_orderkey`
     *    and reload table to prepare new recommendations again.
     * 3. Approve these recommendations. This process will validate these recommendations first,
     *    the comment after each query statement is the validation result.
     */
    @Test
    public void testApproveAll() throws IOException {
        /*
         * RawRecItem id greater than 22 is generated with TableDesc of `illegal_measure/table/SSB.P_LINEORDER.json`,
         */
        reloadTable();
        prepareAllLayoutRecs(Lists.newArrayList(15, 16, 17, 18, 19, 20, 21, 33, 34, 35, 36, 37, 38, 39));
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(0, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(3, modelBeforeApprove.getRecommendationsCount());
        Assert.assertEquals(0, getIndexPlan().getAllLayouts().size());
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "all");
        Assert.assertEquals(8, response.getLayouts().size());
        Set<Integer> brokenRecs = response.getBrokenRecs();
        Assert.assertEquals(11, brokenRecs.size());
        List<Integer> collect = brokenRecs.stream().sorted().collect(Collectors.toList());
        Assert.assertEquals(Lists.newArrayList(4, 5, 8, 13, 14, 15, 16, 18, 19, 20, 21), collect);

        optRecService.updateRecommendationCount(getProject(), getDefaultUUID());
        Assert.assertEquals(8, getModel().getRecommendationsCount());

        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll().stream() //
                .filter(RawRecItem::isLayoutRec).collect(Collectors.toList());
        Set<Integer> normalRecIdSet = Sets.newHashSet(33, 34, 35, 36, 37, 38, 39, 17);
        rawRecItems.forEach(recItem -> {
            if (normalRecIdSet.contains(recItem.getId())) {
                Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, recItem.getState());
            } else {
                Assert.assertEquals(RawRecItem.RawRecState.BROKEN, recItem.getState());
            }
        });

        List<OpenRecApproveResponse.RecToIndexResponse> recToIndexResponses = Lists.newArrayList();
        UnitOfWork.doInTransactionWithRetry(() -> {
            recToIndexResponses
                    .addAll(optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "all", false, false));
            return 0;
        }, getProject());

        Assert.assertEquals(1, recToIndexResponses.size());
        Assert.assertEquals(7, recToIndexResponses.get(0).getAddedIndexes().size());

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(8, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(6, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(7, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testChangeColumnOrder() throws IOException {
        /*
         * Without reload table, we mocked changing the column order, because RawRecItem with
         * id less then 22 is generated by table `ssb.p_lineorder`. Even if they will generate
         * same layout, it works.
         */
        prepareAllLayoutRecs(Lists.newArrayList(15, 16, 17, 18, 19, 20, 21, 33, 34, 35, 36, 37, 38, 39));
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(0, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(0, getIndexPlan().getAllLayouts().size());
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "all");
        Assert.assertEquals(14, response.getLayouts().size());
        jdbcRawRecStore.queryAll().forEach(rawRecItem -> {
            if (rawRecItem.isLayoutRec()) {
                Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, rawRecItem.getState());
            }
        });

        List<OpenRecApproveResponse.RecToIndexResponse> recToIndexResponses = Lists.newArrayList();
        UnitOfWork.doInTransactionWithRetry(() -> {
            recToIndexResponses
                    .addAll(optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "all", false, false));
            return 0;
        }, getProject());

        Assert.assertEquals(1, recToIndexResponses.size());
        Assert.assertEquals(7, recToIndexResponses.get(0).getAddedIndexes().size());

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(8, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(6, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(7, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testJustApproveNewVersionRawRecItems() throws IOException {
        prepareAllLayoutRecs(Lists.newArrayList(33, 34, 35, 36, 37, 38, 39));
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(0, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(0, getIndexPlan().getAllLayouts().size());
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "all");
        Assert.assertEquals(7, response.getLayouts().size());
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll().stream() //
                .filter(RawRecItem::isLayoutRec).collect(Collectors.toList());
        Set<Integer> normalRecIdSet = Sets.newHashSet(33, 34, 35, 36, 37, 38, 39);
        rawRecItems.forEach(recItem -> {
            if (normalRecIdSet.contains(recItem.getId())) {
                Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, recItem.getState());
            } else {
                Assert.assertEquals(RawRecItem.RawRecState.INITIAL, recItem.getState());
            }
        });

        List<OpenRecApproveResponse.RecToIndexResponse> recToIndexResponses = Lists.newArrayList();
        UnitOfWork.doInTransactionWithRetry(() -> {
            recToIndexResponses
                    .addAll(optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "all", false, false));
            return 0;
        }, getProject());

        Assert.assertEquals(1, recToIndexResponses.size());
        Assert.assertEquals(7, recToIndexResponses.get(0).getAddedIndexes().size());

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(8, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(6, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(7, getIndexPlan().getAllLayouts().size());
    }

    private void prepareAllLayoutRecs(List<Integer> layoutRawRecItemIdList) throws IOException {
        prepare(layoutRawRecItemIdList);
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }

    private void reloadTable() throws IOException {
        String basePath = getBasePath();
        String tableJson = "table/SSB.P_LINEORDER.json";
        TableDesc tableDesc = JsonUtil.readValue(new File(basePath, tableJson), TableDesc.class);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager.getInstance(getTestConfig(), getProject()).updateTableDesc(tableDesc.getIdentity(),
                    copyForWrite -> {
                        long mvcc = copyForWrite.getMvcc();
                        tableDesc.copyPropertiesTo(copyForWrite);
                        copyForWrite.setMvcc(mvcc);
                    });
            return null;
        }, getProject());
    }
}
