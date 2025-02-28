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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.response.OptRecResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

public class OptRecServiceCCTest extends OptRecV2TestBase {
    @InjectMocks
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    OptRecApproveService optRecApproveService = Mockito.spy(new OptRecApproveService());
    @Spy
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    public OptRecServiceCCTest() {
        super("../rec-service/src/test/resources/ut_rec_v2/CC",
                new String[] { "6b9a6f00-2154-479d-b68f-34e49e7f2389", "7de7c2e8-3be0-4081-ad88-3e1a34ca038e" });
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testApproveUseModelCC() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        OptRecResponse optRecResponse = optRecApproveService.approve(getProject(), recRequest);
        Assert.assertEquals(1, optRecResponse.getAddedLayouts().size());
        Assert.assertEquals(0, optRecResponse.getRemovedLayouts().size());

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(8), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableMap.of(100000, "COUNT_ALL", 100001, "MEASURE_AUTO_1"),
                extractIdToName(dataModel.getEffectiveMeasures()));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(8, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveProposeCC() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(7);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        OptRecResponse optRecResponse = optRecApproveService.approve(getProject(), recRequest);
        Assert.assertEquals(1, optRecResponse.getAddedLayouts().size());
        Assert.assertEquals(0, optRecResponse.getRemovedLayouts().size());

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        Assert.assertEquals(
                ImmutableMap.of("CC1", "`P_LINEORDER`.`V_REVENUE` * `P_LINEORDER`.`LO_QUANTITY`",
                        "CC_AUTO__1599630851750_1", "`P_LINEORDER`.`V_REVENUE` + `P_LINEORDER`.`LO_QUANTITY`"),
                extractInnerExpression(dataModel.getComputedColumnDescs()));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveReuseCrossModelCC() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(11);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(9), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        Assert.assertEquals(
                ImmutableMap.of("CC1", "`P_LINEORDER`.`V_REVENUE` * `P_LINEORDER`.`LO_QUANTITY`", "CC2",
                        "`P_LINEORDER`.`LO_DISCOUNT` * `P_LINEORDER`.`LO_REVENUE`"),
                extractInnerExpression(dataModel.getComputedColumnDescs()));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(9, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveWithRename() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(7);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, ImmutableMap.of(-4, "CC3", -6, "MEASURE_1"));
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());
        Assert.assertEquals("MEASURE_1", dataModel.getMeasureNameByMeasureId(100001));
        Assert.assertEquals("CC3", dataModel.getComputedColumnDescs().get(1).getColumnName());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveBatch() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 7);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 8), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(8, 100000, 100001)).add(ImmutableList.of(0, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveBatchWithRename() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 7);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, ImmutableMap.of(-2, "KE_TEST_1", -6, "KE_TEST_2"));
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 8), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());
        Assert.assertEquals("KE_TEST_1", dataModel.getMeasureNameByMeasureId(100001));
        Assert.assertEquals("KE_TEST_2", dataModel.getMeasureNameByMeasureId(100002));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(8, 100000, 100001)).add(ImmutableList.of(0, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveWithTwiceRequest() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 7, 11);
        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(Lists.newArrayList(3));
        optRecApproveService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(8), dataModel.getEffectiveDimensions().keySet());
        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(8, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        addLayoutId = Lists.newArrayList(7, 11);
        OptRecRequest recRequest2 = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest2);

        dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 8, 9), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002, 100003), dataModel.getEffectiveMeasures().keySet());

        layoutColOrder = ImmutableList.<List<Integer>> builder().add(ImmutableList.of(8, 100000, 100001))
                .add(ImmutableList.of(0, 100000, 100002)).add(ImmutableList.of(9, 100000, 100003)).build();

        Assert.assertEquals(
                ImmutableMap.of("CC1", "`P_LINEORDER`.`V_REVENUE` * `P_LINEORDER`.`LO_QUANTITY`", "CC2",
                        "`P_LINEORDER`.`LO_DISCOUNT` * `P_LINEORDER`.`LO_REVENUE`", "CC_AUTO__1599630851750_1",
                        "`P_LINEORDER`.`V_REVENUE` + `P_LINEORDER`.`LO_QUANTITY`"),
                extractInnerExpression(dataModel.getComputedColumnDescs()));
        checkIndexPlan(layoutColOrder, getIndexPlan());

    }

    @Test
    public void testApproveWithInverseTwiceRequest() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 7, 11);
        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(Lists.newArrayList(7, 11));
        optRecApproveService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 9), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(0, 100000, 100001)).add(ImmutableList.of(9, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        addLayoutId = Lists.newArrayList(3);
        OptRecRequest recRequest2 = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest2);

        dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 8, 9), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002, 100003), dataModel.getEffectiveMeasures().keySet());

        layoutColOrder = ImmutableList.<List<Integer>> builder().add(ImmutableList.of(0, 100000, 100001))
                .add(ImmutableList.of(9, 100000, 100002)).add(ImmutableList.of(8, 100000, 100003)).build();

        Assert.assertEquals(
                ImmutableMap.of("CC1", "`P_LINEORDER`.`V_REVENUE` * `P_LINEORDER`.`LO_QUANTITY`", "CC2",
                        "`P_LINEORDER`.`LO_DISCOUNT` * `P_LINEORDER`.`LO_REVENUE`", "CC_AUTO__1599630851750_1",
                        "`P_LINEORDER`.`V_REVENUE` + `P_LINEORDER`.`LO_QUANTITY`"),
                extractInnerExpression(dataModel.getComputedColumnDescs()));
        checkIndexPlan(layoutColOrder, getIndexPlan());

    }

    private Map<String, String> extractInnerExpression(List<ComputedColumnDesc> computedColumnDescs) {
        return computedColumnDescs.stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getColumnName, ComputedColumnDesc::getInnerExpression));
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }

    private void checkIndexPlan(List<List<Integer>> layoutColOrder, IndexPlan actualPlan) {
        Assert.assertEquals(layoutColOrder.size(), actualPlan.getAllLayouts().size());
        Assert.assertEquals(layoutColOrder,
                actualPlan.getAllLayouts().stream().map(LayoutEntity::getColOrder).collect(Collectors.toList()));
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), ImmutableMap.of());
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, Map<Integer, String> nameMap) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), nameMap);
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, List<Integer> removeLayoutId,
            Map<Integer, String> nameMap) {
        OptRecRequest recRequest = new OptRecRequest();
        recRequest.setModelId(getDefaultUUID());
        recRequest.setProject(getProject());
        recRequest.setRecItemsToAddLayout(addLayoutId);
        recRequest.setRecItemsToRemoveLayout(removeLayoutId);
        recRequest.setNames(nameMap);
        return recRequest;
    }
}
