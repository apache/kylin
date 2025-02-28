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
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
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

public class OptRecServiceGeneralTest extends OptRecV2TestBase {
    @InjectMocks
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    OptRecApproveService optRecApproveService = Mockito.spy(new OptRecApproveService());
    @Spy
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public OptRecServiceGeneralTest() {
        super("../rec-service/src/test/resources/ut_rec_v2/general",
                new String[] { "cca38043-5e04-4954-b917-039ba37f159e" });
    }

    @Test
    public void testApprove() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableMap.of(100000, "COUNT_ALL", 100001, "MEASURE_AUTO_1"),
                extractIdToName(dataModel.getEffectiveMeasures()));
        Map<Long, FrequencyMap> layoutHitCount = NDataflowManager.getInstance(getTestConfig(), getProject())
                .getDataflow(getDefaultUUID()).getLayoutHitCount();
        Assert.assertEquals(1, layoutHitCount.size());
        Assert.assertEquals(1, layoutHitCount.get(10001L).getDateFrequency().size());
        Assert.assertEquals(ImmutableMap.of(1599580800000L, 1), layoutHitCount.get(10001L).getMap());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveModelWithDiscontinuousColumnId() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(6);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);

        // mock model with discontinuous column id
        final String modelId = getModel().getId();
        final NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<NDataModel.NamedColumn> allNamedColumns = copyForWrite.getAllNamedColumns();
            allNamedColumns.removeIf(namedColumn -> namedColumn.getId() == 4);
        });
        List<NDataModel.NamedColumn> columnList = modelManager.getDataModelDesc(modelId).getAllNamedColumns();
        Assert.assertEquals(17, columnList.size());

        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableMap.of(100000, "COUNT_ALL", 100001, "MEASURE_AUTO_1"),
                extractIdToName(dataModel.getEffectiveMeasures()));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(11, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testDiscard() throws Exception {
        List<Integer> addLayoutid = ImmutableList.of(3);
        List<Integer> removeLayout = Lists.newArrayList(8);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutid, removeLayout);
        prepare(addLayoutid);
        optRecService.discard(getProject(), recRequest);

        Assert.assertEquals(RawRecItem.RawRecState.DISCARD, jdbcRawRecStore.queryById(3).getState());
        Assert.assertEquals(RawRecItem.RawRecState.DISCARD, jdbcRawRecStore.queryById(8).getState());
    }

    @Test
    public void testApproveRemoveLayout() throws IOException {
        //addLayout
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        OptRecResponse optRecResponse = optRecApproveService.approve(getProject(), recRequest);
        Assert.assertEquals(1, optRecResponse.getAddedLayouts().size());
        Assert.assertEquals(0, optRecResponse.getRemovedLayouts().size());

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder().add(ImmutableList.of(1, 100000))
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        //remove
        List<Integer> removeLayoutId = Lists.newArrayList(8);
        OptRecRequest removeQuest = buildOptRecRequest(ImmutableList.of(), removeLayoutId);
        OptRecResponse removedResponse = optRecApproveService.approve(getProject(), removeQuest);
        Assert.assertEquals(0, removedResponse.getAddedLayouts().size());
        Assert.assertEquals(1, removedResponse.getRemovedLayouts().size());

        checkIndexPlan(ImmutableList.of(ImmutableList.of(0, 100000, 100001)), getIndexPlan());

        Assert.assertEquals(RawRecItem.RawRecState.APPLIED, jdbcRawRecStore.queryById(8).getState());
    }

    @Test
    public void testApproveWithRename() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, ImmutableMap.of(-2, "KE_TEST"));
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());
        Assert.assertEquals("KE_TEST", dataModel.getMeasureNameByMeasureId(100001));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveBatch() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)) //
                .add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveConcurrentBatch() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        Thread thread1 = new Thread(() -> optRecApproveService.approve(getProject(), recRequest));
        Thread thread2 = new Thread(() -> optRecApproveService.approve(getProject(), recRequest));
        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());
        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)) //
                .add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveBatchWithRename() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId, ImmutableMap.of(-2, "KE_TEST_1", -5, "KE_TEST_2"));
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());
        Assert.assertEquals("KE_TEST_1", dataModel.getMeasureNameByMeasureId(100001));
        Assert.assertEquals("KE_TEST_2", dataModel.getMeasureNameByMeasureId(100002));

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder().add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)) //
                .add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    @Test
    public void testApproveWithTwiceRequest() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(Lists.newArrayList(3));
        optRecApproveService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        addLayoutId = Lists.newArrayList(6);
        OptRecRequest recRequest2 = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest2);

        dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)) //
                .add(ImmutableList.of(11, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

    }

    @Test
    public void testApproveWithInverseTwiceRequest() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6);

        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(Lists.newArrayList(6));
        optRecApproveService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(11, 100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

        addLayoutId = Lists.newArrayList(3);
        OptRecRequest recRequest2 = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest2);

        dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(11, 100000, 100001)) //
                .add(ImmutableList.of(0, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());

    }

    @Test
    public void testApproveReuseDimAndMeasure() throws IOException {
        List<Integer> addLayoutId = Lists.newArrayList(3, 6, 7);

        prepare(addLayoutId);
        OptRecRequest recRequest1 = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest1);

        NDataModel dataModel = getModel();
        Assert.assertEquals(ImmutableSet.of(0, 1, 11), dataModel.getEffectiveDimensions().keySet());
        Assert.assertEquals(ImmutableSet.of(100000, 100001, 100002), dataModel.getEffectiveMeasures().keySet());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder() //
                .add(ImmutableList.of(1, 100000)) //
                .add(ImmutableList.of(0, 100000, 100001)) //
                .add(ImmutableList.of(11, 100000, 100002)) //
                .add(ImmutableList.of(0, 100000, 100002)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
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

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, List<Integer> removeLayoutId) {
        return buildOptRecRequest(addLayoutId, removeLayoutId, ImmutableMap.of());
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
