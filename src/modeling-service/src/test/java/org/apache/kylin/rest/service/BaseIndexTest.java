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

import static org.apache.kylin.metadata.cube.model.IndexEntity.isAggIndex;
import static org.apache.kylin.metadata.cube.model.IndexEntity.isTableIndex;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.Source;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModel.NamedColumn;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.request.CreateBaseIndexRequest;
import org.apache.kylin.rest.request.CreateBaseIndexRequest.LayoutProperty;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.IndexStatResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.var;

public class BaseIndexTest extends SourceTestCase {

    private static final String COMMON_MODEL_ID = "b780e4e4-69af-449e-b09f-05c90dfa04b6";

    @InjectMocks
    protected final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.setup();
        indexPlanService.setSemanticUpater(semanticService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Test
    public void testCreateBaseLayout() {
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        indexPlanService.createBaseIndex(getProject(), request);
        compareBaseIndex(COMMON_MODEL_ID, LayoutBuilder.builder().colOrder(0, 1, 2, 3).build(),
                LayoutBuilder.builder().colOrder(0, 1, 2, 3, 100000, 100001).build());
    }

    @Test
    public void testCreateEmptyBaseTableLayout() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest modelRequest = FormModel(modelManager.getDataModelDesc(COMMON_MODEL_ID));
        modelRequest.setDimensions(Lists.newArrayList());
        modelRequest.setMeasures(modelRequest.getAllMeasures().subList(0, 0));
        String modelId = modelService.createModel(modelRequest.getProject(), modelRequest).getId();
        modelService.updateDataModelSemantic(getProject(), modelRequest);
        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(10000).build();
        LayoutEntity baseTableLayout = null;
        compareBaseIndex(getModelIdFrom(modelRequest.getAlias()), baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateEmptyBaseTableLayoutWithSecondStorage() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest modelRequest = FormModel(modelManager.getDataModelDesc(COMMON_MODEL_ID));
        modelRequest.setDimensions(Lists.newArrayList());
        modelRequest.setMeasures(modelRequest.getAllMeasures().subList(0, 0));
        String modelId = modelService.createModel(modelRequest.getProject(), modelRequest).getId();
        modelRequest.setWithSecondStorage(true);
        BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(modelRequest, true);
        baseIndexUpdater.setSecondStorageEnabled(true);
        BuildBaseIndexResponse baseIndexResponse = baseIndexUpdater.update(indexPlanService);
        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(10000).build();
        LayoutEntity baseTableLayout = null;
        compareBaseIndex(getModelIdFrom(modelRequest.getAlias()), baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateBaseLayoutWithProperties() {
        // create base index is same with index in rulebaseindex or indexes
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        LayoutProperty tableLayoutProperty = LayoutProperty.builder()
                .colOrder(Lists.newArrayList("TEST_BANK_INCOME.DT", "TEST_BANK_INCOME.COUNTRY",
                        "TEST_BANK_INCOME.INCOME", "TEST_BANK_INCOME.NAME"))
                .shardByColumns(Lists.newArrayList("TEST_BANK_INCOME.COUNTRY")).build();

        LayoutProperty aggLayoutProperty = LayoutProperty.builder()
                .colOrder(Lists.newArrayList("TEST_BANK_INCOME.DT", "TEST_BANK_INCOME.COUNTRY",
                        "TEST_BANK_INCOME.INCOME", "TEST_BANK_INCOME.NAME"))
                .shardByColumns(Lists.newArrayList("TEST_BANK_INCOME.COUNTRY")).build();
        request.setBaseAggIndexProperty(aggLayoutProperty);
        request.setBaseTableIndexProperty(tableLayoutProperty);

        indexPlanService.createBaseIndex(getProject(), request);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(3, 0, 1, 2, 100000, 100001).shardByColumns(0)
                .build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(3, 0, 1, 2).shardByColumns(0).build();
        compareBaseIndex(COMMON_MODEL_ID, baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateBaseLayoutSameWithWhiteList() {
        CreateTableIndexRequest tableIndexRequest = new CreateTableIndexRequest();
        tableIndexRequest.setProject(getProject());
        tableIndexRequest.setModelId(COMMON_MODEL_ID);
        tableIndexRequest.setColOrder(Lists.newArrayList("TEST_BANK_INCOME.COUNTRY", "TEST_BANK_INCOME.INCOME",
                "TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.DT"));
        indexPlanService.createTableIndex(getProject(), tableIndexRequest);
        long id = NIndexPlanManager.getInstance(getTestConfig(), getProject()).getIndexPlan(COMMON_MODEL_ID)
                .getIndexes().get(0).getLastLayout().getId();

        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        indexPlanService.createBaseIndex(getProject(), request);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 100000, 100001).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3).id(id).build();
        compareBaseIndex(COMMON_MODEL_ID, baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateBaseIndexSameWithWhiteList() {
        CreateTableIndexRequest tableIndexRequest = new CreateTableIndexRequest();
        tableIndexRequest.setProject(getProject());
        tableIndexRequest.setModelId(COMMON_MODEL_ID);
        tableIndexRequest.setColOrder(Lists.newArrayList("TEST_BANK_INCOME.INCOME", "TEST_BANK_INCOME.COUNTRY",
                "TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.DT"));
        indexPlanService.createTableIndex(getProject(), tableIndexRequest);
        long id = NIndexPlanManager.getInstance(getTestConfig(), getProject()).getIndexPlan(COMMON_MODEL_ID)
                .getIndexes().get(0).getLastLayout().getId();

        // create base index is same with index in rulebaseindex or indexes
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        indexPlanService.createBaseIndex(getProject(), request);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 100000, 100001).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3).id(id + 1).build();
        compareBaseIndex(COMMON_MODEL_ID, baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateBaseIndexSameWithToBeDelete() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexPlanManager.updateIndexPlan(COMMON_MODEL_ID, copyForWrite -> {
            copyForWrite.markIndexesToBeDeleted(copyForWrite.getId(),
                    copyForWrite.getAllLayouts().stream().collect(Collectors.toSet()));

        });

        // create base index is same with index in rulebaseindex or indexes
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        LayoutProperty aggLayoutProperty = LayoutProperty.builder().colOrder(Lists.newArrayList("TEST_BANK_INCOME.DT",
                "TEST_BANK_INCOME.COUNTRY", "TEST_BANK_INCOME.INCOME", "TEST_BANK_INCOME.NAME")).build();
        request.setBaseAggIndexProperty(aggLayoutProperty);
        indexPlanService.createBaseIndex(getProject(), request);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(3, 0, 1, 2, 100000, 100001).id(110002L).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3).build();
        compareBaseIndex(COMMON_MODEL_ID, baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateBaseLayoutSameWithToBeDelete() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexPlanManager.updateIndexPlan(COMMON_MODEL_ID, copyForWrite -> {
            copyForWrite.markIndexesToBeDeleted(copyForWrite.getId(),
                    copyForWrite.getAllLayouts().stream().collect(Collectors.toSet()));

        });
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        indexPlanService.createBaseIndex(getProject(), request);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 100000, 100001).id(110001L).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3).build();
        compareBaseIndex(COMMON_MODEL_ID, baseTableLayout, baseAggLayout);
    }

    @Test
    public void testCreateModelWithBaseIndex() {
        String modelId = createBaseIndexFromModel(COMMON_MODEL_ID);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 100000, 100001).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3).build();
        compareBaseIndex(modelId, baseTableLayout, baseAggLayout);

        var baseIndexResponse = indexPlanService.getIndexes(getProject(), modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false,
                Lists.newArrayList(Source.BASE_AGG_INDEX, Source.BASE_TABLE_INDEX), null);
        Assert.assertThat(baseIndexResponse.size(), is(2));
    }

    @Test
    public void testBatchCreateModel() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest modelRequest = FormModel(modelManager.getDataModelDesc(COMMON_MODEL_ID));
        modelRequest.setUuid(System.currentTimeMillis() + "");
        IndexPlan indexPlan = new IndexPlan();
        indexPlan.setUuid(modelRequest.getUuid());
        modelRequest.setIndexPlan(indexPlan);
        modelService.batchCreateModel(getProject(), Lists.newArrayList(modelRequest), Lists.newArrayList());

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 100000, 100001).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3).build();
        compareBaseIndex(getModelIdFrom(modelRequest.getAlias()), baseTableLayout, baseAggLayout);

    }

    @Test
    public void testUpdateBaseIndex() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest modelRequest = FormModel(modelManager.getDataModelDesc(COMMON_MODEL_ID));
        String modelId = modelService.createModel(modelRequest.getProject(), modelRequest).getId();

        Assert.assertThat(needUpdateBaseIndex(getProject(), modelId), is(false));
        addDimension(modelRequest, Lists.newArrayList(5, 6));
        BuildBaseIndexResponse response = modelService.updateDataModelSemantic(getProject(), modelRequest);

        Assert.assertThat(needUpdateBaseIndex(getProject(), modelId), is(false));

        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(modelId);

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 5, 6, 100000, 100001).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 5, 6).build();
        compareBaseIndex(getModelIdFrom(modelRequest.getAlias()), baseTableLayout, baseAggLayout);
    }

    @Test
    public void testUpdateAndBuildBaseIndex() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest modelRequest = FormModel(modelManager.getDataModelDesc(COMMON_MODEL_ID));
        //just for create segment when creating modeling
        modelRequest.setPartitionDesc(null);
        modelRequest.setMultiPartitionDesc(null);
        modelRequest.setMultiPartitionKeyMapping(null);
        String modelId = modelService.createModel(modelRequest.getProject(), modelRequest).getId();

        Assert.assertThat(needUpdateBaseIndex(getProject(), modelId), is(false));
        addDimension(modelRequest, Lists.newArrayList(5, 6));
        BuildBaseIndexResponse response = modelService.updateDataModelSemantic(getProject(), modelRequest);

        Assert.assertThat(needUpdateBaseIndex(getProject(), modelId), is(false));

        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(modelId);
        Assert.assertThat(getBaseAggIndex(modelId), notNullValue());
        Assert.assertThat(getBaseTableIndex(modelId), notNullValue());
        List<AbstractExecutable> executables = getRunningExecutables(getProject(), modelId);
        Assert.assertThat(executables.size(), is(1));
        Assert.assertThat(getProcessLayout(executables.get(0)), is(2));
    }

    private int getProcessLayout(AbstractExecutable executable) {
        String layouts = executable.getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isBlank(layouts)) {
            return 0;
        }
        return layouts.split(",").length;
    }

    @Test
    public void testNotUpdateBaseLayoutWithSameCol() {
        testCreateBaseLayoutWithProperties();
        Assert.assertThat(needUpdateBaseIndex(getProject(), COMMON_MODEL_ID), is(false));
    }

    private boolean needUpdateBaseIndex(String project, String modelId) {
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject()).getIndexPlan(modelId);
        NDataModel model = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);

        if (indexPlan.needUpdateBaseAggLayout(indexPlan.createBaseAggIndex(model), true)) {
            return true;
        }
        if (indexPlan.needUpdateBaseTableLayout(indexPlan.createBaseTableIndex(model), true)) {
            return true;
        }

        return false;
    }

    @Test
    public void testUpdateBuiltBaseIndex() {
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setModelId(COMMON_MODEL_ID);
        indexPlanService.createBaseIndex(getProject(), request);
        Assert.assertThat(needUpdateBaseIndex(getProject(), COMMON_MODEL_ID), is(false));

        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest updateModelRequest = FormModel(modelManager.getDataModelDesc(COMMON_MODEL_ID));
        updateModelRequest.setUuid(COMMON_MODEL_ID);
        addDimension(updateModelRequest, Lists.newArrayList(5, 6));
        modelService.updateDataModelSemantic(getProject(), updateModelRequest);

        CreateBaseIndexRequest updateRequest = new CreateBaseIndexRequest();
        updateRequest.setModelId(COMMON_MODEL_ID);

        LayoutEntity beforeBaseLayout = NIndexPlanManager.getInstance(getTestConfig(), getProject())
                .getIndexPlan(COMMON_MODEL_ID).getToBeDeletedIndexes().get(0).getLayouts().get(0);
        Assert.assertThat(beforeBaseLayout.getColOrder(), is(ImmutableList.of(0, 1, 2, 3, 100000, 100001)));

        LayoutEntity baseAggLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 5, 6, 100000, 100001).build();
        LayoutEntity baseTableLayout = LayoutBuilder.builder().colOrder(0, 1, 2, 3, 5, 6).build();
        compareBaseIndex(COMMON_MODEL_ID, baseTableLayout, baseAggLayout);
    }

    @Test
    public void testGetIndexesStat() {
        IndexStatResponse indexStat = indexPlanService.getStat(getProject(), COMMON_MODEL_ID);
        Assert.assertEquals(true, indexStat.isNeedCreateBaseAggIndex());
        Assert.assertEquals(true, indexStat.isNeedCreateBaseAggIndex());
    }

    private String getModelIdFrom(String alias) {
        return NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDescByAlias(alias).getId();
    }

    private void addDimension(ModelRequest modelRequest, ArrayList<Integer> dims) {
        List<NamedColumn> dimCols = modelRequest.getAllNamedColumns().stream()
                .filter(col -> col.isDimension() || dims.contains(col.getId())).collect(Collectors.toList());
        modelRequest.setSimplifiedDimensions(dimCols);

    }

    private LayoutEntity getBaseTableIndex(String modelId) {
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject()).getIndexPlan(modelId);
        return indexPlan.getAllLayouts().stream()
                .filter(layoutEntity -> layoutEntity.isBase() && isTableIndex(layoutEntity.getId())).findFirst()
                .orElse(null);
    }

    private LayoutEntity getBaseAggIndex(String modelId) {
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject()).getIndexPlan(modelId);
        return indexPlan.getAllLayouts().stream()
                .filter(layoutEntity -> layoutEntity.isBase() && isAggIndex(layoutEntity.getId())).findFirst()
                .orElseGet(null);
    }

    private String createBaseIndexFromModel(String modelId) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        ModelRequest modelRequest = FormModel(modelManager.getDataModelDesc(modelId));
        return modelService.createModel(modelRequest.getProject(), modelRequest).getId();
    }

    private void compareBaseIndex(String indexPlanId, LayoutEntity baseTableLayout, LayoutEntity baseAggLayout) {
        LayoutEntity expectedBaseTableLayout = getBaseTableIndex(indexPlanId);
        LayoutEntity expectedBaseAggLayout = getBaseAggIndex(indexPlanId);
        if (expectedBaseTableLayout == null && baseTableLayout == null) {
            return;
        }
        Assert.assertThat(baseAggLayout.getColOrder(), equalTo(expectedBaseAggLayout.getColOrder()));
        Assert.assertThat(baseAggLayout.getShardByColumns(), equalTo(expectedBaseAggLayout.getShardByColumns()));
        Assert.assertThat(baseAggLayout.getSortByColumns(), equalTo(expectedBaseAggLayout.getSortByColumns()));
        Assert.assertThat(baseTableLayout.getColOrder(), equalTo(expectedBaseTableLayout.getColOrder()));
        Assert.assertThat(baseTableLayout.getShardByColumns(), equalTo(expectedBaseTableLayout.getShardByColumns()));
        Assert.assertThat(baseTableLayout.getSortByColumns(), equalTo(expectedBaseTableLayout.getSortByColumns()));
        if (baseAggLayout.getId() != -1) {
            Assert.assertEquals(expectedBaseAggLayout.getId(), baseAggLayout.getId());
        }
        if (baseTableLayout.getId() != -1) {
            Assert.assertEquals(expectedBaseTableLayout.getId(), baseTableLayout.getId());

        }
    }

    public static ModelRequest FormModel(NDataModel dataModel) {
        ModelRequest request = new ModelRequest(dataModel);
        request.setDimensions(
                dataModel.getAllNamedColumns().stream().filter(NamedColumn::isDimension).collect(Collectors.toList()));
        request.setProject(dataModel.getProject());
        request.setMeasures(dataModel.getAllMeasures());
        request.setUuid(null);
        request.setAlias(System.currentTimeMillis() + "");
        request.setWithBaseIndex(true);
        return request;
    }

    private static class LayoutBuilder {

        long id = -1;

        private List<Integer> colOrder = Lists.newArrayList();

        private List<Integer> shardByColumns = Lists.newArrayList();

        public static LayoutBuilder builder() {
            return new LayoutBuilder();
        }

        LayoutEntity build() {
            LayoutEntity layout = new LayoutEntity();
            layout.setColOrder(colOrder);
            layout.setShardByColumns(shardByColumns);
            layout.setId(id);
            return layout;
        }

        public LayoutBuilder shardByColumns(Integer... ids) {
            this.shardByColumns = toList(ids);
            return this;
        }

        public LayoutBuilder colOrder(Integer... ids) {
            this.colOrder = toList(ids);
            return this;
        }

        private List<Integer> toList(Integer[] ids) {
            return Arrays.asList(ids);
        }

        public LayoutBuilder id(long id) {
            this.id = id;
            return this;
        }
    }
}
