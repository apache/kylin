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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.optimization.GarbageLayoutType;
import org.apache.kylin.metadata.cube.optimization.event.ApproveRecsEvent;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.ref.MeasureRef;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.OpenRecApproveResponse.RecToIndexResponse;
import org.apache.kylin.rest.response.OptRecDetailResponse;
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

import lombok.val;

public class OptRecServiceTest extends OptRecV2TestBase {
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

    public OptRecServiceTest() {
        super("../rec-service/src/test/resources/ut_rec_v2/opt_service",
                new String[] { "db89adb4-3aad-4f2a-ac2e-72ea0a30420b" });
    }

    @Test
    public void testGetOptRecRequestExcludeRecIfDeleteDependLayout() throws IOException {
        prepareAllLayoutRecs();
        OptRecLayoutsResponse recResp1 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(28, recResp1.getLayouts().size());

        // delete the depend layout
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        Set<Long> deletedLayouts = Sets.newHashSet();
        deletedLayouts.add(130001L);
        indexMgr.updateIndexPlan(getDefaultUUID(), copyForWrite -> {
            copyForWrite.removeLayouts(deletedLayouts, true, true);
        });
        OptRecLayoutsResponse recResp2 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(27, recResp2.getLayouts().size());
    }

    @Test
    public void testGetOptRecRequest() throws IOException {
        // test get all
        prepareAllLayoutRecs();
        OptRecLayoutsResponse recResp1 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(28, recResp1.getLayouts().size());
        assertOptRecDetailResponse(recResp1);

        // set topN to 50, get all and assert
        changeRecTopN(50);
        OptRecLayoutsResponse recResp2 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(), "ALL");
        Assert.assertEquals(35, recResp2.getLayouts().size());
        assertOptRecDetailResponse(recResp2);

        // test empty recTypeList
        OptRecLayoutsResponse recResp = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList(), null, false, "", 0, 10);
        Assert.assertEquals(10, recResp.getLayouts().size());
        assertOptRecDetailResponse(recResp);

        // only get add_table_index
        OptRecLayoutsResponse recResp3 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 0, 10);
        Assert.assertEquals(1, recResp3.getLayouts().size());
        assertOptRecDetailResponse(recResp3);

        // test limit
        OptRecLayoutsResponse recResp4 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX", "ADD_TABLE_INDEX"), null, false, "", 0, 30);
        Assert.assertEquals(27, recResp4.getLayouts().size());
        assertOptRecDetailResponse(recResp4);
        recResp4 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, false, "", 0, 20);
        Assert.assertEquals(20, recResp4.getLayouts().size());
        assertOptRecDetailResponse(recResp4);

        // test offset
        OptRecLayoutsResponse recResp5 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 1, 10);
        Assert.assertTrue(recResp5.getLayouts().isEmpty());
        assertOptRecDetailResponse(recResp5);
        recResp5 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_TABLE_INDEX"), null, false, "", 0, 10);
        Assert.assertEquals(1, recResp5.getLayouts().size());
        assertOptRecDetailResponse(recResp5);

        // test orderBy
        OptRecLayoutsResponse recResp6 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, false, "usage", 0, 30);
        Assert.assertEquals(89, recResp6.getLayouts().get(recResp6.getLayouts().size() - 1).getId());
        assertOptRecDetailResponse(recResp6);
        recResp6 = optRecService.getOptRecLayoutsResponse(getProject(), getDefaultUUID(),
                Lists.newArrayList("ADD_AGG_INDEX"), null, true, "usage", 0, 30);
        Assert.assertEquals(89, recResp6.getLayouts().get(0).getId());
        assertOptRecDetailResponse(recResp6);
    }

    private void assertOptRecDetailResponse(OptRecLayoutsResponse recResp) {
        recResp.getLayouts().forEach(layout -> {
            OptRecDetailResponse optRecDetailResponse = optRecService.getSingleOptRecDetail(getProject(),
                    getDefaultUUID(), layout.getId(), layout.isAdd());
            Assert.assertEquals(optRecDetailResponse, layout.getRecDetailResponse());
        });
    }

    private void prepareAllLayoutRecs() throws IOException {
        prepare(Lists.newArrayList(2, 3, 6, 10, 24, 59, 60, 61, 62, 76, 77, 78, 79, 80, 82, 83, 84, 85, 87, 88, 89, 91,
                92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104));
    }

    @Test
    public void getColumnCardinalities() throws IOException {
        prepareAllLayoutRecs();
        NDataModel model = modelManager.getDataModelDesc(getDefaultUUID());
        Map<TableRef, TableExtDesc> modelTables = optRecService.getModelTables(model);
        Assert.assertEquals(model.getAllTables().size(), modelTables.size());
    }

    @Test
    public void getDimensionCardinality() throws IOException {
        prepareAllLayoutRecs();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(kylinConfig, getProject());
        val tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, getProject());
        val model = modelManager.getDataModelDesc(getDefaultUUID());
        val tables = model.getAllTables();
        val modelTables = Maps.<TableRef, TableExtDesc> newHashMap();
        for (TableRef table : tables) {
            val tableDesc = table.getTableDesc();
            val tableExt = tableMetadataManager.getOrCreateTableExt(table.getTableDesc());
            List<TableExtDesc.ColumnStats> columnStats = Lists.newArrayList();
            for (int i = 0; i < tableDesc.getColumns().length; i++) {
                val columnDesc = tableDesc.getColumns()[i];
                if (columnDesc.isComputedColumn()) {
                    continue;
                }
                TableExtDesc.ColumnStats colStats = tableExt.getColumnStatsByName(columnDesc.getName());
                if (colStats == null) {
                    colStats = new TableExtDesc.ColumnStats();
                    colStats.setColumnName(columnDesc.getName());
                }
                if (i % 5 == 0) {
                    colStats.setCardinality(1000);
                } else if (i % 3 == 0) {
                    colStats.setCardinality(100);
                } else if (i % 2 == 0) {
                    colStats.setCardinality(10);
                }
                columnStats.add(colStats);
            }
            tableExt.setColumnStats(columnStats);
            tableMetadataManager.saveTableExt(tableExt);
            modelTables.put(table, tableExt);
        }

        val measureRef = new MeasureRef();
        val measureRefCardinalities = OptRecService.getRecCardinality(measureRef, model, modelTables);
        assertNull(measureRefCardinalities);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        assertEquals(17, optRecV2.getDimensionRefs().size());
        val dimensionRefCardinalities = optRecV2.getDimensionRefs().values().stream()
                .map(ref -> OptRecService.getRecCardinality(ref, model, modelTables)).filter(Objects::nonNull)
                .collect(Collectors.toList());
        val cardinalities1 = dimensionRefCardinalities.stream().filter(cardinality -> cardinality == 10).count();
        assertEquals(5, cardinalities1);
        val cardinalities2 = dimensionRefCardinalities.stream().filter(cardinality -> cardinality == 100).count();
        assertEquals(4, cardinalities2);
        val cardinalities3 = dimensionRefCardinalities.stream().filter(cardinality -> cardinality == 1000).count();
        assertEquals(4, cardinalities3);
    }

    @Test
    public void testApproveAll() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "all", false, false);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(56, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(21, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveDiscardTableIndex() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "all", false, true);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(56, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());
        Object[] array = getIndexPlan().getAllIndexes().stream().filter(IndexEntity::isTableIndex).toArray();
        Assert.assertEquals(19, getIndexPlan().getAllIndexes().size());
        Assert.assertEquals(0, array.length);
    }

    @Test
    public void testApproveAllRemovalRecItems() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "REMOVE_INDEX", false, false);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(7, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(1, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveAllAdditionalRecItems() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());
        Assert.assertEquals(9, getIndexPlan().getAllLayouts().size());

        changeRecTopN(50);
        optRecApproveService.batchApprove(getProject(), Lists.newArrayList(), "ADD_INDEX", false, false);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());
        Assert.assertEquals(36, getIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testApproveOneModel() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        List<NDataModelResponse> modelResponses = modelService.getModels(
                modelBeforeApprove.getAlias().toLowerCase(Locale.ROOT), getProject(), true, null, null, "last_modify",
                true);
        List<String> modelIds = modelResponses.stream().map(NDataModelResponse::getUuid).collect(Collectors.toList());

        changeRecTopN(50);
        List<RecToIndexResponse> responses = optRecApproveService.batchApprove(getProject(), modelIds, "all", true,
                false);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());

        Assert.assertEquals(1, responses.size());
        RecToIndexResponse recToIndexResponse = responses.get(0);
        Assert.assertEquals("db89adb4-3aad-4f2a-ac2e-72ea0a30420b", recToIndexResponse.getModelId());
        Assert.assertEquals("m0", recToIndexResponse.getModelAlias());
        Assert.assertEquals(27, recToIndexResponse.getAddedIndexes().size());
        Assert.assertEquals(8, recToIndexResponse.getRemovedIndexes().size());
    }

    @Test
    public void testApproveOneModelWithUpperCase() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        List<NDataModelResponse> modelResponses = modelService.getModels(
                modelBeforeApprove.getAlias().toUpperCase(Locale.ROOT), getProject(), true, null, null, "last_modify",
                true);
        List<String> modelIds = modelResponses.stream().map(NDataModelResponse::getUuid).collect(Collectors.toList());

        changeRecTopN(50);
        List<RecToIndexResponse> responses = optRecApproveService.batchApprove(getProject(), modelIds, "all", true,
                false);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(19, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(58, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(2, modelAfterApprove.getComputedColumnDescs().size());

        Assert.assertEquals(1, responses.size());
        RecToIndexResponse recToIndexResponse = responses.get(0);
        Assert.assertEquals("db89adb4-3aad-4f2a-ac2e-72ea0a30420b", recToIndexResponse.getModelId());
        Assert.assertEquals("m0", recToIndexResponse.getModelAlias());
        Assert.assertEquals(27, recToIndexResponse.getAddedIndexes().size());
        Assert.assertEquals(8, recToIndexResponse.getRemovedIndexes().size());
    }

    @Test
    public void testApproveIllegalLayoutRecommendation() throws IOException {
        prepareAllLayoutRecs();
        NDataModel modelBeforeApprove = getModel();
        Assert.assertEquals(7, modelBeforeApprove.getEffectiveDimensions().size());
        Assert.assertEquals(17, modelBeforeApprove.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeApprove.getEffectiveMeasures().size());
        Assert.assertEquals(0, modelBeforeApprove.getComputedColumnDescs().size());

        prepareAbnormalLayoutRecommendation();

        RawRecItem rawRecItem = jdbcRawRecStore.queryAll().stream() //
                .filter(recItem -> recItem.getId() == 6) //
                .findAny().orElse(null);
        Assert.assertNotNull(rawRecItem);
        List<Integer> dependIds = Lists.newArrayList();
        for (int dependID : rawRecItem.getDependIDs()) {
            dependIds.add(dependID);
        }
        Assert.assertEquals(Lists.newArrayList(4, 16, 14, 100000, -1, -1), dependIds);

        List<NDataModelResponse> modelResponses = modelService.getModels(
                modelBeforeApprove.getAlias().toLowerCase(Locale.ROOT), getProject(), true, null, null, "last_modify",
                true);
        List<String> modelIds = modelResponses.stream().map(NDataModelResponse::getUuid).collect(Collectors.toList());

        changeRecTopN(50);
        List<RecToIndexResponse> responses = optRecApproveService.batchApprove(getProject(), modelIds, "all", true,
                false);

        NDataModel modelAfterApprove = getModel();
        Assert.assertEquals(17, modelAfterApprove.getEffectiveDimensions().size());
        Assert.assertEquals(18, modelAfterApprove.getAllNamedColumns().size());
        Assert.assertEquals(57, modelAfterApprove.getEffectiveMeasures().size());
        Assert.assertEquals(1, modelAfterApprove.getComputedColumnDescs().size());

        Assert.assertEquals(1, responses.size());
        RecToIndexResponse recToIndexResponse = responses.get(0);
        Assert.assertEquals("db89adb4-3aad-4f2a-ac2e-72ea0a30420b", recToIndexResponse.getModelId());
        Assert.assertEquals("m0", recToIndexResponse.getModelAlias());
        Assert.assertEquals(26, recToIndexResponse.getAddedIndexes().size());
        Assert.assertEquals(8, recToIndexResponse.getRemovedIndexes().size());
    }

    @Test
    public void testApproveRawRecItemsByHitCount() throws IOException {
        prepareAllLayoutRecs();
        String project = getProject();
        NDataModel modelBeforeApprove = getModel();

        overwriteSystemProp("kylin.index.expected-size-after-optimization", "10");
        Map<NDataflow, Map<Long, GarbageLayoutType>> needOptAggressivelyModels = Maps.newHashMap();
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow(modelBeforeApprove.getUuid());

        needOptAggressivelyModels.put(dataflow, new HashMap<>());
        ApproveRecsEvent approveRecsEvent = new ApproveRecsEvent(project, needOptAggressivelyModels);
        optRecApproveService.approveRawRecItemsByHitCount(approveRecsEvent);
        Assert.assertEquals(10, dataflow.getIndexPlan().getIndexes().size());
    }

    private void prepareAbnormalLayoutRecommendation() {
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.forEach(recItem -> {
            // prepare an abnormal RawRecItem
            if (recItem.getId() == 6) {
                final LayoutRecItemV2 recEntity = (LayoutRecItemV2) recItem.getRecEntity();
                recEntity.getLayout().setColOrder(Lists.newArrayList(4, 16, 14, 100000, -1, -1));
                recItem.setDependIDs(new int[] { 4, 16, 14, 100000, -1, -1 });
            }
        });
        jdbcRawRecStore.batchAddOrUpdate(rawRecItems);
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }
}
