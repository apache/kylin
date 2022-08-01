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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.TABLE_RELOAD_HAVING_NOT_FINAL_JOB;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDictionaryDesc;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.S3TableExtInfo;
import org.apache.kylin.rest.response.OpenPreReloadTableResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.clickhouse.MockSecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableReloadServiceTest extends CSVSourceTestCase {

    private static final String PROJECT = "default";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Autowired
    private TableService tableService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    private final MockModelQueryService modelQueryService = Mockito.spy(new MockModelQueryService());

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    @Before
    @Override
    public void setup() {
        super.setup();
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        try {
            setupPushdownEnv();
        } catch (Exception ignore) {
        }
        EventBusFactory.getInstance().register(modelBrokenListener, false);
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.updateIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream().peek(i -> {
                if (i.getId() == 0) {
                    i.setLayouts(Lists.newArrayList(i.getLayouts().get(0)));
                }
            }).collect(Collectors.toList()));
        });
        NTableMetadataManager.getInstance(getTestConfig(), PROJECT);

    }

    @After
    @Override
    public void cleanup() {
        try {
            cleanPushdownEnv();
        } catch (Exception ignore) {
        }
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        super.cleanup();
    }

    @Test
    public void testPreProcess_AffectTwoTables() throws Exception {
        removeColumn("DEFAULT.TEST_COUNTRY", "NAME");

        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(1, response.getRemoveColumnCount());
        // affect dimension:
        //     ut_inner_join_cube_partial: 21,25
        //     nmodel_basic: 21,25,29,30
        //     nmodel_basic_inner: 21,25
        //     all_fixed_length: 21,25
        Assert.assertEquals(10, response.getRemoveDimCount());
        Assert.assertEquals(18, response.getRemoveLayoutsCount());
    }

    @Test
    public void testPreProcess_AffectByCC() throws Exception {
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "PRICE");

        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(1, response.getRemoveColumnCount());

        // affect dimension:
        //     nmodel_basic: 27,33,34,35,36,38
        //     nmodel_basic_inner: 27,29,30,31,32
        //     all_fixed_length: 11
        Assert.assertEquals(12, response.getRemoveDimCount());

        // affect measure:
        //     ut_inner_join_cube_partial: 100001,100002,100003,100009,100011
        //     nmodel_basic: 100001,100002,100003,100009,100011,100013,100016,100015
        //     nmodel_basic_inner: 100001,100002,100003,100009,100011,100013,100016,100015
        //     all_fixed_length: 100001,100002,100003,100009,100011
        Assert.assertEquals(26, response.getRemoveMeasureCount());
        // affect table index:
        // IndexPlan [741ca86a-1f13-46da-a59f-95fb68615e3a(nmodel_basic_inner)]: 20000000000
        // IndexPlan [89af4ee2-2cdb-4b07-b39e-4c29856309aa(nmodel_basic)]: 20000000000
        Assert.assertEquals(58, response.getRemoveLayoutsCount());
    }

    @Test
    public void testPreProcess_RefreshCount() throws Exception {
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("PRICE", "bigint");
            }
        }, true);

        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(58, response.getRefreshLayoutsCount());
    }

    @Test
    public void testPreProcess_ChangeCCType() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        var model = modelManager.getDataModelDescByAlias("nmodel_basic");
        Assert.assertEquals("decimal(30,4)", model.getComputedColumnDescs().get(0).getDatatype());
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("PRICE", "bigint");
            }
        }, true);
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", false, null);
        model = modelManager.getDataModelDescByAlias("nmodel_basic");
        Assert.assertEquals("BIGINT", model.getComputedColumnDescs().get(0).getDatatype());
    }

    private void dropModelWhen(Predicate<String> predicate) {
        modelService.listAllModelIdsInProject(PROJECT).stream().filter(predicate)
                .forEach(id -> modelService.innerDropModel(id, PROJECT));
    }

    @Test
    public void testReload_RemoveCC() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        var originModel = modelManager.getDataModelDescByAlias("nmodel_basic");
        val originSize = originModel.getComputedColumnDescs().size();
        dropModelWhen(id -> !id.equals(originModel.getId()));
        val request = modelService.convertToRequest(originModel);
        request.setProject(PROJECT);
        val cc1 = new ComputedColumnDesc();
        cc1.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        cc1.setTableAlias("TEST_KYLIN_FACT");
        cc1.setColumnName("RELOAD_CC1");
        cc1.setExpression("\"TEST_KYLIN_FACT\".\"TRANS_ID\" + 1");
        cc1.setDatatype("INTEGER");
        val cc2 = new ComputedColumnDesc();
        cc2.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        cc2.setTableAlias("TEST_KYLIN_FACT");
        cc2.setColumnName("RELOAD_CC2");
        cc2.setExpression("\"TEST_KYLIN_FACT\".\"RELOAD_CC1\" + 1");
        cc2.setDatatype("INTEGER");
        request.getComputedColumnDescs().add(cc1);
        request.getComputedColumnDescs().add(cc2);
        modelService.updateDataModelSemantic(PROJECT, request);
        var modifiedModel = modelManager.getDataModelDesc(originModel.getId());
        Assert.assertEquals(originSize + 2, modifiedModel.getComputedColumnDescs().size());
        Assert.assertTrue(modifiedModel.getComputedColumnDescs().stream()
                .anyMatch(cc -> cc.getColumnName().equals("RELOAD_CC2")));
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "TRANS_ID");
        tableService.reloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", false, 0, false);
        var reloadedModel = modelManager.getDataModelDesc(originModel.getId());
        Assert.assertEquals(originSize, reloadedModel.getComputedColumnDescs().size());
    }

    @Test
    public void testReload_RemoveMeasureAffectedAggGroup() throws Exception {
        val MODEL_ID = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        modelManager.listAllModels().forEach(model -> {
            if (!model.getId().equals(MODEL_ID)) {
                modelService.dropModel(model.getId(), PROJECT);
            }
        });
        modelManager.updateDataModel(MODEL_ID, copyForWrite -> {
            copyForWrite.setPartitionDesc(null);
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            for (NDataModel.NamedColumn column : copyForWrite.getAllNamedColumns()) {
                if (column.getId() == 11) {
                    column.setStatus(NDataModel.ColumnStatus.DIMENSION);
                }
            }
        });
        val df = dfManager.updateDataflow(MODEL_ID, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        dfManager.fillDfManually(df, Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.updateIndexPlan(MODEL_ID, copyForWrite -> {
            copyForWrite.setRuleBasedIndex(JsonUtil.readValueQuietly(("{\n"//
                    + "    \"dimensions\" : [ 9, 3, 11 ],\n" //
                    + "    \"measures\" : [ 100012, 100008, 100001 ],\n"//
                    + "    \"global_dim_cap\" : null,\n" //
                    + "    \"aggregation_groups\" : [ {\n"//
                    + "      \"includes\" : [ 9, 3, 11 ],\n" //
                    + "      \"measures\" : [  100012, 100008, 100001 ],\n"//
                    + "      \"select_rule\" : {\n" //
                    + "        \"hierarchy_dims\" : [ ],\n"//
                    + "        \"mandatory_dims\" : [ ],\n" //
                    + "        \"joint_dims\" : [ ]\n"//
                    + "      }\n"//
                    + "    } ],\n" //
                    + "    \"scheduler_version\" : 2\n"//
                    + "  }").getBytes(StandardCharsets.UTF_8), RuleBasedIndex.class));
            copyForWrite.setIndexes(Lists.newArrayList());
            copyForWrite.getRuleBasedIndex().setIndexPlan(copyForWrite);
        });
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "PRICE");
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("LSTG_FORMAT_NAME", "int");
            }
        }, false);

        val jobs = tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        val execManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        val executables = execManager.getRunningExecutables(PROJECT, MODEL_ID);
        val indexPlan = indexManager.getIndexPlan(MODEL_ID);
        Assert.assertEquals(
                Joiner.on(",")
                        .join(indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList())),
                ((NSparkCubingJob) executables.get(0)).getTasks().get(0).getParam("layoutIds"));
    }

    @Test
    public void testCleanupToBeDeleted_afterChangeType() throws Exception {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val project = "default";

        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val saved = indexManager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.setIndexes(Lists.newArrayList());
            RuleBasedIndex ruleBasedIndex = JsonUtil.readValueQuietly(("{" //
                    + "    \"dimensions\": [ 0, 1, 2, 3, 4, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 28, 29, 30, 31, 32, 33 ],\n" //
                    + "    \"measures\": [ 100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016 ],\n" //
                    + "    \"aggregation_groups\": [\n" + "      {\n" //
                    + "        \"includes\": [ 2, 8, 3, 4, 16, 33, 6, 7 ],\n" //
                    + "        \"select_rule\": {\n" //
                    + "          \"hierarchy_dims\": [\n" //
                    + "            [ 33, 6, 7, 8 ]\n" //
                    + "          ],\n" //
                    + "          \"mandatory_dims\": [],\n" //
                    + "          \"joint_dims\": [\n" //
                    + "            [ 3, 4, 16 ]\n" //
                    + "          ],\n" //
                    + "          \"dim_cap\": 1\n" //
                    + "        }\n" //
                    + "      },\n" //
                    + "      {\n" //
                    + "        \"includes\": [ 2, 8, 3, 4, 16, 33, 6, 7, 9, 18, 19, 20, 21, 13, 14, 15, 17, 22, 23, 24, 25 ],\n" //
                    + "        \"select_rule\": {\n" //
                    + "          \"hierarchy_dims\": [],\n" //
                    + "          \"mandatory_dims\": [ 2 ],\n" + "          \"joint_dims\": [\n" //
                    + "            [ 33, 6, 7, 8 ],\n" //
                    + "            [ 3, 4, 16 ],\n" //
                    + "            [ 9, 18, 19, 20, 21 ],\n" //
                    + "            [ 13, 14, 15, 17, 22, 23, 24, 25 ]\n" //
                    + "          ],\n" //
                    + "          \"dim_cap\": 1\n" //
                    + "        }\n" //
                    + "      }\n" //
                    + "    ],\n" //
                    + "    \"storage_type\": 20" //
                    + "}").getBytes(StandardCharsets.UTF_8), RuleBasedIndex.class);
            ruleBasedIndex.setIndexPlan(copyForWrite);
            copyForWrite.setRuleBasedIndex(ruleBasedIndex, false, true);
        });

        val toBeDeletedLayouts = saved.getToBeDeletedIndexes();
        Assert.assertEquals(1, toBeDeletedLayouts.size());

        changeTypeColumn("DEFAULT.TEST_CATEGORY_GROUPINGS", new HashMap<String, String>() {
            {
                put("META_CATEG_NAME", "int");
            }
        }, true);

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_CATEGORY_GROUPINGS", false, null);

        val saved2 = indexManager.getIndexPlan(modelId);
        val toBeDeletedLayouts2 = saved2.getToBeDeletedIndexes();
        Assert.assertEquals(0, toBeDeletedLayouts2.size());
    }

    @Test
    public void testReload_AddIndexCount() throws Exception {

        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16));
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [14,15,16],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [],\n" //
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": []\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        newRule.setMeasures(Lists.newArrayList(100000, 100008));
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            newRule.setIndexPlan(copyForWrite);
            copyForWrite.setRuleBasedIndex(newRule);
        });
        dropModelWhen(id -> !id.equals(originIndexPlan.getId()));
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "LSTG_FORMAT_NAME");
        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(11, response.getRemoveLayoutsCount());
        Assert.assertEquals(7, response.getAddLayoutsCount());
    }

    @Test
    public void testReload_AddIndexCountHierarchy() throws Exception {

        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16));
        val group1 = JsonUtil.readValue("{\n"//
                + "        \"includes\": [14,15,16],\n"//
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [[14,15,16]],\n"//
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": []\n"//
                + "        }\n"//
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        newRule.setMeasures(Lists.newArrayList(100000, 100008));
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            newRule.setIndexPlan(copyForWrite);
            copyForWrite.setRuleBasedIndex(newRule);
        });
        dropModelWhen(id -> !id.equals(originIndexPlan.getId()));
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_ORDER");
        Assert.assertEquals(4, response.getRemoveLayoutsCount());
        Assert.assertEquals(1, response.getAddLayoutsCount());

    }

    @Test
    public void testReload_AddIndexCountMandatory() throws Exception {

        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16, 17, 18, 19));
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [14,15,16,17,18,19],\n"//
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [[14,15,16]],\n" //
                + "          \"mandatory_dims\": [17,18],\n" //
                + "          \"joint_dims\": []\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        newRule.setMeasures(Lists.newArrayList(100000, 100008));
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            newRule.setIndexPlan(copyForWrite);
            copyForWrite.setRuleBasedIndex(newRule);
        });
        dropModelWhen(id -> !id.equals(originIndexPlan.getId()));
        removeColumn("DEFAULT.TEST_ORDER", "BUYER_ID");
        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_ORDER");
        Assert.assertEquals(10, response.getRemoveLayoutsCount());
        Assert.assertEquals(8, response.getAddLayoutsCount());

    }

    @Test
    public void testReload_AddIndexCountJoint() throws Exception {

        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16, 17, 18, 19));
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [14,15,16,17,18,19],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [[14,15,16]],\n" //
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": [[17,18,19]]\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        newRule.setMeasures(Lists.newArrayList(100000, 100008));
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            newRule.setIndexPlan(copyForWrite);
            copyForWrite.setRuleBasedIndex(newRule);
        });
        modelService.listAllModelIdsInProject(PROJECT).forEach(id -> {
            if (!id.equals(originIndexPlan.getId())) {
                modelService.innerDropModel(id, PROJECT);
            }
        });
        removeColumn("DEFAULT.TEST_ORDER", "BUYER_ID");
        val response = tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_ORDER");
        Assert.assertEquals(6, response.getRemoveLayoutsCount());
        Assert.assertEquals(4, response.getAddLayoutsCount());

    }

    @Test
    public void testReload_BrokenModelInAutoProject() throws Exception {
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID");
        overwriteSystemProp("kylin.metadata.broken-model-deleted-on-smart-mode", "true");
        await().atMost(10000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
            SecurityContextHolder.getContext().setAuthentication(authentication);
            tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
            val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
            Assert.assertEquals(4, modelManager.listAllModels().size());
            val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
            Assert.assertEquals(4, indexManager.listAllIndexPlans().size());
            val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
            Assert.assertEquals(4, dfManager.listAllDataflows().size());
        });
    }

    @Test
    public void testReload_BrokenModelInManualProject() throws Exception {
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4, modelManager.listAllModels().stream().filter(RootPersistentEntity::isBroken).count());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4,
                indexManager.listAllIndexPlans(true).stream().filter(RootPersistentEntity::isBroken).count());
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4,
                dfManager.listAllDataflows(true).stream().filter(NDataflow::checkBrokenWithRelatedInfo).count());
    }

    @Test
    public void testReloadLookup_RemoveFact() throws Exception {
        modelService.listAllModelIdsInProject(PROJECT).forEach(id -> {
            if (!id.equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa")) {
                modelService.dropModel(id, PROJECT);
            }
        });
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(1, modelManager.listAllModels().size());

        removeColumn("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID");
        val mockTableService = Mockito.mock(TableService.class);
        val mockModerService = Mockito.spy(ModelService.class);
        ReflectionTestUtils.setField(mockTableService, "modelService", mockModerService);
        AtomicBoolean modifiedModel = new AtomicBoolean(false);

        Mockito.doAnswer(invocation -> {
            modifiedModel.set(true);
            return null;
        }).when(mockModerService).updateDataModelSemantic(Mockito.anyString(), Mockito.any());

        mockTableService.innerReloadTable(PROJECT, "DEFAULT.TEST_CATEGORY_GROUPINGS", true, null);

        Assert.assertFalse(modifiedModel.get());
    }

    private void prepareReload() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        var originModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(copyForUpdate);

        var originModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, originModels.size());
        originModel = originModels.get(0);
        Assert.assertEquals(9, originModel.getJoinTables().size());
        Assert.assertEquals(17, originModel.getAllMeasures().size());
        Assert.assertEquals(197, originModel.getAllNamedColumns().size());
    }

    @Test
    public void testNothingChanged() throws Exception {
        prepareReload();
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val TARGET_TABLE = "DEFAULT.TEST_ACCOUNT";

        val copy = tableManager.copyForWrite(tableManager.getTableDesc(TARGET_TABLE));
        copy.setLastSnapshotPath("/path/to/snapshot");
        tableManager.updateTableDesc(copy);

        tableService.innerReloadTable(PROJECT, TARGET_TABLE, true, null);
        val newTable = tableManager.getTableDesc(TARGET_TABLE);
        Assert.assertNotNull(newTable.getLastSnapshotPath());
    }

    @Test
    public void testReload_GetAndEditJoinBrokenModelInManualProject() throws Exception {
        prepareReload();

        changeColumnName("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID", "ORDER_ID2");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);

        var brokenModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, brokenModels.size());
        val brokenModel = brokenModels.get(0);
        Assert.assertEquals(9, brokenModel.getJoinTables().size());
        Assert.assertEquals(17, brokenModel.getAllMeasures().size());
        Assert.assertEquals(197, brokenModel.getAllNamedColumns().size());
        Assert.assertEquals("ORDER_ID", brokenModel.getAllNamedColumns().get(13).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, brokenModel.getAllNamedColumns().get(13).getStatus());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val brokenDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow(brokenModel.getId());
            Assert.assertEquals(0, brokenDataflow.getSegments().size());
            Assert.assertEquals(RealizationStatusEnum.BROKEN, brokenDataflow.getStatus());
        });

        Assert.assertTrue(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(brokenModel.getId()).isBroken());

        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        val updateJoinTables = copyModel.getJoinTables();
        updateJoinTables.get(0).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.ORDER_ID2" });
        copyModel.setJoinTables(updateJoinTables);
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val reModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertNotNull(reModel);
        Assert.assertFalse(reModel.isBroken());
        Assert.assertEquals(9, reModel.getJoinTables().size());
        Assert.assertEquals(17, reModel.getAllMeasures().size());
        Assert.assertEquals(198, reModel.getAllNamedColumns().size());
        Assert.assertEquals("ORDER_ID", reModel.getAllNamedColumns().get(13).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(13).getStatus());
        val reDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(reModel.getId());
        Assert.assertEquals(0, reDataflow.getSegments().size());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, reDataflow.getStatus());
        Assert.assertFalse(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(reModel.getId()).isBroken());
    }

    private ModelRequest createModelRequest(NDataModel copyModel) {
        val updateRequest = new ModelRequest(copyModel);
        updateRequest.setProject(PROJECT);
        updateRequest.setStart("1262275200000");
        updateRequest.setEnd("1388505600000");
        updateRequest.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        return updateRequest;
    }

    @Test
    public void testReload_GetAndEditPartitionBrokenModelInManualProject() throws Exception {
        prepareReload();

        changeColumnName("DEFAULT.TEST_KYLIN_FACT", "CAL_DT", "CAL_DT2");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);

        var brokenModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, brokenModels.size());
        val brokenModel = brokenModels.get(0);
        Assert.assertEquals(9, brokenModel.getJoinTables().size());
        Assert.assertEquals(17, brokenModel.getAllMeasures().size());
        Assert.assertEquals(197, brokenModel.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", brokenModel.getAllNamedColumns().get(2).getName());
        Assert.assertEquals("DEAL_YEAR", brokenModel.getAllNamedColumns().get(28).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, brokenModel.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, brokenModel.getAllNamedColumns().get(28).getStatus());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val brokenDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow(brokenModel.getId());
            Assert.assertEquals(0, brokenDataflow.getSegments().size());
            Assert.assertEquals(RealizationStatusEnum.BROKEN, brokenDataflow.getStatus());
        });
        Assert.assertTrue(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(brokenModel.getId()).isBroken());

        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        copyModel.getPartitionDesc().setPartitionDateColumn("DEFAULT.TEST_KYLIN_FACT.CAL_DT2");
        val updateJoinTables = copyModel.getJoinTables();
        updateJoinTables.get(2).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.CAL_DT2" });
        copyModel.setJoinTables(updateJoinTables);

        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val reModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertNotNull(reModel);
        Assert.assertFalse(reModel.isBroken());
        Assert.assertEquals(9, reModel.getJoinTables().size());
        Assert.assertEquals(17, reModel.getAllMeasures().size());
        Assert.assertEquals(198, reModel.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", reModel.getAllNamedColumns().get(2).getName());
        Assert.assertEquals("DEAL_YEAR", reModel.getAllNamedColumns().get(28).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(28).getStatus());
        val reDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(reModel.getId());
        Assert.assertEquals(0, reDataflow.getSegments().size());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, reDataflow.getStatus());
        Assert.assertFalse(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(reModel.getId()).isBroken());
    }

    @Test
    public void testRepairBrokenModelWithNullPartitionDesc() throws Exception {
        prepareReload();

        changeColumnName("DEFAULT.TEST_KYLIN_FACT", "CAL_DT", "CAL_DT2");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);

        var brokenModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, brokenModels.size());
        val brokenModel = brokenModels.get(0);
        Assert.assertNotNull(brokenModel.getPartitionDesc());

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val df2 = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                    .getDataflowByModelAlias("nmodel_basic_inner");
            Assert.assertEquals(RealizationStatusEnum.BROKEN, df2.getStatus());
            Assert.assertEquals(0, df2.getSegments().size());
        });

        modelService.checkFlatTableSql(brokenModel);

        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        copyModel.setPartitionDesc(null);
        val updateJoinTables = copyModel.getJoinTables();
        updateJoinTables.get(2).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.CAL_DT2" });
        copyModel.setJoinTables(updateJoinTables);

        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val reModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertNotNull(reModel);
        Assert.assertFalse(reModel.isBroken());
        Assert.assertEquals(9, reModel.getJoinTables().size());
        Assert.assertEquals(17, reModel.getAllMeasures().size());
        Assert.assertEquals(198, reModel.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", reModel.getAllNamedColumns().get(2).getName());
        Assert.assertEquals("DEAL_YEAR", reModel.getAllNamedColumns().get(28).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(28).getStatus());
        val reDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(reModel.getId());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, reDataflow.getStatus());
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow(reModel.getId());
            Assert.assertEquals(1, dataflow.getSegments().size());
        });
        Assert.assertNull(reModel.getPartitionDesc());
        Assert.assertFalse(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(reModel.getId()).isBroken());

    }

    @Test
    public void testReloadAutoRemoveEmptyAggGroup() throws Exception {
        prepareReload();
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        var originModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), PROJECT).updateIndexPlan(originModel.getUuid(),
                    copyForWrite -> {
                        SelectRule selectRule = new SelectRule();
                        selectRule.setMandatoryDims(new Integer[] {});
                        selectRule.setJointDims(new Integer[][] {});
                        selectRule.setHierarchyDims(new Integer[][] {});
                        copyForWrite.getRuleBasedIndex().getAggregationGroups().get(0).setSelectRule(selectRule);
                        copyForWrite.getRuleBasedIndex().getAggregationGroups().get(0).setIncludes(new Integer[] { 2 });
                    });
            return null;
        }, PROJECT);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT)
                .getIndexPlan(originModel.getUuid());
        Assert.assertEquals(2, indexPlan.getRuleBasedIndex().getAggregationGroups().size());
        Assert.assertEquals(21, indexPlan.getRuleBasedIndex().getAggregationGroups().get(1).getIncludes().length);
        Integer removeId = 2;
        val identity = originModel.getEffectiveDimensions().get(removeId).toString();
        String db = identity.split("\\.")[0];
        String tb = identity.split("\\.")[1];
        String col = identity.split("\\.")[2];
        removeColumn(db + "." + tb, col);

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        val brokenModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        copyModel.getJoinTables().get(2).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.LSTG_SITE_ID" });
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);

        indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(originModel.getUuid());
        Assert.assertEquals(1, indexPlan.getRuleBasedIndex().getAggregationGroups().size());
        Assert.assertEquals(20, indexPlan.getRuleBasedIndex().getAggregationGroups().get(0).getIncludes().length);
    }

    @Test
    public void testReload_WhenProjectHasBrokenModel() throws Exception {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.removeSourceTable("DEFAULT.TEST_MEASURE");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(7, dfManager.listUnderliningDataModels().size());

        testPreProcess_AffectTwoTables();
    }

    @Test
    public void testReload_RemoveDimensionsAndIndexes() throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getTableDesc("DEFAULT.TEST_ORDER");
        prepareTableExt("DEFAULT.TEST_ORDER");
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_ORDER", true, null);

        // index_plan with rule
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB,
                model.getAllNamedColumns().stream().filter(n -> n.getId() == 15).findAny().get().getStatus());
        val indexPlan = indexManager.getIndexPlan(model.getId());
        indexPlan.getAllIndexes().forEach(index -> {
            String message = "index " + index.getId() + " have 15, dimensions are " + index.getDimensions();
            Assert.assertFalse(message, index.getDimensions().contains(15));
        });
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val dataflow = dataflowManager.getDataflow(model.getId());
        for (NDataSegment segment : dataflow.getSegments()) {
            for (NDataLayout layout : segment.getLayoutsMap().values()) {
                String message = "data_layout " + layout.getLayout().getId() + " have 15, col_order is "
                        + layout.getLayout().getColOrder();
                Assert.assertFalse(message, layout.getLayout().getColOrder().contains(15));
            }
        }

        // index_plan without rule
        val model2 = modelManager.getDataModelDescByAlias("nmodel_basic");
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB,
                model2.getAllNamedColumns().stream().filter(n -> n.getId() == 15).findAny().get().getStatus());
        val indexPlan2 = indexManager.getIndexPlan(model2.getId());
        Assert.assertEquals(
                originIndexPlan.getAllIndexes().stream().filter(index -> !index.getDimensions().contains(15)).count(),
                indexPlan2.getAllIndexes().size());
        indexPlan2.getAllIndexes().forEach(index -> {
            String message = "index " + index.getId() + " have 15, dimensions are " + index.getDimensions();
            Assert.assertFalse(message, index.getDimensions().contains(15));
        });

        var executables = getRunningExecutables(PROJECT, model2.getId());
        Assert.assertEquals(1, executables.size());
        deleteJobByForce(executables.get(0));
        executables = getRunningExecutables(PROJECT, model.getId());
        Assert.assertEquals(1, executables.size());
        deleteJobByForce(executables.get(0));

        // check table sample
        val tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_ORDER");
        Assert.assertEquals(originTable.getColumns().length - 1, tableExt.getAllColumnStats().size());
        for (TableExtDesc.ColumnStats stat : tableExt.getAllColumnStats()) {
            Assert.assertNotEquals("TEST_TIME_ENC", stat.getColumnName());
        }
        for (String[] sampleRow : tableExt.getSampleRows()) {
            Assert.assertFalse(Joiner.on(",").join(sampleRow).contains("col_3"));
        }

        Assert.assertEquals("PRICE", model.getAllNamedColumns().get(11).getName());
        Assert.assertTrue(model.getAllNamedColumns().get(11).isExist());
        Assert.assertTrue(isTableIndexContainColumn(indexManager, model.getAlias(), 11));
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "PRICE");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        Assert.assertFalse(isTableIndexContainColumn(indexManager, model.getAlias(), 11));
    }

    @Test
    public void testReload_RemoveAggShardByColumns() throws Exception {
        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16));
        val group1 = JsonUtil.readValue("{\n" + "        \"includes\": [14,15,16],\n" + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": []\n" + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        testReload_AggShardByColumns(newRule, Lists.newArrayList(14, 15), Lists.newArrayList());

    }

    @Test
    public void testReload_KeepAggShardByColumns() throws Exception {
        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(13, 14, 15));
        val group1 = JsonUtil.readValue("{\n" + "        \"includes\": [13,14,15],\n" + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": []\n" + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        testReload_AggShardByColumns(newRule, Lists.newArrayList(13, 14), Lists.newArrayList(13, 14));

    }

    private void testReload_AggShardByColumns(RuleBasedIndex ruleBasedIndex, List<Integer> beforeAggShardBy,
            List<Integer> endAggShardBy) throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val updatedIndexPlan = indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            ruleBasedIndex.setIndexPlan(copyForWrite);
            copyForWrite.setRuleBasedIndex(ruleBasedIndex);
            copyForWrite.setAggShardByColumns(beforeAggShardBy);
        });
        Assert.assertEquals(beforeAggShardBy, updatedIndexPlan.getAggShardByColumns());
        prepareTableExt("DEFAULT.TEST_ORDER");
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_ORDER", true, null);

        // index_plan with rule
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic");
        val indexPlan = indexManager.getIndexPlan(model.getId());
        Assert.assertEquals(endAggShardBy, indexPlan.getAggShardByColumns());
    }

    private boolean isTableIndexContainColumn(NIndexPlanManager indexPlanManager, String modelAlias, Integer col) {
        for (IndexEntity indexEntity : indexPlanManager.getIndexPlanByModelAlias(modelAlias).getIndexes()) {
            if (indexEntity.getDimensions().contains(col)) {
                return true;
            }
        }

        return false;
    }

    @Test
    public void testReload_addColumn_blacklistNotEmpty() throws Exception {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val dataflow1 = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        int layoutSize = dataflow1.getIndexPlan().getRuleBaseLayouts().size();

        NIndexPlanManager indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.updateIndexPlan(dataflow1.getIndexPlan().getId(), copyForWrite -> {
            copyForWrite.addRuleBasedBlackList(Lists.newArrayList(1070001L));
        });
        addColumn("DEFAULT.TEST_KYLIN_FACT", true, new ColumnDesc("", "newColumn", "int", "", "", "", null));

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);

        Assert.assertEquals(layoutSize - 1, dataflowManager.getDataflowByModelAlias("nmodel_basic_inner").getIndexPlan()
                .getRuleBaseLayouts().size());
    }

    @Test
    public void testReload_AddColumn() throws Exception {
        String mockPath = "default/table_snapshot/mock";
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_COUNTRY");
        tableDesc.setLastSnapshotPath(mockPath);
        tableMetadataManager.updateTableDesc(tableDesc);

        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT", true, null);

        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val originMaxId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val dataflow1 = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        Assert.assertNotNull(NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getTableDesc("DEFAULT.TEST_COUNTRY").getLastSnapshotPath());

        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getTableDesc("DEFAULT.TEST_COUNTRY");
        prepareTableExt("DEFAULT.TEST_COUNTRY");
        addColumn("DEFAULT.TEST_COUNTRY", true, new ColumnDesc("", "tmp1", "bigint", "", "", "", null));
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_COUNTRY", true, null);

        val model2 = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val maxId = model2.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();
        // Assert.assertEquals(originMaxId + 2, maxId);

        val dataflow2 = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        Assert.assertNull(NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getTableDesc("DEFAULT.TEST_COUNTRY").getLastSnapshotPath());
        // check table sample
        val tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(originTable.getColumns().length, tableExt.getAllColumnStats().size());
        Assert.assertNull(tableExt.getColumnStatsByName("TMP1"));
        for (String[] sampleRow : tableExt.getSampleRows()) {
            Assert.assertEquals(originTable.getColumns().length + 1, sampleRow.length);
            Assert.assertTrue(Joiner.on(",").join(sampleRow).endsWith(","));
        }

        addColumn("DEFAULT.TEST_KYLIN_FACT", true, new ColumnDesc("", "DEAL_YEAR", "int", "", "", "", null));

        OpenPreReloadTableResponse response = tableService.preProcessBeforeReloadWithoutFailFast(PROJECT,
                "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertTrue(response.isHasDatasourceChanged());
        Assert.assertTrue(response.isHasDuplicatedColumns());
        Assert.assertEquals(1, response.getDuplicatedColumns().size());

        try {
            tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
            Assert.fail();
        } catch (TransactionException e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getMessage().contains("KE-010007007(Duplicated Column Name)"));
        }

        removeColumn("DEFAULT.TEST_KYLIN_FACT", "DEAL_YEAR");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
    }

    @Test
    public void testReload_AddLookupColumn() throws Exception {
        addColumn("EDW.TEST_CAL_DT", true, new ColumnDesc("", "DEAL_YEAR", "int", "", "", "", null));

        OpenPreReloadTableResponse response = tableService.preProcessBeforeReloadWithoutFailFast(PROJECT,
                "EDW.TEST_CAL_DT");
        Assert.assertTrue(response.isHasDatasourceChanged());
        Assert.assertTrue(response.isHasDuplicatedColumns());
        Assert.assertEquals(1, response.getDuplicatedColumns().size());
        String colName = response.getDuplicatedColumns().get(0);
        Assert.assertEquals("EDW.TEST_CAL_DT.DEAL_YEAR", colName);
    }

    @Test
    public void testReloadTableWithoutModel() throws Exception {
        addColumn("EDW.TEST_CAL_DT", true, new ColumnDesc("", "DEAL_YEAR", "int", "", "", "", null));

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        modelManager.listAllModelIds().forEach(modelManager::dropModel);
        Assert.assertTrue(modelManager.listAllModels().isEmpty());

        OpenPreReloadTableResponse response = tableService.preProcessBeforeReloadWithoutFailFast(PROJECT,
                "EDW.TEST_CAL_DT");
        Assert.assertTrue(response.isHasDatasourceChanged());
        Assert.assertFalse(response.isHasDuplicatedColumns());
    }

    @Test
    public void testCheckEffectedJobs() throws Exception {
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        AbstractExecutable job1 = new NTableSamplingJob();
        job1.setTargetSubject("DEFAULT.TEST_ORDER");
        job1.setJobType(JobTypeEnum.TABLE_SAMPLING);
        executableManager.addJob(job1);
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");

        OpenPreReloadTableResponse response = tableService.preProcessBeforeReloadWithoutFailFast(PROJECT,
                "DEFAULT.TEST_ORDER");
        Assert.assertTrue(response.isHasEffectedJobs());
        Assert.assertEquals(1, response.getEffectedJobs().size());

        try {
            tableService.preProcessBeforeReloadWithFailFast(PROJECT, "DEFAULT.TEST_ORDER");
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertTrue(e.toString().contains(TABLE_RELOAD_HAVING_NOT_FINAL_JOB.getErrorCode().getCode()));
        }
    }

    @Test
    public void testReload_ChangeColumn() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT", true, null);

        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val df = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan = df.getIndexPlan();
        val model = df.getModel();
        val originMaxId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();

        val layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        val nextAggIndexId = indexPlan.getNextAggregationIndexId();
        val nextTableIndexId = indexPlan.getNextTableIndexId();
        val tableIdentity = "DEFAULT.TEST_COUNTRY";
        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableIdentity);
        prepareTableExt(tableIdentity);
        changeTypeColumn(tableIdentity, new HashMap<String, String>() {
            {
                put("LATITUDE", "bigint");
                put("NAME", "int");
            }
        }, true);

        tableService.innerReloadTable(PROJECT, tableIdentity, true, null);

        val df2 = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan2 = df2.getIndexPlan();
        val model2 = df2.getModel();
        val maxId = model2.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();
        // do not change model
        Assert.assertEquals(originMaxId, maxId);
        // remove layouts in df
        Assert.assertNull(df2.getLastSegment().getLayout(1000001));

        val layoutIds2 = indexPlan2.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        val diff = Sets.difference(layoutIds, layoutIds2);
        Assert.assertEquals(4, diff.size());
        Assert.assertEquals(
                nextAggIndexId + IndexEntity.INDEX_ID_STEP
                        * diff.stream().filter(l -> l < IndexEntity.TABLE_INDEX_START_ID).count(),
                indexPlan2.getNextAggregationIndexId());
        Assert.assertEquals(
                nextTableIndexId + IndexEntity.INDEX_ID_STEP * diff.stream().filter(IndexEntity::isTableIndex).count(),
                indexPlan2.getNextTableIndexId());

        val executables = getRunningExecutables(PROJECT, model.getId());
        Assert.assertEquals(1, executables.size());

        // check table sample
        val tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getOrCreateTableExt(tableIdentity);
        Assert.assertEquals(originTable.getColumns().length, tableExt.getAllColumnStats().size());
        int i = 1;
        for (String[] sampleRow : tableExt.getSampleRows()) {
            Assert.assertEquals(originTable.getColumns().length, sampleRow.length);
            int finalI = i;
            Assert.assertEquals(
                    Stream.of(0, 1, 2, 3).map(j -> "row_" + finalI + "_col_" + j).collect(Collectors.joining(",")),
                    Joiner.on(",").join(sampleRow));
            i++;
        }
    }

    @Test
    public void testReload_ChangeTypeAndRemoveDimension() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT", true, null);

        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val originDF = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val originIndexPlan = originDF.getIndexPlan();
        val originModel = originDF.getModel();

        // in this case will fire 3 AddCuboid Events
        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "LSTG_FORMAT_NAME");
        changeTypeColumn(tableIdentity, new HashMap<String, String>() {
            {
                put("PRICE", "string");
            }
        }, false);
        tableService.innerReloadTable(PROJECT, tableIdentity, true, null);

        val df = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan = df.getIndexPlan();
        val model = indexPlan.getModel();

        val layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        for (Long id : Arrays.asList(1000001L, 20001L, 20000020001L)) {
            Assert.assertFalse(layoutIds.contains(id));
        }
        for (LayoutEntity layout : originIndexPlan.getRuleBaseLayouts()) {
            Assert.assertFalse(layoutIds.contains(layout.getId()));
        }
        Assert.assertFalse(model.getEffectiveCols().containsKey(3));
        Assert.assertFalse(model.getEffectiveMeasures().containsKey(100008));
    }

    @Test
    public void testReload_ChangeColumnInAggManual() throws Exception {
        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16));
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [14,15,16],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [],\n" //
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": []\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        group1.setMeasures(new Integer[] { 100000, 100008 });
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val modelId = originIndexPlan.getId();
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(newRule);
        });
        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts1 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());
        Assert.assertEquals(4, layouts1.size());
        dropModelWhen(id -> !id.equals(modelId));
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("SLR_SEGMENT_CD", "bigint");
            }
        }, true);
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts2 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());
        Assert.assertEquals(layouts1.size(), layouts2.size());
        Assert.assertTrue(layouts1.stream()
                .allMatch(l -> layouts2.stream().anyMatch(l2 -> l2.equals(l) && l2.getId() > l.getId())));
    }

    @Test
    public void testReload_WithNoBlacklistLayoutRestore() throws Exception {
        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16));
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [14,15,16],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [],\n" //
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": []\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        group1.setMeasures(new Integer[] { 100000, 100008 });
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val modelId = originIndexPlan.getId();
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(newRule);
        });
        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts1 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());
        Assert.assertEquals(4, layouts1.size());

        indexPlanService.removeIndexes(getProject(), modelId,
                layouts1.stream().map(LayoutEntity::getId).collect(Collectors.toSet()));
        dropModelWhen(id -> !id.equals(modelId));

        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("SLR_SEGMENT_CD", "bigint");
            }
        }, true);
        addColumn("DEFAULT.TEST_KYLIN_FACT", false, new ColumnDesc("5", "newCol", "double", "", "", "", null));
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "IS_EFFECTUAL");

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts2 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());
        Assert.assertThat(layouts2.size(), is(0));
    }

    @Test
    public void testReload_ChangeColumnInAggManualUnsuitable() throws Exception {
        // add TEST_KYLIN_FACT.ITEM_COUNT as dimension

        var indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var dataModelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        NDataModel model = dataModelManager.getDataModelDescByAlias("nmodel_basic");

        var request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid(model.getUuid());
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        NDataModel.NamedColumn newDimension = new NDataModel.NamedColumn();
        newDimension.setName("ITEM_COUNT");
        newDimension.setAliasDotColumn("TEST_KYLIN_FACT.ITEM_COUNT");
        newDimension.setStatus(NDataModel.ColumnStatus.DIMENSION);
        request.getSimplifiedDimensions().add(newDimension);
        modelService.updateDataModelSemantic("default", request);

        // add agg group contains TEST_KYLIN_FACT.ITEM_COUNT and sum(TEST_KYLIN_FACT.ITEM_COUNT)
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");

        val newRule = new RuleBasedIndex();
        // TEST_KYLIN_FACT.ITEM_COUNT， TEST_ORDER.TEST_TIME_ENC， TEST_KYLIN_FACT.SLR_SEGMENT_CD
        newRule.setDimensions(Arrays.asList(12, 15, 16));
        val group1 = JsonUtil.readValue("{\n" + "        \"includes\": [12,15,16],\n" + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": []\n" + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        // 100000 count(1), 100004 sum(TEST_KYLIN_FACT.ITEM_COUNT)
        group1.setMeasures(new Integer[] { 100000, 100004 });
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(newRule);
        });

        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts1 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());
        Assert.assertEquals(4, layouts1.size());
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("ITEM_COUNT", "string");
            }
        }, true);

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);

        indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);

        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts2 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());

        // 100004 measure has removed and TEST_KYLIN_FACT.ITEM_COUNT column exists
        Assert.assertTrue(layouts2.stream().anyMatch(layoutEntity -> layoutEntity.getColOrder().contains(12)
                && !layoutEntity.getMeasureIds().contains(100004)));
    }

    @Test
    public void testReload_ChangeColumnInAggManualSuitable() throws Exception {
        // add TEST_KYLIN_FACT.ITEM_COUNT as dimension

        var indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var dataModelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        NDataModel model = dataModelManager.getDataModelDescByAlias("nmodel_basic");

        var request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid(model.getUuid());
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        NDataModel.NamedColumn newDimension = new NDataModel.NamedColumn();
        newDimension.setName("ITEM_COUNT");
        newDimension.setAliasDotColumn("TEST_KYLIN_FACT.ITEM_COUNT");
        newDimension.setStatus(NDataModel.ColumnStatus.DIMENSION);
        request.getSimplifiedDimensions().add(newDimension);
        modelService.updateDataModelSemantic("default", request);

        // add agg group contains TEST_KYLIN_FACT.ITEM_COUNT and sum(TEST_KYLIN_FACT.ITEM_COUNT)
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");

        val newRule = new RuleBasedIndex();
        // TEST_KYLIN_FACT.ITEM_COUNT， TEST_ORDER.TEST_TIME_ENC， TEST_KYLIN_FACT.SLR_SEGMENT_CD
        newRule.setDimensions(Arrays.asList(12, 15, 16));
        val group1 = JsonUtil.readValue("{\n" //
                + "        \"includes\": [12,15,16],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [],\n" //
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": []\n" //
                + "        }\n" //
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        // 100000 count(1), 100004 sum(TEST_KYLIN_FACT.ITEM_COUNT)
        group1.setMeasures(new Integer[] { 100000, 100004 });
        indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(newRule);
        });

        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts1 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());
        Assert.assertEquals(4, layouts1.size());
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("ITEM_COUNT", "decimal(30,4)");
            }
        }, true);

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);

        indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);

        originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val layouts2 = originIndexPlan.getAllLayouts().stream().filter(LayoutEntity::isManual)
                .filter(l -> l.getId() < IndexEntity.TABLE_INDEX_START_ID).filter(l -> l.getColOrder().contains(16))
                .collect(Collectors.toList());

        dataModelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        Optional<NDataModel.Measure> sumMeasure = model.getAllMeasures().stream().filter(measure -> {
            FunctionDesc function = measure.getFunction();
            return !measure.isTomb() && function.getExpression().equals("SUM")
                    && function.getParameters().get(0).getValue().equalsIgnoreCase("TEST_KYLIN_FACT.ITEM_COUNT")
                    && function.getReturnType().contains("decimal");
        }).findAny();

        Assert.assertTrue(sumMeasure.isPresent());

        // sum measure and TEST_KYLIN_FACT.ITEM_COUNT column still exists
        Assert.assertTrue(layouts2.stream().anyMatch(layoutEntity -> layoutEntity.getColOrder().contains(12)
                && layoutEntity.getMeasureIds().contains(sumMeasure.get().getId())));
    }

    @Test
    public void testReload_ChangeColumnInAggManualAndAuto() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val index = new IndexEntity();
        index.setDimensions(Arrays.asList(14, 15, 16));
        index.setMeasures(Arrays.asList(100000, 100008));
        val layout = new LayoutEntity();
        layout.setColOrder(Arrays.asList(14, 15, 16, 100000, 100008));
        layout.setAuto(true);
        indexManager.updateIndexPlan(modelId, copyForWrite -> {
            val indexCopy = JsonUtil.deepCopyQuietly(index, IndexEntity.class);
            indexCopy.setId(copyForWrite.getNextAggregationIndexId());
            val layoutCopy = JsonUtil.deepCopyQuietly(layout, LayoutEntity.class);
            layoutCopy.setId(indexCopy.getId() + 1);
            indexCopy.getLayouts().add(layoutCopy);
            indexCopy.setNextLayoutOffset(2);
            val indexes = copyForWrite.getIndexes();
            indexes.add(indexCopy);
            copyForWrite.setIndexes(indexes);
        });
        testReload_ChangeColumnInAggManual();
        Assert.assertTrue(indexManager.getIndexPlan(modelId).getAllLayouts().stream()
                .anyMatch(l -> l.equals(layout) && l.isAuto() && l.isManual() && l.getId() > layout.getId()));
    }

    @Test
    public void testReload_ChangeColumnInTableIndex() throws Exception {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        dropModelWhen(id -> !id.equals(modelId));
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val index = new IndexEntity();
        index.setDimensions(Arrays.asList(14, 15, 16));
        val layout = new LayoutEntity();
        layout.setColOrder(Arrays.asList(14, 15, 16));
        layout.setAuto(true);
        layout.setManual(true);
        indexManager.updateIndexPlan(modelId, copyForWrite -> {
            val indexCopy = JsonUtil.deepCopyQuietly(index, IndexEntity.class);
            indexCopy.setId(copyForWrite.getNextTableIndexId());
            val layoutCopy = JsonUtil.deepCopyQuietly(layout, LayoutEntity.class);
            layoutCopy.setId(indexCopy.getId() + 1);
            indexCopy.getLayouts().add(layoutCopy);
            indexCopy.setNextLayoutOffset(2);
            val indexes = copyForWrite.getIndexes();
            indexes.add(indexCopy);
            copyForWrite.setIndexes(indexes);
        });
        changeTypeColumn("DEFAULT.TEST_KYLIN_FACT", new HashMap<String, String>() {
            {
                put("SLR_SEGMENT_CD", "bigint");
            }
        }, true);
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", true, null);
        Assert.assertTrue(indexManager.getIndexPlan(modelId).getAllLayouts().stream()
                .anyMatch(l -> l.equals(layout) && l.isAuto() && l.getId() > layout.getId()));
    }

    @Test
    public void testReload_ChangeColumnOrderAndDeleteColumn() throws Exception {
        val tableIdentity = "DEFAULT.TEST_COUNTRY";
        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableIdentity);
        prepareTableExt(tableIdentity);
        removeColumn(tableIdentity, "LATITUDE");
        addColumn(tableIdentity, false, new ColumnDesc("5", "LATITUDE", "double", "", "", "", null));
        addColumn(tableIdentity, false, new ColumnDesc("6", "LATITUDE6", "double", "", "", "", null));

        tableService.innerReloadTable(PROJECT, tableIdentity, true, null);

        // check table sample
        var tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(originTable.getColumns().length, tableExt.getAllColumnStats().size());
        for (int i = 0; i < tableExt.getSampleRows().size(); i++) {
            val sampleRow = tableExt.getSampleRows().get(i);
            int finalI = i;
            Assert.assertEquals(
                    Stream.of(0, 2, 3, 1).map(j -> "row_" + (finalI + 1) + "_col_" + j).collect(Collectors.joining(","))
                            + ",",
                    Joiner.on(",").join(sampleRow));
        }

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NIndexPlanManager.getInstance(getTestConfig(), PROJECT).updateIndexPlan(dataflow.getUuid(), copyForWrite -> {
            val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                    .filter(layoutEntity -> 1000001L == layoutEntity.getId()).collect(Collectors.toSet());
            copyForWrite.markIndexesToBeDeleted(dataflow.getUuid(), toBeDeletedSet);
        });
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(dataflow.getUuid());
        Assert.assertTrue(CollectionUtils.isNotEmpty(indexPlan.getToBeDeletedIndexes()));

        removeColumn(tableIdentity, "NAME");
        tableService.innerReloadTable(PROJECT, tableIdentity, true, null);

        indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(dataflow.getUuid());
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getToBeDeletedIndexes()));
        // check table sample
        tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(originTable.getColumns().length - 1, tableExt.getAllColumnStats().size());
        Assert.assertNull(tableExt.getColumnStatsByName("NAME"));
        for (int i = 0; i < tableExt.getSampleRows().size(); i++) {
            val sampleRow = tableExt.getSampleRows().get(i);
            int finalI = i;
            Assert.assertEquals(
                    Stream.of(0, 2, 1).map(j -> "row_" + (finalI + 1) + "_col_" + j).collect(Collectors.joining(","))
                            + ",",
                    Joiner.on(",").join(sampleRow));
        }
    }

    @Test
    public void testReload_IndexPlanHasDictionary() throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val indexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic_inner");
        indexManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.setDictionaries(Arrays.asList(
                    new NDictionaryDesc(12, 1, "org.apache.kylin.dict.NGlobalDictionaryBuilder2", null, null),
                    new NDictionaryDesc(3, 1, "org.apache.kylin.dict.NGlobalDictionaryBuilder2", null, null)));
        });

        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "ITEM_COUNT", "LSTG_FORMAT_NAME");

        tableService.innerReloadTable(PROJECT, tableIdentity, true, null);

        val indexPlan2 = indexManager.getIndexPlan(indexPlan.getId());
        Assert.assertEquals(0, indexPlan2.getDictionaries().size());
    }

    @Test
    public void testReloadTableWithSecondStorage() throws Exception {
        val model = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val project = "default";
        MockSecondStorage.mock("default", new ArrayList<>(), this);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            indexPlanManager.updateIndexPlan(model, indexPlan -> {
                indexPlan.createAndAddBaseIndex(indexPlan.getModel());
            });
            return null;
        }, project);
        SecondStorageUtil.initModelMetaData("default", model);
        Assert.assertTrue(indexPlanManager.getIndexPlan(model).containBaseTableLayout());
        ModelRequest request = new ModelRequest();
        request.setWithSecondStorage(true);
        request.setUuid(model);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "IS_EFFECTUAL");
        tableService.innerReloadTable(PROJECT, tableIdentity, true, null);

        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));
    }

    @Test
    public void testReload_NoChangeAndUpdateTableExtDesc() throws Exception {
        S3TableExtInfo tableExtInfo = prepareTableExtInfo("DEFAULT.TEST_ORDER", "endpoint", "role");
        prepareTableExt("DEFAULT.TEST_ORDER");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.use-dynamic-S3-role-credential-in-table", "true");
        tableService.innerReloadTable(PROJECT, tableExtInfo.getName(), true, tableExtInfo);
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val table = tableManager.getTableDesc(tableExtInfo.getName());
        TableExtDesc tableExtDesc = tableManager.getTableExtIfExists(table);
        String endpoint = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ENDPOINT_KEY);
        String roleArn = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ROLE_PROPERTY_KEY);
        Assert.assertEquals("endpoint", endpoint);
        Assert.assertEquals("role", roleArn);
        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.use-dynamic-S3-role-credential-in-table", "false");
        tableService.innerReloadTable(PROJECT, tableExtInfo.getName(), true, null);
        tableExtDesc = tableManager.getTableExtIfExists(table);
        endpoint = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ENDPOINT_KEY);
        roleArn = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ROLE_PROPERTY_KEY);
        Assert.assertNull(endpoint);
        Assert.assertNull(roleArn);
    }

    @Test
    public void testReload_RemoveColumnAndUpdateTableExtDesc() throws Exception {
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        S3TableExtInfo tableExtInfo = prepareTableExtInfo("DEFAULT.TEST_ORDER", "endpoint", "role");

        prepareTableExt("DEFAULT.TEST_ORDER");
        tableService.innerReloadTable(PROJECT, tableExtInfo.getName(), true, tableExtInfo);
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val table = tableManager.getTableDesc(tableExtInfo.getName());
        val tableExtDesc = tableManager.getTableExtIfExists(table);
        String endpoint = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ENDPOINT_KEY);
        String roleArn = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ROLE_PROPERTY_KEY);
        Assert.assertEquals("endpoint", endpoint);
        Assert.assertEquals("role", roleArn);
    }

    @Test
    public void testReloadAWSTableCompatibleCrossAccountNoSample(){
        S3TableExtInfo tableExtInfo = prepareTableExtInfo("DEFAULT.TEST_ORDER", "endpoint", "role");
        prepareTableExt("DEFAULT.TEST_ORDER");
        tableService.reloadAWSTableCompatibleCrossAccount(PROJECT, tableExtInfo, false, 10000, true, 3, null);
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        TableDesc table = tableManager.getTableDesc(tableExtInfo.getName());
        TableExtDesc tableExtDesc = tableManager.getTableExtIfExists(table);
        String endpoint = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ENDPOINT_KEY);
        String roleArn = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ROLE_PROPERTY_KEY);
        Assert.assertEquals("endpoint", endpoint);
        Assert.assertEquals("role", roleArn);

        tableService.reloadAWSTableCompatibleCrossAccount(PROJECT, tableExtInfo, true, 0, true, 3, null);
        table = tableManager.getTableDesc(tableExtInfo.getName());
        tableExtDesc = tableManager.getTableExtIfExists(table);
        endpoint = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ENDPOINT_KEY);
        roleArn = tableExtDesc.getDataSourceProps().get(TableExtDesc.S3_ROLE_PROPERTY_KEY);
        Assert.assertEquals("endpoint", endpoint);
        Assert.assertEquals("role", roleArn);
    }

    @Test(expected = Exception.class)
    public void testReloadAWSTableCompatibleCrossAccountNeedSample(){
        S3TableExtInfo tableExtInfo = prepareTableExtInfo("DEFAULT.TEST_ORDER", "endpoint", "role");
        prepareTableExt("DEFAULT.TEST_ORDER");
        tableService.reloadAWSTableCompatibleCrossAccount(PROJECT, tableExtInfo, true, 10000, true, 3, null);
    }

    private S3TableExtInfo prepareTableExtInfo(String dbTable, String endpoint, String role){
        S3TableExtInfo tableExtInfo = new S3TableExtInfo();
        tableExtInfo.setName(dbTable);
        tableExtInfo.setEndpoint(endpoint);
        tableExtInfo.setRoleArn(role);
        return tableExtInfo;
    }


    private void prepareTableExt(String tableIdentity) {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val table = tableManager.getTableDesc(tableIdentity);
        val ext = tableManager.getOrCreateTableExt(tableIdentity);
        ext.setColumnStats(Stream.of(table.getColumns()).map(desc -> {
            val res = new TableExtDesc.ColumnStats();
            res.setColumnName(desc.getName());
            res.setCardinality(1000);
            res.setMaxLength(100);
            return res;
        }).collect(Collectors.toList()));
        ext.setSampleRows(Stream.of(1, 2, 3, 4).map(i -> {
            val row = new String[table.getColumns().length];
            for (int j = 0; j < row.length; j++) {
                row[j] = "row_" + i + "_col_" + j;
            }
            return row;
        }).collect(Collectors.toList()));
        ext.addDataSourceProp("location", "test-location");
        tableManager.saveTableExt(ext);
    }

    private void changeTypeColumn(String tableIdentity, Map<String, String> columns, boolean useMeta)
            throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Stream.of(useMeta ? tableManager.copyForWrite(factTable).getColumns() : tableMeta.getColumns())
                .peek(col -> {
                    if (columns.containsKey(col.getName())) {
                        col.setDatatype(columns.get(col.getName()));
                    }
                }).toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void addColumn(String tableIdentity, boolean useMeta, ColumnDesc... columns) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Lists.newArrayList(useMeta ? factTable.getColumns() : tableMeta.getColumns());
        long maxId = Stream.of(useMeta ? tableManager.copyForWrite(factTable).getColumns() : tableMeta.getColumns())
                .mapToLong(col -> Long.parseLong(col.getId())).max().getAsLong();
        for (ColumnDesc column : columns) {
            maxId++;
            column.setId("" + maxId);
            newColumns.add(column);
        }
        tableMeta.setColumns(newColumns.toArray(new ColumnDesc[0]));
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void removeColumn(String tableIdentity, String... column) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val columns = Sets.newHashSet(column);
        val newColumns = Stream.of(factTable.getColumns()).filter(col -> !columns.contains(col.getName()))
                .toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void changeColumnName(String tableIdentity, String oldName, String newName) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Stream.of(factTable.getColumns()).map(columnDesc -> {
            if (columnDesc.getName().equals(oldName)) {
                columnDesc.setName(newName);
            }
            return columnDesc;
        }).toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }
}
