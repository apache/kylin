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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.INDEX_DUPLICATE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.LAYOUT_NOT_EXISTS;
import static org.apache.kylin.metadata.cube.model.IndexEntity.Source.CUSTOM_TABLE_INDEX;
import static org.apache.kylin.metadata.cube.model.IndexEntity.Source.RECOMMENDED_TABLE_INDEX;
import static org.apache.kylin.metadata.model.SegmentStatusEnum.READY;
import static org.apache.kylin.metadata.model.SegmentStatusEnum.WARNING;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.engine.spark.job.ExecutableAddCuboidHandler;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.request.AggShardByColumnsRequest;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.AggIndexCombResult;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.response.OpenGetIndexResponse;
import org.apache.kylin.rest.response.TableIndexResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexPlanServiceTest extends SourceTestCase {

    @InjectMocks
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.setup();
        indexPlanService.setSemanticUpater(semanticService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
    }

    private AtomicBoolean prepare(String modelId) {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        AtomicBoolean clean = new AtomicBoolean(false);
        Assert.assertFalse(clean.get());
        return clean;
    }

    private UpdateRuleBasedCuboidRequest createUpdateRuleRequest(String project, String modelId,
            NAggregationGroup aggregationGroup, boolean restoreDelIndex) {
        return UpdateRuleBasedCuboidRequest.builder().project(project).modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).restoreDeletedIndex(restoreDelIndex).build();
    }

    private NAggregationGroup mkAggGroup(Integer... dimension) {
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(dimension);
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        return aggregationGroup;
    }

    @Test
    public void testUpdateRuleWithRollbackBlacklistLayout() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        prepare(modelId);

        NAggregationGroup aggregationGroup = mkAggGroup(1);
        IndexPlan saved = indexPlanService.updateRuleBasedCuboid(getProject(),
                createUpdateRuleRequest(getProject(), modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        List<Long> ids = saved.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());
        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> copyForWrite.addRuleBasedBlackList(ids));

        aggregationGroup = mkAggGroup(1, 2);
        val updateRuleRequest = createUpdateRuleRequest(getProject(), modelId, aggregationGroup, true);

        saved = indexPlanService.updateRuleBasedCuboid(getProject(), updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
    }

    @Test
    public void testUpdateRuleWithDelBlacklistLayout() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        prepare(modelId);

        val rq = createUpdateRuleRequest(getProject(), modelId, mkAggGroup(1), false);
        IndexPlan saved = indexPlanService.updateRuleBasedCuboid(getProject(), rq).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        List<Long> ids = saved.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList());
        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> copyForWrite.addRuleBasedBlackList(ids));

        val updateRuleRequest = createUpdateRuleRequest(getProject(), modelId, mkAggGroup(1, 2), false);
        val diff = indexPlanService.calculateDiffRuleBasedIndex(updateRuleRequest);

        Assert.assertThat(diff.getIncreaseLayouts(), is(2));
        Assert.assertThat(diff.getDecreaseLayouts(), is(0));
        Assert.assertThat(diff.getRollbackLayouts(), is(1));
        saved = indexPlanService.updateRuleBasedCuboid(getProject(), updateRuleRequest).getFirst();
        Assert.assertEquals(2, saved.getRuleBaseLayouts().size());
    }

    @Test
    public void testUpdateSingleRuleBasedCuboid() throws Exception {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan(modelId);
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        val saved = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default")
                                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 15, saved.getAllLayouts().size());
        // Assert.assertTrue(clean.get());
    }

    @Test
    public void testUpdateRuleBasedSortDimension() {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup1.setSelectRule(selectRule);

        val aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 4, 3, 5 });
        aggregationGroup2.setSelectRule(selectRule);

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId(modelId)
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .build();
        val diff1 = indexPlanService.calculateDiffRuleBasedIndex(request);
        Assert.assertTrue(diff1.getIncreaseLayouts() > 0);

        var saved = indexPlanService.updateRuleBasedCuboid("default", request).getFirst();
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(5, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 3, 4, 5]", saved.getRuleBasedIndex().getDimensions().toString());

        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3 });
        aggregationGroup2.setIncludes(new Integer[] { 4, 3 });

        val aggregationGroup3 = new NAggregationGroup();
        aggregationGroup3.setIncludes(new Integer[] { 2, 4 });
        aggregationGroup3.setSelectRule(selectRule);

        val aggregationGroup4 = new NAggregationGroup();
        aggregationGroup4.setIncludes(new Integer[] { 5, 4 });
        aggregationGroup4.setSelectRule(selectRule);

        request = UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId)
                .aggregationGroups(
                        Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3, aggregationGroup4))
                .build();
        val diff2 = indexPlanService.calculateDiffRuleBasedIndex(request);
        Assert.assertTrue(diff2.getDecreaseLayouts() > 0 && diff2.getIncreaseLayouts() > 0);

        saved = indexPlanService.updateRuleBasedCuboid("default", request).getFirst();

        Assert.assertEquals(5, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 5, 4, 3]", saved.getRuleBasedIndex().getDimensions().toString());

        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        aggregationGroup2.setIncludes(new Integer[] { 2, 5, 6, 4 });
        aggregationGroup3.setIncludes(new Integer[] { 5, 3 });

        saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId)
                        .aggregationGroups(Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3))
                        .build())
                .getFirst();

        Assert.assertEquals(6, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 5, 6, 3, 4]", saved.getRuleBasedIndex().getDimensions().toString());

        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3 });
        aggregationGroup2.setIncludes(new Integer[] { 2, 4 });
        aggregationGroup3.setIncludes(new Integer[] { 4, 3 });
        aggregationGroup4.setIncludes(new Integer[] { 3, 2 });

        saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId).aggregationGroups(
                        Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3, aggregationGroup4))
                        .build())
                .getFirst();

        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 3, 4]", saved.getRuleBasedIndex().getDimensions().toString());
    }

    @Test
    public void testUpdateIndexPlanDuplicate() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        val saved = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default")
                                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 15, saved.getAllLayouts().size());

        long lastModifiedTime = saved.getRuleBasedIndex().getLastModifiedTime();

        val res = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build());
        long lastModifiedTime2 = res.getFirst().getRuleBasedIndex().getLastModifiedTime();
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_LAYOUT, res.getSecond().getType());
        Assert.assertTrue(lastModifiedTime2 > lastModifiedTime);
    }

    @Test
    public void testUpdateIndexPlanWithToBeDelete() {
        val id = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan(id);
        var aggregationGroup = getNAggregationGroup(new Integer[] { 100000 });

        val saved = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default").modelId(id).isLoadData(true)
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertEquals(origin.getAllLayouts().size() + 3, saved.getAllLayouts().size());

        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val readySegs = dfManager.getDataflow(id).getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        val last = readySegs.getLatestReadySegment().getSegDetails();
        List<NDataLayout> lists = Lists.newArrayList(last.getLayouts());
        saved.getRuleBasedIndex().getLayoutIdMapping().forEach(layoutId -> {
            if (saved.getLayoutEntity(layoutId).getDimsIds().size() > 1) {
                lists.add(NDataLayout.newDataLayout(last, layoutId));
            }
        });
        dfManager.updateDataflowDetailsLayouts(readySegs.getLatestReadySegment(), lists);
        indexPlanManager.updateIndexPlan(id, copyForWrite -> {
            copyForWrite.markIndexesToBeDeleted(copyForWrite.getId(), saved.getRuleBaseLayouts().stream()
                    .filter(layoutEntity -> layoutEntity.getColOrder().size() == 3).collect(Collectors.toSet()));
        });

        aggregationGroup = getNAggregationGroup(new Integer[] { 100000, 100005 });
        var res = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default").modelId(id).isLoadData(true)
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertEquals(1, res.getToBeDeletedIndexes().size());
        aggregationGroup = getNAggregationGroup(new Integer[] { 100000 });
        res = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default")
                                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertEquals(0, res.getToBeDeletedIndexes().size());
    }

    private NAggregationGroup getNAggregationGroup(Integer[] measures) {
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2 });
        aggregationGroup.setMeasures(measures);
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[] {};
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        return aggregationGroup;
    }

    @Test
    public void testUpdateRuleBasedIndexWithDifferentMeasure() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        aggregationGroup1.setMeasures(new Integer[] { 100000, 100001, 100002, 100003 });
        val selectRule1 = new SelectRule();
        selectRule1.mandatoryDims = new Integer[] { 1 };
        selectRule1.hierarchyDims = new Integer[][] { { 2, 3 } };
        selectRule1.jointDims = new Integer[0][0];
        aggregationGroup1.setSelectRule(selectRule1);
        val aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 1, 3, 4, 5 });
        aggregationGroup2.setMeasures(new Integer[] { 100001, 100003, 100004, 100005 });
        val selectRule2 = new SelectRule();
        selectRule2.mandatoryDims = new Integer[] { 3 };
        selectRule2.hierarchyDims = new Integer[0][0];
        selectRule2.jointDims = new Integer[][] { { 4, 5 } };
        aggregationGroup2.setSelectRule(selectRule2);

        val revertedBefore = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                        .build())
                .getFirst();
        Assert.assertNotNull(revertedBefore.getRuleBasedIndex());
        Assert.assertEquals(5, revertedBefore.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(6, revertedBefore.getRuleBasedIndex().getMeasures().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 11, revertedBefore.getAllLayouts().size());

        // revert agg groups order
        val reverted = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup2, aggregationGroup1))
                        .build())
                .getFirst();

        Assert.assertEquals(5, revertedBefore.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(6, revertedBefore.getRuleBasedIndex().getMeasures().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 11, revertedBefore.getAllLayouts().size());

        Assert.assertEquals(revertedBefore.getRuleBasedIndex().getMeasures(),
                reverted.getRuleBasedIndex().getMeasures());
    }

    @Test
    public void testUpdateIndexPlanWithNoSegment() {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val dfUpdate = new NDataflowUpdate(df.getId());
        dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(dfUpdate);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);

        val res = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build());
        val saved = res.getFirst();
        val response = res.getSecond();
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_SEGMENT, response.getType());
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 15, saved.getAllLayouts().size());
    }

    @Test
    public void testCreateTableIndex() throws Exception {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        var response = indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .layoutOverrideIndexes(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
        val saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout == null) {
                newLayout = layout;
            } else {
                if (newLayout.getId() < layout.getId()) {
                    newLayout = layout;
                }
            }
        }
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(1, 2, 3, 4)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(1)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
        // Assert.assertTrue(clean.get());
        deleteJobByForce(executables.get(0));

        int before = origin.getIndexes().size();
        response = indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).isLoadData(true)
                        .layoutOverrideIndexes(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());

        int after = origin.getIndexes().size();
        Assert.assertEquals(before, after);
    }

    @Test
    public void testCreateTableIndexWithId() throws Exception {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        var response = indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .layoutOverrideIndexes(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build(),
                20000040000L);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
        val saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout == null) {
                newLayout = layout;
            } else {
                if (newLayout.getId() < layout.getId()) {
                    newLayout = layout;
                }
            }
        }
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(1, 2, 3, 4)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(1)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        indexPlanService.updateTableIndex("default",
                CreateTableIndexRequest.builder().id(20000040000L).project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).build());

        Assert.assertFalse(indexPlanService.getManager(NIndexPlanManager.class, "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getAllLayouts().stream()
                .anyMatch(l -> l.getId() == 20000040000L));
        Assert.assertTrue(indexPlanService.getManager(NIndexPlanManager.class, "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getAllLayouts().stream()
                .anyMatch(l -> l.getId() == 20000040000L + IndexEntity.INDEX_ID_STEP));
    }

    @Test
    public void testCreateTableIndexIsAuto() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val originLayoutSize = origin.getAllLayouts().size();
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_SITES.SITE_NAME",
                                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_SITE_ID", "TEST_KYLIN_FACT.PRICE"))
                        .sortByColumns(Arrays.asList("TEST_SITES.SITE_NAME")).build());
        var saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(originLayoutSize, saved.getAllLayouts().size());
        var layout = saved.getLayoutEntity(20000000001L);
        Assert.assertTrue(layout.isManual());
        Assert.assertTrue(layout.isAuto());

        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, executables.size());

        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000000001L);
        saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        layout = saved.getLayoutEntity(20000000001L);
        Assert.assertEquals(originLayoutSize, saved.getAllLayouts().size());
        Assert.assertFalse(layout.isManual());
        Assert.assertTrue(layout.isAuto());
    }

    @Test
    public void testCreateDuplicateTableIndex() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(INDEX_DUPLICATE.getMsg());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());

    }

    @Test
    public void testRemoveTableIndex() {
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000040001L);

        Assert.assertFalse(indexPlanService.getManager(NIndexPlanManager.class, "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getAllLayouts().stream()
                .anyMatch(l -> l.getId() == 20000040001L));
    }

    @Test
    public void testRemoveWrongId() {
        thrown.expect(IllegalStateException.class);
        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000020001L);
    }

    @Test
    public void testUpdateTableIndex() throws Exception {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        long prevMaxId = 20000040001L;
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId(modelId)
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        var executables = getRunningExecutables("default", modelId);
        Assert.assertEquals(1, executables.size());
        deleteJobByForce(executables.get(0));
        val response = indexPlanService.updateTableIndex("default",
                CreateTableIndexRequest.builder().id(prevMaxId).project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).build());

        Assert.assertFalse(indexPlanService.getManager(NIndexPlanManager.class, "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getAllLayouts().stream()
                .anyMatch(l -> l.getId() == prevMaxId));
        Assert.assertTrue(indexPlanService.getManager(NIndexPlanManager.class, "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getAllLayouts().stream()
                .anyMatch(l -> l.getId() == prevMaxId + IndexEntity.INDEX_ID_STEP));
        executables = getRunningExecutables("default", modelId);
        Assert.assertEquals(1, executables.size());
        // Assert.assertTrue(clean.get());
    }

    @Test
    public void testUpdateTableIndex_markToBeDeleted() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        long existLayoutId = 20000010001L;
        long maxTableLayoutId = indexPlanService.getManager(NIndexPlanManager.class, project).getIndexPlan(modelId)
                .getWhitelistLayouts().stream().map(LayoutEntity::getId).max(Long::compare).get();
        indexPlanService.updateTableIndex(project,
                CreateTableIndexRequest.builder().id(existLayoutId).project(project).modelId(modelId)
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(false).build());

        Assert.assertTrue(indexPlanService.getManager(NIndexPlanManager.class, project).getIndexPlan(modelId)
                .getAllLayouts().stream().anyMatch(l -> l.getId() == existLayoutId));
        Assert.assertTrue(indexPlanService.getManager(NIndexPlanManager.class, project).getIndexPlan(modelId)
                .getToBeDeletedIndexes().stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                .anyMatch(l -> l.getId() == existLayoutId));
        Assert.assertTrue(indexPlanService.getManager(NIndexPlanManager.class, project).getIndexPlan(modelId)
                .getAllLayouts().stream().anyMatch(l -> l.getId() == maxTableLayoutId + IndexEntity.INDEX_ID_STEP));
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        NDataSegment segment = df.getLatestReadySegment();
        Assert.assertNotNull(segment.getLayout(existLayoutId));
    }

    @Test
    public void testGetTableIndex() {
        val originSize = indexPlanService.getTableIndexs("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa").size();
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                                "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME", "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                                "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val result = indexPlanService.getTableIndexs("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(3 + originSize, result.size());
        val first = result.get(originSize);
        Assert.assertThat(first.getColOrder(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID",
                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID")));
        Assert.assertThat(first.getShardByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(20000040001L, first.getId().longValue());
        Assert.assertEquals("default", first.getProject());
        Assert.assertEquals("ADMIN", first.getOwner());
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", first.getModel());
        Assert.assertEquals(TableIndexResponse.Status.EMPTY, first.getStatus());
        Assert.assertTrue(first.isManual());
        Assert.assertFalse(first.isAuto());
    }

    @Test
    public void testGetRule() throws Exception {
        Assert.assertNull(indexPlanService.getRule("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        val rule = JsonUtil.deepCopy(indexPlanService.getRule("default", "741ca86a-1f13-46da-a59f-95fb68615e3a"),
                RuleBasedIndex.class);
        Assert.assertNotNull(rule);
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Lists.newArrayList(1, 2, 3));
        newRule.setMeasures(Lists.newArrayList(1001, 1002));

        indePlanManager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copy -> {

            copy.setRuleBasedIndex(newRule);
        });
        val rule2 = indexPlanService.getRule("default", "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertNotEquals(rule2, rule);
    }

    @Test
    public void testCalculateAggIndexCountEmpty() throws Exception {
        String aggGroupStr = "{\"includes\":[],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        val aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        val ret = calculateCount(Lists.newArrayList(aggGroup));
        AggIndexCombResult aggIndexCombResult = ret.getAggIndexCounts().get(0);
        Assert.assertEquals(0L, aggIndexCombResult.getResult());
        Assert.assertEquals(0L, ret.getTotalCount().getResult());
    }

    @Test
    public void testCalculateAggIndexCount() throws Exception {
        String aggGroupStr = "{\"includes\":[0, 1, 2, 3],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[[1, 3]],\"joint_dims\":[]}}";
        val aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        val ret = calculateCount(Lists.newArrayList(aggGroup));
        AggIndexCombResult aggIndexCombResult = ret.getAggIndexCounts().get(0);
        Assert.assertEquals(11L, aggIndexCombResult.getResult());
        Assert.assertEquals(11L, ret.getTotalCount().getResult());
    }

    @Test
    public void testCalculateAggIndexCountFail() throws Exception {
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,13,14,15],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        val aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        var response = calculateCount(Lists.newArrayList(aggGroup));
        Assert.assertEquals(1, response.getAggIndexCounts().size());
        Assert.assertEquals("FAIL", response.getAggIndexCounts().get(0).getStatus());
        Assert.assertEquals("FAIL", response.getTotalCount().getStatus());
    }

    @Test
    public void testCalculateAggIndexCountTwoGroups() throws Exception {
        String aggGroupStr1 = "{\"includes\":[0,1,2,3,4,5],\"select_rule\":{\"mandatory_dims\":[0, 1, 2],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        String aggGroupStr2 = "{\"includes\":[2,3,4,5,6,7],\"select_rule\":{\"mandatory_dims\":[2, 4],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        val aggGroup1 = JsonUtil.readValue(aggGroupStr1, NAggregationGroup.class);
        val aggGroup2 = JsonUtil.readValue(aggGroupStr2, NAggregationGroup.class);
        val ret = calculateCount(Lists.newArrayList(aggGroup1, aggGroup2));
        Assert.assertEquals(8L, ret.getAggIndexCounts().get(0).getResult());
        Assert.assertEquals(4096L, ret.getAggrgroupMaxCombination().longValue());
        Assert.assertEquals(16L, ret.getAggIndexCounts().get(1).getResult());
        Assert.assertEquals(25L, ret.getTotalCount().getResult());
    }

    @Test
    public void testCheckIndexCountWithinLimit() {
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,10,11],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[],\"dim_cap\":2}}";
        NAggregationGroup aggGroup = null;
        try {
            aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        } catch (IOException e) {
            log.error("Read value fail ", e);
        }
        var request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(Lists.newArrayList(aggGroup))
                .build();
        indexPlanService.checkIndexCountWithinLimit(request);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIndexCountWithinLimitFail() {
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,10,11,12],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        NAggregationGroup aggGroup = null;
        try {
            aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        } catch (IOException e) {
            log.error("Read value fail ", e);
        }
        var request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(Lists.newArrayList(aggGroup))
                .build();
        indexPlanService.checkIndexCountWithinLimit(request);
    }

    @Test
    public void testUpdateAggShardByColumns() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val request = new AggShardByColumnsRequest();
        request.setModelId(modelId);
        request.setProject("default");
        request.setLoadData(true);
        request.setShardByColumns(Lists.newArrayList("TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
        indexPlanService.updateShardByColumns("default", request);

        var indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        var layouts = indexPlan.getRuleBaseLayouts();
        for (LayoutEntity layout : layouts) {
            if (layout.getColOrder().containsAll(Lists.newArrayList(2, 3))) {
                Assert.assertEquals(2, layout.getId() % IndexEntity.INDEX_ID_STEP);
            } else {
                Assert.assertEquals(1, layout.getId() % IndexEntity.INDEX_ID_STEP);
            }

        }

        val response = indexPlanService.getShardByColumns("default", modelId);
        Assert.assertArrayEquals(request.getShardByColumns().toArray(new String[0]),
                response.getShardByColumns().toArray(new String[0]));

        val executables = getRunningExecutables("default", modelId);
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);

        // change shard by columns
        request.setShardByColumns(Lists.newArrayList("TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
        request.setLoadData(false);
        indexPlanService.updateShardByColumns("default", request);

        indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        layouts = indexPlan.getRuleBaseLayouts();
        for (LayoutEntity layout : layouts) {
            if (layout.getColOrder().containsAll(Lists.newArrayList(2, 3))) {
                Assert.assertEquals(3, layout.getId() % IndexEntity.INDEX_ID_STEP);
            } else if (layout.getColOrder().contains(3)) {
                Assert.assertEquals(2, layout.getId() % IndexEntity.INDEX_ID_STEP);
            } else {
                Assert.assertEquals(1, layout.getId() % IndexEntity.INDEX_ID_STEP);
            }
        }
    }

    @Test
    public void testUpdateAggShard_WithInvalidColumn() {
        val wrongColumn = "TEST_CAL_DT.WEEK_BEG_DT";
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, Message.getInstance().getColumuIsNotDimension(), wrongColumn));
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val request = new AggShardByColumnsRequest();
        request.setModelId(modelId);
        request.setProject("default");
        request.setLoadData(true);
        request.setShardByColumns(
                Lists.newArrayList("TEST_KYLIN_FACT.CAL_DT", wrongColumn, "TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
        indexPlanService.updateShardByColumns("default", request);
    }

    @Test
    public void testExtendPartitionColumns() {
        NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel("741ca86a-1f13-46da-a59f-95fb68615e3a", modeDesc -> modeDesc.setStorageType(2));
        NIndexPlanManager instance = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        IndexPlan indexPlan = instance.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        checkLayoutEntityPartitionCoulumns(instance, indexPlan);
        instance.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", index -> {
            List<Integer> extendPartitionColumns = new ArrayList<>(index.getExtendPartitionColumns());
            extendPartitionColumns.add(1);
            index.setExtendPartitionColumns(extendPartitionColumns);
        });
        indexPlan = instance.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        checkLayoutEntityPartitionCoulumns(instance, indexPlan);

    }

    private void checkLayoutEntityPartitionCoulumns(NIndexPlanManager instance, IndexPlan indexPlan) {
        ArrayList<Integer> partitionColumns = new ArrayList<>(indexPlan.getExtendPartitionColumns());
        instance.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").getAllIndexes().stream()
                .flatMap(indexEntity -> indexEntity.getLayouts().stream()).filter(LayoutEntity::isManual)
                .filter(layoutEntity -> !layoutEntity.isAuto()).forEach(layoutEntity -> {
                    if (layoutEntity.getOrderedDimensions().keySet().containsAll(partitionColumns)) {
                        if (!ListUtils.isEqualList(layoutEntity.getPartitionByColumns(), partitionColumns)) {
                            throw new RuntimeException("Partition column is not match.");
                        }
                    }
                });
    }

    private AggIndexResponse calculateCount(List<NAggregationGroup> aggGroups) {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(aggGroups).build();

        return indexPlanService.calculateAggIndexCount(request);
    }

    @Test
    public void testRemoveWarningSegmentIndex() throws Exception {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        val manualAgg = indexPlan.getLayoutEntity(1010001L);
        Assert.assertNotNull(manualAgg);
        Assert.assertTrue(manualAgg.isManual());

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val df = dataflowManager.getDataflow(modelId);
        val dfUpdate = new NDataflowUpdate(df.getId());
        dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(modelId, dataflow -> {
            dataflow.getSegments().getLastSegment().setStatus(SegmentStatusEnum.WARNING);
        });
        Assert.assertEquals(WARNING, dataflowManager.getDataflow(modelId).getLastSegment().getStatus());
        indexPlanService.removeIndexes(getProject(), modelId, df.getLastSegment().getLayoutIds());
        Assert.assertEquals(READY, dataflowManager.getDataflow(modelId).getLastSegment().getStatus());
    }

    @Test
    public void testRemoveWarningSegmentIndexFromSegment() throws Exception {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        val manualAgg = indexPlan.getLayoutEntity(1010001L);
        Assert.assertNotNull(manualAgg);
        Assert.assertTrue(manualAgg.isManual());

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val df = dataflowManager.getDataflow(modelId);
        val dfUpdate = new NDataflowUpdate(df.getId());
        dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(modelId, dataflow -> {
            dataflow.getSegments().getLastSegment().setStatus(SegmentStatusEnum.WARNING);
        });

        Assert.assertEquals(WARNING, dataflowManager.getDataflow(modelId).getLastSegment().getStatus());
        dataflowManager.updateDataflowDetailsLayouts(df.getLastSegment(), Lists.newArrayList());
        Assert.assertEquals(READY, dataflowManager.getDataflow(modelId).getLastSegment().getStatus());
    }

    @Test
    public void testRemoveIndex() throws Exception {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        val manualAgg = indexPlan.getLayoutEntity(1010001L);
        Assert.assertNotNull(manualAgg);
        Assert.assertTrue(manualAgg.isManual());
        indexPlanService.removeIndex(getProject(), modelId, manualAgg.getId());
        indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNull(indexPlan.getLayoutEntity(1010001L));
        val autoTable = indexPlan.getLayoutEntity(20000000001L);
        Assert.assertNotNull(autoTable);
        Assert.assertTrue(autoTable.isAuto());
        indexPlanService.removeIndex(getProject(), modelId, autoTable.getId());
        indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNull(indexPlan.getLayoutEntity(20000000001L));

        testUpdateTableIndex_markToBeDeleted();
        indexPlanService.removeIndex(getProject(), modelId, 20000010001L);
        indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNull(indexPlan.getLayoutEntity(20000010001L));
        // Assert.assertTrue(clean.get());
    }

    @Test
    public void testRemoveIndexes() throws Exception {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        var manualAgg = indexPlan.getLayoutEntity(1010001L);
        Assert.assertNotNull(manualAgg);
        var autoTable = indexPlan.getLayoutEntity(20000000001L);
        Assert.assertNotNull(autoTable);

        autoTable = indexPlan.getLayoutEntity(20000010001L);
        Assert.assertNotNull(autoTable);

        // delete layoutIds
        indexPlanService.removeIndexes(getProject(), modelId,
                new HashSet<>(Arrays.asList(1010001L, 20000000001L, 20000010001L)));

        indexPlan = indexPlanManager.getIndexPlan(modelId);

        manualAgg = indexPlan.getLayoutEntity(1010001L);
        Assert.assertNull(manualAgg);
        autoTable = indexPlan.getLayoutEntity(20000000001L);
        Assert.assertNull(autoTable);

        autoTable = indexPlan.getLayoutEntity(20000010001L);
        Assert.assertNull(autoTable);

        // delete not exists layoutIds
        thrown.expect(KylinException.class);
        thrown.expectMessage(LAYOUT_NOT_EXISTS.getMsg("1010001,20000000001,20000010001"));
        indexPlanService.removeIndexes(getProject(), modelId,
                new HashSet<>(Arrays.asList(1010001L, 20000000001L, 20000010001L)));

        // empty layoutIds
        thrown.expect(KylinException.class);
        thrown.expectMessage("Layouts id list can not empty!");
        indexPlanService.removeIndexes(getProject(), modelId, new HashSet<>());
    }

    @Test
    public void testGetIndexes() throws Exception {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var indexResponses = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(), "data_size",
                true, Lists.newArrayList());
        Assert.assertEquals(25, indexResponses.size());
        Assert.assertEquals(1000001L, indexResponses.get(0).getId().longValue());

        indexResponses = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(), "data_size", true,
                Lists.newArrayList(RECOMMENDED_TABLE_INDEX, CUSTOM_TABLE_INDEX));

        List<OpenGetIndexResponse.IndexDetail> indexDetails = indexResponses.stream()
                .map(OpenGetIndexResponse.IndexDetail::newIndexDetail).collect(Collectors.toList());
        indexDetails.forEach(indexDetail -> {
            if (indexDetail.getId() == 20000000001L || indexDetail.getId() == 20000010001L) {
                Assert.assertEquals(RECOMMENDED_TABLE_INDEX, indexDetail.getSource());
                Assert.assertEquals(IndexEntity.Status.ONLINE, indexDetail.getStatus());
            } else if (indexDetail.getId() == 20000020001L || indexDetail.getId() == 20000030001L) {
                Assert.assertEquals(RECOMMENDED_TABLE_INDEX, indexDetail.getSource());
                Assert.assertEquals(IndexEntity.Status.BUILDING, indexDetail.getStatus());
            }
        });

        Assert.assertTrue(
                indexResponses.stream().allMatch(indexResponse -> RECOMMENDED_TABLE_INDEX == indexResponse.getSource()
                        || CUSTOM_TABLE_INDEX == indexResponse.getSource()));

        // test default order by
        indexResponses = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(), null, false,
                null);
        Assert.assertSame(IndexEntity.Status.BUILDING, indexResponses.get(0).getStatus());
        IndexResponse prev = null;
        for (IndexResponse current : indexResponses) {
            if (prev == null) {
                prev = current;
                continue;
            }
            if (current.getStatus() == IndexEntity.Status.ONLINE) {
                Assert.assertTrue(current.getDataSize() >= prev.getDataSize());
            }
            prev = current;
        }
    }

    @Test
    public void testGetIndexes_WithKey() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        // find normal column, and measure parameter
        var response = indexPlanService.getIndexes(getProject(), modelId, "PRICE", Lists.newArrayList(), "data_size",
                false, null);
        var ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(18, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
        Assert.assertTrue(ids.contains(10001L));

        // find CC column as dimension
        response = indexPlanService.getIndexes(getProject(), modelId, "NEST3", Lists.newArrayList(), "data_size", false,
                null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(2, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
        Assert.assertTrue(ids.contains(1000001L));

        // find CC column as measure
        response = indexPlanService.getIndexes(getProject(), modelId, "NEST4", Lists.newArrayList(), "data_size", false,
                null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(12, response.size());
        for (IndexResponse res : response) {
            Assert.assertTrue(res.getColOrder().stream().map(IndexResponse.ColOrderPair::getKey)
                    .anyMatch(col -> col.equals("TEST_KYLIN_FACT.NEST4") || col.equals("SUM_NEST4")));
        }

        response = indexPlanService.getIndexes(getProject(), modelId, "nest4", Lists.newArrayList(), "data_size", false,
                null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(12, response.size());
        for (IndexResponse res : response) {
            Assert.assertTrue(res.getColOrder().stream().map(IndexResponse.ColOrderPair::getKey)
                    .anyMatch(col -> col.equals("TEST_KYLIN_FACT.NEST4") || col.equals("SUM_NEST4")));
        }
    }

    @Test
    public void testGetIndexes_WithStatus() throws Exception {
        testUpdateTableIndex_markToBeDeleted();

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        // find normal column, and measure parameter
        var response = indexPlanService.getIndexes(getProject(), modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null);
        var ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(3, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
        Assert.assertTrue(ids.contains(20000030001L));

        response = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(IndexEntity.Status.ONLINE),
                "data_size", false, null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(7, response.size());
        Assert.assertFalse(ids.contains(20000010001L));
        Assert.assertTrue(ids.contains(10001L));

        response = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(IndexEntity.Status.LOCKED),
                "data_size", false, null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(1, response.size());
        Assert.assertTrue(ids.contains(20000010001L));

        Mockito.doReturn(Sets.newHashSet(20000020001L)).when(indexPlanService).getLayoutsByRunningJobs(getProject(),
                modelId);
        response = indexPlanService.getIndexes(getProject(), modelId, "",
                Lists.newArrayList(IndexEntity.Status.BUILDING), "data_size", false, null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(1, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
    }

    @Test
    public void testGetIndex_WithIndexId() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        List<Long> ids = Arrays.asList(20000020001L, 20000030001L);
        var response = indexPlanService.getIndexes(getProject(), modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, ids);
        var actual_ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(ids.size(), response.size());
        Assert.assertTrue(actual_ids.contains(20000020001L));
        Assert.assertTrue(actual_ids.contains(20000030001L));
    }

    @Test
    public void testGetIndex_WithRelatedTables() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        List<Long> ids = Arrays.asList(20000020001L, 20000030001L);
        var response = indexPlanService.getIndexesWithRelatedTables(getProject(), modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, ids);
        Assert.assertEquals(ids.size(), response.size());
        response.forEach(indexResponse -> {
            val relatedTables = indexResponse.getRelatedTables();
            if (indexResponse.getId() == 20000030001L) {
                Assert.assertEquals(2, relatedTables.size());
                Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", relatedTables.get(0));
            } else if (indexResponse.getId() == 20000020001L) {
                Assert.assertEquals(6, relatedTables.size());
                Assert.assertEquals("DEFAULT.TEST_ACCOUNT", relatedTables.get(0));
            }
        });
    }

    @Test
    public void testGetIndexGraph_EmptyFullLoad() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(modelId, copyForWrite -> {
            copyForWrite.setPartitionDesc(null);
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        dataflowManager.dropDataflow(modelId);
        indexManager.dropIndexPlan(modelId);
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(modelId);
        indexManager.createIndexPlan(indexPlan);
        val dataflow = new NDataflow();
        dataflow.setUuid(modelId);
        dataflowManager.createDataflow(indexPlan, "ADMIN");
        val df = dataflowManager.getDataflow(modelId);
        dataflowManager.fillDfManually(df,
                Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));

        val response = indexPlanService.getIndexGraph(getProject(), modelId, 100);
        Assert.assertEquals(0, response.getStartTime());
        Assert.assertEquals(Long.MAX_VALUE, response.getEndTime());
        Assert.assertTrue(response.isFullLoaded());

    }

    @Test
    public void testSegmentComplementCount() {
        String project = "default";
        String modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);

        //clear segment from df
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        long tobeDeleteLayoutId = 20000000001L;
        //add two segment(include full layout)
        val update2 = new NDataflowUpdate(df.getUuid());
        val seg1 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val seg2 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg2.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(seg1, seg2);

        List<NDataLayout> layouts1 = Lists.newArrayList();
        List<NDataLayout> layouts2 = Lists.newArrayList();
        indexManager.getIndexPlan(modelId).getAllLayouts().forEach(layout -> {
            layouts1.add(NDataLayout.newDataLayout(df, seg1.getId(), layout.getId()));
            if (layout.getId() != 1) {
                layouts2.add(NDataLayout.newDataLayout(df, seg2.getId(), layout.getId()));
            }
        });

        List<NDataLayout> layouts = Lists.newArrayList();
        layouts.addAll(layouts1);
        layouts.addAll(layouts2);
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        dfManager.updateDataflow(update2);
        Assert.assertEquals(1,
                indexPlanService.getIndexGraph(getProject(), modelId, 100).getSegmentToComplementCount());

        // mark a layout tobedelete
        indexManager.updateIndexPlan(modelId,
                copyForWrite -> copyForWrite.markWhiteIndexToBeDelete(modelId, Sets.newHashSet(tobeDeleteLayoutId)));

        //remove tobedelete layout from seg1
        val newDf = dfManager.getDataflow(modelId);
        dfManager.updateDataflowDetailsLayouts(newDf.getSegment(seg1.getId()), layouts1.stream()
                .filter(layout -> layout.getLayoutId() != tobeDeleteLayoutId).collect(Collectors.toList()));
        //segment1
        Assert.assertEquals(1,
                indexPlanService.getIndexGraph(getProject(), modelId, 100).getSegmentToComplementCount());
    }

    @Test
    public void testUpdateIndexPlanWithMDC() throws Exception {
        // aggregationGroup1 will generate [1,2,10000] [1,3,10000] [1,4,10000] [1,10000] total 4 layout
        NAggregationGroup aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        aggregationGroup1.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule1 = new SelectRule();
        selectRule1.setMandatoryDims(new Integer[] { 1 });
        selectRule1.setDimCap(1);
        aggregationGroup1.setSelectRule(selectRule1);

        // aggregationGroup2 will generate [5,10000,10001] [5,6,7,10000,10001] total 2 layout
        NAggregationGroup aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 5, 6, 7 });
        aggregationGroup2.setMeasures(new Integer[] { 10000, 10001 });
        SelectRule selectRule2 = new SelectRule();
        selectRule2.setMandatoryDims(new Integer[] { 5 });
        selectRule2.setJointDims(new Integer[][] { { 6, 7 } });
        aggregationGroup2.setSelectRule(selectRule2);

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .build();
        request.setGlobalDimCap(2);

        AggIndexResponse aggIndexResponse = indexPlanService.calculateAggIndexCount(request);
        List<AggIndexCombResult> aggIndexCounts = aggIndexResponse.getAggIndexCounts();
        Assert.assertEquals(4L, aggIndexCounts.get(0).getResult());
        Assert.assertEquals(2L, aggIndexCounts.get(1).getResult());
        // 4 + 2 + baseCuboid
        Assert.assertEquals(7L, aggIndexResponse.getTotalCount().getResult());

        val diff = indexPlanService.calculateDiffRuleBasedIndex(request);
        Assert.assertEquals(7, (int) diff.getIncreaseLayouts());

        IndexPlan indexPlan = indexPlanService.updateRuleBasedCuboid("default", request).getFirst();
        Assert.assertEquals(7, indexPlan.getRuleBaseLayouts().size());

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        List<LayoutEntity> ruleBaseLayouts = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .getIndexPlan().getRuleBaseLayouts();
        Assert.assertEquals(7L, ruleBaseLayouts.size());
        Assert.assertEquals("[1, 2, 10000]", ruleBaseLayouts.get(0).getColOrder().toString());
        Assert.assertEquals("[5, 6, 7, 10000, 10001]", ruleBaseLayouts.get(1).getColOrder().toString());
        Assert.assertEquals("[1, 3, 10000]", ruleBaseLayouts.get(2).getColOrder().toString());
        Assert.assertEquals("[1, 4, 10000]", ruleBaseLayouts.get(3).getColOrder().toString());
        Assert.assertEquals("[1, 10000]", ruleBaseLayouts.get(4).getColOrder().toString());
        Assert.assertEquals("[5, 10000, 10001]", ruleBaseLayouts.get(5).getColOrder().toString());
        Assert.assertEquals("[1, 2, 3, 4, 5, 6, 7, 10000, 10001]", ruleBaseLayouts.get(6).getColOrder().toString());
    }

    @Test
    public void testCalculateAggIndexCountWhenTotalCuboidsOutOfMaxComb() throws Exception {
        testOutOfCombination(1);
    }

    @Test
    public void testCalculateAggIndexCountWhenTotalCuboidsOutOfMaxComb_WithSchedulerV2() throws Exception {
        testOutOfCombination(2);
    }

    private void testOutOfCombination(int version) {
        // agg group1 over 4096
        NAggregationGroup aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
                45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
                71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
                97, 98, 99 });
        aggregationGroup1.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule1 = new SelectRule();
        selectRule1.setMandatoryDims(new Integer[] {});
        selectRule1.setDimCap(100);
        aggregationGroup1.setSelectRule(selectRule1);

        // agg group2 is normal
        NAggregationGroup aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 5, 6, 7 });
        aggregationGroup2.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule2 = new SelectRule();
        selectRule2.setMandatoryDims(new Integer[] {});
        aggregationGroup2.setSelectRule(selectRule2);

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("enormous_cuboids_test")
                .modelId("c4437350-fa42-48b4-b1e4-060ae92ab527")
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .schedulerVersion(version).build();
        request.setGlobalDimCap(2);

        AggIndexResponse aggIndexResponse = indexPlanService.calculateAggIndexCount(request);
        List<AggIndexCombResult> aggIndexCounts = aggIndexResponse.getAggIndexCounts();
        Assert.assertEquals("FAIL", aggIndexCounts.get(0).getStatus());
        Assert.assertEquals("SUCCESS", aggIndexCounts.get(1).getStatus());
        Assert.assertEquals("FAIL", aggIndexResponse.getTotalCount().getStatus());
    }

    @Test
    public void testCalculateAggIndexCountWhenOutOfMaxComb() throws Exception {
        // agg group1 over 4096
        NAggregationGroup aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18 });
        aggregationGroup1.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule1 = new SelectRule();
        selectRule1.setMandatoryDims(new Integer[] {});
        selectRule1.setDimCap(100);
        aggregationGroup1.setSelectRule(selectRule1);

        // agg group2 is normal
        NAggregationGroup aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 5, 6, 7 });
        aggregationGroup2.setMeasures(new Integer[] { 10000, 10001 });
        SelectRule selectRule2 = new SelectRule();
        selectRule2.setMandatoryDims(new Integer[] {});
        aggregationGroup2.setSelectRule(selectRule2);

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .build();
        request.setGlobalDimCap(2);

        AggIndexResponse aggIndexResponse = indexPlanService.calculateAggIndexCount(request);
        List<AggIndexCombResult> aggIndexCounts = aggIndexResponse.getAggIndexCounts();
        Assert.assertEquals("FAIL", aggIndexCounts.get(0).getStatus());
        Assert.assertEquals("SUCCESS", aggIndexCounts.get(1).getStatus());
        Assert.assertEquals("FAIL", aggIndexResponse.getTotalCount().getStatus());
    }

    @Test
    public void testCalculateAggIndexCountWhenDimensionsNotExist() throws Exception {
        NAggregationGroup aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 6, 7, 8, 9, 10, 11, 12, 13 });
        aggregationGroup1.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule1 = new SelectRule();
        selectRule1.setMandatoryDims(new Integer[] {});
        selectRule1.setDimCap(100);
        aggregationGroup1.setSelectRule(selectRule1);

        NAggregationGroup aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 5, 6, 7 });
        aggregationGroup2.setMeasures(new Integer[] { 10000, 10001 });
        SelectRule selectRule2 = new SelectRule();
        selectRule2.setMandatoryDims(new Integer[] {});
        aggregationGroup2.setSelectRule(selectRule2);

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .build();

        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "The following columns are not added as dimensions to the model. Please delete them before saving or add them to the model.\nColumn ID: 10,11,12");
        indexPlanService.calculateAggIndexCount(request);
    }

    @Test
    public void testGetShardByColumns() {
        val project = "default";
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        Segments<NDataSegment> segments = dataflowManager.getDataflow(modelId).getSegments();

        val response = indexPlanService.getShardByColumns("default", modelId);
        Assert.assertTrue(response.isShowLoadData());

        val dfUpdate = new NDataflowUpdate(modelId);
        dfUpdate.setToRemoveSegs(segments.toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(dfUpdate);

        val response2 = indexPlanService.getShardByColumns("default", modelId);
        Assert.assertFalse(response2.isShowLoadData());
    }
}
