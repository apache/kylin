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

import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.exception.code.ErrorCodeServer;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.Range;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.OpenUpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.FusionRuleDataResult;
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
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class FusionIndexServiceTest extends SourceTestCase {

    @InjectMocks
    private FusionIndexService fusionIndexService = Mockito.spy(new FusionIndexService());

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

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        ReflectionTestUtils.setField(fusionIndexService, "indexPlanService", indexPlanService);
        indexPlanService.setSemanticUpater(semanticService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private UpdateRuleBasedCuboidRequest createUpdateRuleRequest(String project, String modelId,
            boolean restoreDelIndex, NAggregationGroup... aggregationGroup) {
        return UpdateRuleBasedCuboidRequest.builder().project(project).modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).isLoadData(false).schedulerVersion(2)
                .restoreDeletedIndex(restoreDelIndex).build();
    }

    private NAggregationGroup mkAggGroup(Integer[] dimensions, Integer[] measures) {
        NAggregationGroup aggregationGroup = mkAggGroup(dimensions);
        aggregationGroup.setMeasures(measures);
        return aggregationGroup;
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
    public void testUpdateRuleWithHybrid() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(Range.HYBRID);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, false, aggregationGroup)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(1, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.HYBRID, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(Range.HYBRID);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, true, aggregationGroup);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(3, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testUpdateRuleWithStreamingAndHybridAggGroups() {
        val fusionId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";

        NAggregationGroup streamingAggGroup = mkAggGroup(new Integer[] { 0 },
                new Integer[] { 100000, 100001, 100002, 100003, 100004 });
        streamingAggGroup.setIndexRange(Range.STREAMING);
        NAggregationGroup fusionAggGroup = mkAggGroup(new Integer[] { 0 },
                new Integer[] { 100000, 100001, 100003, 100004 });
        fusionAggGroup.setIndexRange(Range.HYBRID);
        val request = createUpdateRuleRequest("streaming_test", fusionId, false, streamingAggGroup, fusionAggGroup);
        fusionIndexService.updateRuleBasedCuboid("streaming_test", request);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val indexPlan = indexPlanManager.getIndexPlan(fusionId);
        Assert.assertFalse(indexPlan.isBroken());
        Assert.assertEquals(2, indexPlan.getRuleBasedIndex().getLayoutIdMapping().size());
    }

    @Test
    public void testGetRuleWithHybrid() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.HYBRID, rule.getAggregationGroups().get(0).getIndexRange());
        Assert.assertEquals(true, rule.getIndexUpdateEnabled());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());
        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
        Assert.assertEquals(false, rule1.getIndexUpdateEnabled());
    }

    @Test
    public void testUpdateRuleWithBatch() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(Range.BATCH);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, false, aggregationGroup)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(0, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.BATCH, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(Range.BATCH);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, true, aggregationGroup);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(0, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(3, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testUpdateRuleWithStreaming() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(Range.STREAMING);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, false, aggregationGroup)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(1, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.STREAMING, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(Range.STREAMING);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, true, aggregationGroup);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testGetTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        var response = fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null,
                Lists.newArrayList(Range.STREAMING, Range.HYBRID));
        Assert.assertEquals(5, streamingIndexes.size());

        var batchIndexes = fusionIndexService.getIndexes("streaming_test", batchId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null,
                Lists.newArrayList(Range.BATCH, Range.HYBRID));
        Assert.assertEquals(4, batchIndexes.size());

        batchIndexes = fusionIndexService.getIndexes("streaming_test", batchId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null,
                Lists.newArrayList(Range.HYBRID));
        Assert.assertEquals(3, batchIndexes.size());
    }

    @Test
    public void testHybridTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(modelId);
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
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(0, 11)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(11)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(Range.HYBRID, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID).id(20000060001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, saved.getAllLayouts().size());

        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 1, indexPlan.getAllLayouts().size());

        LayoutEntity newLayout1 = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout1 == null) {
                newLayout1 = layout;
            } else {
                if (newLayout1.getId() < layout.getId()) {
                    newLayout1 = layout;
                }
            }
        }
        Assert.assertThat(newLayout1.getColOrder(), CoreMatchers.is(Arrays.asList(0, 12)));
        Assert.assertThat(newLayout1.getShardByColumns(), CoreMatchers.is(Arrays.asList(12)));
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000070001L, Range.HYBRID);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(5, streamingIndexes.size());
    }

    @Test
    public void testBatchTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(batchId);
        val originLayoutSize = origin.getAllLayouts().size();
        fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 2, indexPlan.getAllLayouts().size());
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
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(0, 11)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(11)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(Range.BATCH, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        var index = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null,
                Lists.newArrayList(20000000001L), null);
        Assert.assertEquals(1, index.size());
        Assert.assertEquals(Range.BATCH, index.get(0).getIndexRange());

        fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.BATCH).id(20000000001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout1 = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout1 == null) {
                newLayout1 = layout;
            } else {
                if (newLayout1.getId() < layout.getId()) {
                    newLayout1 = layout;
                }
            }
        }

        Assert.assertThat(newLayout1.getColOrder(), CoreMatchers.is(Arrays.asList(0, 12)));
        Assert.assertThat(newLayout1.getShardByColumns(), CoreMatchers.is(Arrays.asList(12)));
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, Range.BATCH);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(5, streamingIndexes.size());
    }

    @Test
    public void testStreamingTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(modelId);
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
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(0, 11)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(11)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(Range.STREAMING, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING).id(20000050001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout1 = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout1 == null) {
                newLayout1 = layout;
            } else {
                if (newLayout1.getId() < layout.getId()) {
                    newLayout1 = layout;
                }
            }
        }
        Assert.assertThat(newLayout1.getColOrder(), CoreMatchers.is(Arrays.asList(0, 12)));
        Assert.assertThat(newLayout1.getShardByColumns(), CoreMatchers.is(Arrays.asList(12)));
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000070001L, Range.STREAMING);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(5, streamingIndexes.size());
    }

    @Test
    public void testFusionCalculateAggIndexCount() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val aggGroup1 = mkAggGroup(0, 11);
        aggGroup1.setIndexRange(Range.HYBRID);

        val aggGroup2 = mkAggGroup(12);
        aggGroup2.setIndexRange(Range.BATCH);

        val aggGroup3 = mkAggGroup(13);
        aggGroup3.setIndexRange(Range.STREAMING);

        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Arrays.asList(aggGroup1, aggGroup2, aggGroup3)).build();
        AggIndexResponse response = fusionIndexService.calculateAggIndexCount(request);

        Assert.assertThat(response.getTotalCount().getResult(), is(10L));

        Assert.assertThat(response.getAggIndexCounts().get(0).getResult(), is(6L));
        Assert.assertThat(response.getAggIndexCounts().get(1).getResult(), is(1L));
        Assert.assertThat(response.getAggIndexCounts().get(2).getResult(), is(1L));

        val request1 = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Collections.emptyList()).build();
        AggIndexResponse response1 = fusionIndexService.calculateAggIndexCount(request1);
        Assert.assertEquals(0, response1.getAggIndexCounts().size());
    }

    @Test
    public void testCalculateEmptyAggIndexCount() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val aggGroup1 = mkAggGroup(0, 11);
        aggGroup1.setIndexRange(Range.HYBRID);

        val aggGroup2 = mkAggGroup();
        aggGroup2.setIndexRange(Range.EMPTY);

        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Arrays.asList(aggGroup1, aggGroup2)).build();
        AggIndexResponse response = fusionIndexService.calculateAggIndexCount(request);

        Assert.assertEquals(1, response.getAggIndexCounts().size());
    }

    @Test
    public void testFusionDiffRuleBaseIndex() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        // hybrid +8 index
        val aggGroup1 = mkAggGroup(0, 11, 12);
        aggGroup1.setIndexRange(Range.HYBRID);

        //batch +1 index
        val aggGroup2 = mkAggGroup(12);
        aggGroup2.setIndexRange(Range.BATCH);

        //stream +1 index
        val aggGroup3 = mkAggGroup(13);
        aggGroup3.setIndexRange(Range.STREAMING);

        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Arrays.asList(aggGroup1, aggGroup2, aggGroup3)).build();
        val response = fusionIndexService.calculateDiffRuleBasedIndex(request);

        Assert.assertThat(response.getIncreaseLayouts(), is(10));
        Assert.assertThat(response.getDecreaseLayouts(), is(0));
        Assert.assertThat(response.getRollbackLayouts(), is(0));

        val request1 = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Collections.emptyList()).build();
        val response1 = fusionIndexService.calculateDiffRuleBasedIndex(request1);
        Assert.assertEquals(0, response1.getIncreaseLayouts().shortValue());
        Assert.assertEquals(0, response1.getDecreaseLayouts().shortValue());
    }

    @Test
    public void testStreamingIndexChange() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b73";

        try {
            fusionIndexService.createTableIndex("streaming_test",
                    CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId)
                            .indexRange(Range.STREAMING)
                            .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_LINENUMBER"))
                            .shardByColumns(Arrays.asList("SSB_STREAMING.LO_LINENUMBER")).isLoadData(true)
                            .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.updateTableIndex("streaming_test",
                    CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId)
                            .indexRange(Range.STREAMING).id(20000000001L)
                            .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_CUSTKEY"))
                            .shardByColumns(Arrays.asList("SSB_STREAMING.LO_CUSTKEY")).isLoadData(true)
                            .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, Range.STREAMING);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                    .aggregationGroups(Lists.newArrayList(mkAggGroup(3))).build();
            fusionIndexService.calculateDiffRuleBasedIndex(request);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testFusionModelWithBatchIndexChange() throws Exception {
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        val batchId = "cd2b9a23-699c-4699-b0dd-38c9412b3dfd";

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        NDataflow df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        var saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, saved.getAllLayouts().size());

        fusionIndexService.createTableIndex("streaming_test",
                CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                        .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_CUSTKEY"))
                        .shardByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).isLoadData(true)
                        .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(5, saved.getAllLayouts().size());

        fusionIndexService.updateTableIndex("streaming_test",
                CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                        .id(20000000001L)
                        .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_CUSTKEY"))
                        .shardByColumns(Arrays.asList("SSB_STREAMING.LO_CUSTKEY")).isLoadData(true)
                        .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, Range.BATCH);

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, saved.getAllLayouts().size());

        NAggregationGroup aggregationGroup = mkAggGroup(3);
        aggregationGroup.setIndexRange(Range.BATCH);
        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).build();
        val response = fusionIndexService.calculateDiffRuleBasedIndex(request);
        Assert.assertThat(response.getIncreaseLayouts(), is(1));
    }

    @Test
    public void testFusionModelWithStreamingIndexChange() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        NDataflow df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        try {
            fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING).id(20000050001L)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.removeIndex("streaming_test", modelId, 20000050001L, Range.STREAMING);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        NAggregationGroup aggregationGroup = mkAggGroup(3);
        aggregationGroup.setIndexRange(Range.STREAMING);
        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).build();
        try {
            fusionIndexService.calculateDiffRuleBasedIndex(request);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testFusionModelWithHybridIndexChange() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        NDataflow df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        try {
            fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID).id(20000040001L)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.removeIndex("streaming_test", modelId, 20000040001L, Range.HYBRID);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        NAggregationGroup aggregationGroup = mkAggGroup(3);
        aggregationGroup.setIndexRange(Range.HYBRID);
        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).build();
        try {
            fusionIndexService.calculateDiffRuleBasedIndex(request);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testRemoveIndexes() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(5, indexPlan.getAllIndexes().size());

        try {
            fusionIndexService.removeIndexes("streaming_test", modelId, indexPlan.getAllLayoutIds(false));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        val batchId = "cd2b9a23-699c-4699-b0dd-38c9412b3dfd";
        val batchIndexPlan = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, batchIndexPlan.getAllLayouts().size());
        fusionIndexService.removeIndexes("streaming_test", batchId, batchIndexPlan.getAllLayoutIds(false));

        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getAllLayouts().size());
    }

    @Test
    public void testBatchRemoveIndex() {
        val project = "streaming_test";
        val model = "4965c827-fbb4-4ea1-a744-3f341a3b030d";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(model);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());
        try {
            fusionIndexService.batchRemoveIndex("streaming_test", model,
                    indexPlanManager.getIndexPlan(model).getAllLayoutIds(false), Range.STREAMING);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        }

        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        Assert.assertEquals(5, indexPlanManager.getIndexPlan(modelId).getAllLayouts().size());
        fusionIndexService.batchRemoveIndex(project, modelId, Sets.newHashSet(20000040001L), Range.STREAMING);
        FusionModel fusionModel = KylinConfig.getInstanceFromEnv()
                .getManager("streaming_test", FusionModelManager.class).getFusionModel(modelId);
        String batchId = fusionModel.getBatchModel().getUuid();
        Assert.assertEquals(4, indexPlanManager.getIndexPlan(modelId).getAllLayouts().size());

        Assert.assertEquals(3, indexPlanManager.getIndexPlan(batchId).getAllLayouts().size());
        fusionIndexService.batchRemoveIndex(project, modelId, Sets.newHashSet(10001L), Range.BATCH);
        Assert.assertEquals(4, indexPlanManager.getIndexPlan(modelId).getAllLayouts().size());
        Assert.assertEquals(2, indexPlanManager.getIndexPlan(batchId).getAllLayouts().size());

        fusionIndexService.batchRemoveIndex(project, batchId, Sets.newHashSet(1L), Range.BATCH);
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getAllLayouts().size());

        Assert.assertTrue(
                indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream().anyMatch(e -> e.getId() == 20001L));
        Assert.assertTrue(
                indexPlanManager.getIndexPlan(batchId).getAllLayouts().stream().anyMatch(e -> e.getId() == 20001L));
        fusionIndexService.batchRemoveIndex(project, modelId, Sets.newHashSet(20001L), Range.HYBRID);
        Assert.assertFalse(
                indexPlanManager.getIndexPlan(modelId).getAllLayouts().stream().anyMatch(e -> e.getId() == 20001L));
        Assert.assertFalse(
                indexPlanManager.getIndexPlan(batchId).getAllLayouts().stream().anyMatch(e -> e.getId() == 20001L));
    }

    @Test
    public void testRemoveIndexe() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        fusionIndexService.removeIndex("streaming_test", modelId, 20000040001L, Range.HYBRID);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val saved = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(4, saved.getAllLayouts().size());
    }

    @Test
    public void testCheckStreamingJobAndSegments() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        val indexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        val indexUpdateEnabled = FusionIndexService.checkUpdateIndexEnabled("streaming_test", modelId);
        val result = FusionRuleDataResult.get(indexes, 20, 10, indexUpdateEnabled);
        Assert.assertEquals(indexUpdateEnabled, result.isIndexUpdateEnabled());

        val nullResult = FusionRuleDataResult.get(null, 20, 10, indexUpdateEnabled);
        Assert.assertEquals(indexUpdateEnabled, nullResult.isIndexUpdateEnabled());
        Assert.assertEquals(0, nullResult.getTotalSize());
    }

    @Test
    public void testGetAllIndex() throws Exception {
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        var indexResponses = fusionIndexService.getAllIndexes("streaming_test", modelId, "", Lists.newArrayList(),
                "data_size", true, Lists.newArrayList());
        Assert.assertEquals(8, indexResponses.size());

        val modelId1 = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        var indexResponses1 = fusionIndexService.getAllIndexes("streaming_test", modelId1, "", Lists.newArrayList(),
                "data_size", true, Lists.newArrayList());
        Assert.assertEquals(8, indexResponses1.size());

        val modelId2 = "e78a89dd-847f-4574-8afa-8768b4228b72";
        var indexResponses2 = fusionIndexService.getAllIndexes("streaming_test", modelId2, "", Lists.newArrayList(),
                "data_size", true, Lists.newArrayList());
        Assert.assertEquals(64, indexResponses2.size());
    }

    private Integer[] toIntegerArray(Map<String, Integer> dimMap, String[] dimensions) {
        return Arrays.stream(dimensions).map(dimMap::get).toArray(Integer[]::new);
    }

    private void testDimsOrder(OpenUpdateRuleBasedCuboidRequest open, UpdateRuleBasedCuboidRequest internal,
            NDataModel model) {
        List<OpenUpdateRuleBasedCuboidRequest.OpenAggGroupRequest> openGroups = open.getAggregationGroups();
        int from = internal.getAggregationGroups().size() - openGroups.size();
        int to = internal.getAggregationGroups().size();
        List<NAggregationGroup> internalGroups = internal.getAggregationGroups().subList(from, to);
        val dimMap = model.getEffectiveDimensions().entrySet().stream().collect(Collectors
                .toMap(e -> e.getValue().getAliasDotName(), Map.Entry::getKey, (u, v) -> v, LinkedHashMap::new));
        for (int i = 0; i < openGroups.size(); i++) {
            val openGroup = openGroups.get(i);
            val internalGroup = internalGroups.get(i);
            Assert.assertArrayEquals(toIntegerArray(dimMap, openGroup.getDimensions()), internalGroup.getIncludes());
            if (openGroup.getMandatoryDims() != null) {
                Assert.assertArrayEquals(toIntegerArray(dimMap, openGroup.getMandatoryDims()),
                        internalGroup.getSelectRule().getMandatoryDims());
            }
            if (!ArrayUtils.isEmpty(openGroup.getJointDims())) {
                for (int j = 0; j < openGroup.getJointDims().length; j++) {
                    if (!ArrayUtils.isEmpty(openGroup.getJointDims()[j])) {
                        Assert.assertArrayEquals(toIntegerArray(dimMap, openGroup.getJointDims()[j]),
                                internalGroup.getSelectRule().getJointDims()[j]);
                    }
                }
            }
            if (!ArrayUtils.isEmpty(openGroup.getHierarchyDims())) {
                for (int j = 0; j < openGroup.getHierarchyDims().length; j++) {
                    if (!ArrayUtils.isEmpty(openGroup.getHierarchyDims()[j])) {
                        Assert.assertArrayEquals(toIntegerArray(dimMap, openGroup.getHierarchyDims()[j]),
                                internalGroup.getSelectRule().getHierarchyDims()[j]);
                    }
                }
            }
        }
    }

    @Test
    public void testNormalConvertOpenToInternal() {
        OpenUpdateRuleBasedCuboidRequest request = new OpenUpdateRuleBasedCuboidRequest();
        request.setProject("default");
        request.setModelAlias("test_bank");
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        NDataModel model = modelManager.getDataModelDescByAlias(request.getModelAlias());
        val aggGroup = new OpenUpdateRuleBasedCuboidRequest.OpenAggGroupRequest();
        request.setAggregationGroups(Collections.singletonList(aggGroup));

        {
            aggGroup.setDimensions(new String[]{"TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.DT"});
            aggGroup.setMeasures(new String[]{"SUM_INCOME"});
            String[][] jointDims = new String[][]{{"TEST_BANK_INCOME.DT"}, {}};
            aggGroup.setJointDims(jointDims);
            UpdateRuleBasedCuboidRequest internal = fusionIndexService.convertOpenToInternal(request, model);
            Assert.assertEquals(2, internal.getAggregationGroups().size());
            testDimsOrder(request, internal, model);
        }

        {
            aggGroup.setDimensions(new String[]{"TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.DT"});
            aggGroup.setMeasures(null);
            String[][] hierarchyDims = { { "TEST_BANK_INCOME.NAME" } };
            aggGroup.setHierarchyDims(hierarchyDims);
            String[][] jointDims = new String[][]{{"TEST_BANK_INCOME.DT"}, {}};
            aggGroup.setJointDims(jointDims);
            UpdateRuleBasedCuboidRequest internal = fusionIndexService.convertOpenToInternal(request, model);
            Assert.assertEquals(2, internal.getAggregationGroups().size());
            testDimsOrder(request, internal, model);
        }

        {
            aggGroup.setDimensions(new String[]{"TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.DT"});
            aggGroup.setMeasures(new String[]{"SUM_INCOME"});
            aggGroup.setHierarchyDims(null);
            aggGroup.setJointDims(new String[][]{});
            aggGroup.setMandatoryDims(new String[] {"TEST_BANK_INCOME.NAME"});
            UpdateRuleBasedCuboidRequest internal = fusionIndexService.convertOpenToInternal(request, model);
            Assert.assertEquals(2, internal.getAggregationGroups().size());
            testDimsOrder(request, internal, model);
        }
    }

    @Test
    public void testIllegalConvertOpenToInternal() {
        OpenUpdateRuleBasedCuboidRequest request = new OpenUpdateRuleBasedCuboidRequest();
        request.setProject("default");
        request.setModelAlias("test_bank");
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        NDataModel model = modelManager.getDataModelDescByAlias(request.getModelAlias());
        val aggGroup = new OpenUpdateRuleBasedCuboidRequest.OpenAggGroupRequest();
        request.setAggregationGroups(Collections.singletonList(aggGroup));

        request.setGlobalDimCap(-1);
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.INTEGER_POSITIVE_CHECK.getMsg(), e.getMessage());
        }

        checkDimAndMeas(aggGroup, request, model);

        checkSelectRuleDims(aggGroup, request, model);
    }

    private void checkDimAndMeas(OpenUpdateRuleBasedCuboidRequest.OpenAggGroupRequest aggGroup,
            OpenUpdateRuleBasedCuboidRequest request, NDataModel model) {
        request.setGlobalDimCap(null);
        aggGroup.setDimensions(null);
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (NullPointerException e) {
            Assert.assertEquals("dimension should not null", e.getMessage());
        }

        aggGroup.setDimCap(1);
        aggGroup.setDimensions(new String[] { "TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.NAME" });
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(
                    e.getMessage().startsWith("Dimension or measure in agg group must not contain duplication"));
        }

        aggGroup.setDimensions(new String[] { "TEST_KYLIN_FACT.LSTG_SITE_ID1", "TEST_KYLIN_FACT.LSTG_SITE_ID" });
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.DIMENSION_NOT_IN_MODEL.getMsg(), e.getMessage());
        }

        aggGroup.setDimensions(new String[] { "TEST_BANK_INCOME.NAME", "TEST_BANK_INCOME.DT" });
        aggGroup.setMeasures(new String[] { "TRANS_CNT1" });
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.MEASURE_NOT_IN_MODEL.getMsg(), e.getMessage());
        }
    }

    private void checkSelectRuleDims(OpenUpdateRuleBasedCuboidRequest.OpenAggGroupRequest aggGroup,
            OpenUpdateRuleBasedCuboidRequest request, NDataModel model) {
        aggGroup.setMeasures(new String[] { "SUM_INCOME" });
        aggGroup.setMandatoryDims(new String[] { "TEST_KYLIN_FACT.LSTG_FORMAT_NAME" });
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.MANDATORY_NOT_IN_DIMENSION.getMsg(), e.getMessage());
        }

        aggGroup.setMandatoryDims(new String[] {});
        String[][] hierarchyDims = { { "TEST_KYLIN_FACT.LSTG_FORMAT_NAME" } };
        aggGroup.setHierarchyDims(hierarchyDims);
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.HIERARCHY_NOT_IN_DIMENSION.getMsg(), e.getMessage());
        }

        aggGroup.setMeasures(null);
        aggGroup.setHierarchyDims(null);
        String[][] jointDims = { { "TEST_KYLIN_FACT.LSTG_FORMAT_NAME" }, {} };
        aggGroup.setJointDims(jointDims);
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.JOINT_NOT_IN_DIMENSION.getMsg(), e.getMessage());
        }

        hierarchyDims = new String[][] { { "TEST_BANK_INCOME.NAME" } };
        jointDims = new String[][] { { "TEST_BANK_INCOME.NAME" } };
        aggGroup.setHierarchyDims(hierarchyDims);
        aggGroup.setJointDims(jointDims);
        try {
            fusionIndexService.convertOpenToInternal(request, model);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(ErrorCodeServer.DIMENSION_ONLY_SET_ONCE.getMsg(), e.getMessage());
        }
    }
}
