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
package org.apache.kylin.metadata.cube.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.CubeTestUtils;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleBasedCuboidDescTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testGenCuboids() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        val oldRule = newPlan.getRuleBasedIndex();
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");

        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1,3,4,5,6],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [1],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [3,5],\n" //
                        + "            [4,6]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" //
                        + "        \"includes\": [1,2,3,4,5],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [[2,3,4]],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [1,5]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                log.error("Something wrong happened when update this IndexPlan.", e);
            }
        });
        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(12, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.<List<Integer>> newArrayList(Lists.newArrayList(1),
                Lists.newArrayList(1, 3, 5), Lists.newArrayList(1, 4, 6), Lists.newArrayList(1, 3, 4, 5, 6)));
        Assert.assertThat(indexPlan.getRuleBasedIndex().getLayoutIdMapping(), CoreMatchers.is(Arrays.asList(10001L,
                120001L, 30001L, 40001L, 80001L, 130001L, 140001L, 150001L, 160001L, 170001L, 180001L, 190001L)));
    }

    @Test
    public void testCorrectnessOfGenRuleBasedIndexes() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);
        indexPlanManager.createIndexPlan(newPlan);
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Lists.newArrayList(2, 1, 3));
            try {
                val group = JsonUtil.readValue("{ \"includes\": [2, 1, 3], "
                        + "\"select_rule\": { \"hierarchy_dims\": [], \"mandatory_dims\": [2], "
                        + "\"joint_dims\": [ [1,3] ] } }", NAggregationGroup.class);
                newRule.setAggregationGroups(Lists.newArrayList(group));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (Exception e) {
                log.error("Something wrong happened when update this indexPlan.", e);
            }
        });
        logLayouts(indexPlan.getAllLayouts());
        List<IndexEntity> allIndexes = indexPlan.getAllIndexes();
        Assert.assertEquals(2, allIndexes.size());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());
        Assert.assertEquals(Lists.newArrayList(2), allIndexes.get(0).getDimensions());
        Assert.assertEquals("{2}", allIndexes.get(0).getDimensionBitset().toString());
        IndexEntity entity0 = allIndexes.get(0);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008, 100009,
                100010, 100011, 100012, 100013, 100014, 100015, 100016, 100017), entity0.getMeasures());
        Assert.assertEquals(
                Lists.newArrayList(2, 100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008, 100009, 100010,
                        100011, 100012, 100013, 100014, 100015, 100016, 100017),
                entity0.getLayouts().get(0).getColOrder());
        Assert.assertEquals(Lists.newArrayList(2, 1, 3), allIndexes.get(1).getDimensions());
        Assert.assertEquals("{1, 2, 3}", allIndexes.get(1).getDimensionBitset().toString());
        IndexEntity entity1 = allIndexes.get(1);
        Assert.assertEquals(allIndexes.get(0).getMeasures(), entity1.getMeasures());
        Assert.assertEquals(Lists.newArrayList(2, 1, 3, 100000, 100001, 100002, 100003, 100004, 100005, 100007, //
                100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016, 100017),
                entity1.getLayouts().get(0).getColOrder());
    }

    @Test
    public void testGenTooManyCuboids() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/enormous_rule_based_cube.json"),
                IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        try {
            indexPlanManager.createIndexPlan(newPlan);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("The number of indexes generated by the aggregate group exceeds the maximum "
                    + "number(40960) of indexes allowed by the system.", e.getMessage());
        }
    }

    @Test
    public void testGenTooManyCuboidsWithMaxDimCompIsOne() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/enormous_rule_based_cube.json"),
                IndexPlan.class);
        newPlan.setLastModified(0L);
        List<NAggregationGroup> aggregationGroups = newPlan.getRuleBasedIndex().getAggregationGroups();
        Assert.assertEquals(1, aggregationGroups.size());
        aggregationGroups.get(0).getSelectRule().setDimCap(1);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        IndexPlan indexPlan = indexPlanManager.createIndexPlan(newPlan);
        List<LayoutEntity> allLayouts = indexPlan.getAllLayouts();
        Assert.assertEquals(34, allLayouts.size());
    }

    @Test
    public void testGenTooManyCuboidsWithScheduleVersion2() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/enormous_rule_based_cube.json"),
                IndexPlan.class);
        newPlan.setLastModified(0L);
        newPlan.getRuleBasedIndex().setSchedulerVersion(2);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        try {
            indexPlanManager.createIndexPlan(newPlan);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "The number of indexes generated by the aggregate group exceeds the maximum number(4096) of indexes allowed by the system.",
                    e.getMessage());
        }
    }

    @Test
    public void testGenTooManyCuboidsWithScheduleV2AndMaxDimCompIsOne() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/enormous_rule_based_cube.json"),
                IndexPlan.class);
        newPlan.setLastModified(0L);
        newPlan.getRuleBasedIndex().setSchedulerVersion(2);
        List<NAggregationGroup> aggregationGroups = newPlan.getRuleBasedIndex().getAggregationGroups();
        Assert.assertEquals(1, aggregationGroups.size());
        aggregationGroups.get(0).getSelectRule().setDimCap(1);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        IndexPlan indexPlan = indexPlanManager.createIndexPlan(newPlan);
        List<LayoutEntity> allLayouts = indexPlan.getAllLayouts();
        Assert.assertEquals(34, allLayouts.size());
    }

    @Test
    public void testGenCuboidsWithAuto() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_mixed.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        val oldRule = newPlan.getRuleBasedIndex();
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(14, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");

        val indexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1,3,4,5,6],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [1],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [3,5],\n" //
                        + "            [4,6]\n" //
                        + "          ]\n" //
                        + "        }\n" //
                        + "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" //
                        + "      {\n" //
                        + "        \"includes\": [1,2,3,4,5],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [[2,3,4]],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [1,5]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(14, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.<List<Integer>> newArrayList(Lists.newArrayList(1, 3, 4, 5, 6),
                Lists.newArrayList(1), Lists.newArrayList(1, 3, 5), Lists.newArrayList(1, 4, 6)));
        Assert.assertThat(indexPlan.getRuleBasedIndex().getLayoutIdMapping(), CoreMatchers.is(Arrays.asList(130001L,
                240001L, 150001L, 160001L, 200001L, 250001L, 260001L, 270001L, 280001L, 290001L, 300001L, 310001L)));
        val actualDims = indexPlan.getRuleBaseLayouts().stream()
                .map(layout -> layout.getOrderedDimensions().keySet().asList()).collect(Collectors.toSet());
        val expectedDims = Lists.<List> newArrayList(Lists.newArrayList(1, 2, 5), Lists.newArrayList(2, 3, 4),
                Lists.newArrayList(1, 3, 5), Lists.newArrayList(1, 2, 3, 5), Lists.newArrayList(1, 3, 4, 5, 6),
                Lists.newArrayList(1, 4, 6), Lists.newArrayList(1), Lists.newArrayList(1, 5),
                Lists.newArrayList(1, 2, 3, 4, 5), Lists.newArrayList(2, 3), Lists.newArrayList(2),
                Lists.newArrayList(1, 2, 3, 4, 5, 6));
        Assert.assertEquals(actualDims.size(), expectedDims.size());
        Assert.assertTrue(actualDims.containsAll(expectedDims));
    }

    @Test
    public void testGenCuboidsPartialEqual() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        val oldRule = newPlan.getRuleBasedIndex();
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");

        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1,3,4,5,6],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [3],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [1,5],\n" //
                        + "            [4,6]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" //
                        + "        \"includes\": [0,1,2,3,4,5],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [[0,1,2]],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [3,4]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(20, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan,
                Lists.<List<Integer>> newArrayList(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6),
                        Lists.newArrayList(1, 3, 4, 5, 6), Lists.newArrayList(0, 1, 2, 3, 4),
                        Lists.newArrayList(0, 3, 4), Lists.newArrayList(0, 1), Lists.newArrayList(0, 1, 3, 4),
                        Lists.newArrayList(1, 3, 5), Lists.newArrayList(3, 4), Lists.newArrayList(0, 1, 2),
                        Lists.newArrayList(0)));
    }

    @Test
    public void testDiffRuleBasedIndex() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        IndexPlan indexPlan = indexPlanManager.createIndexPlan(newPlan);
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
        val group1 = JsonUtil.readValue(
                "" + "{\n" + "        \"includes\": [1, 3, 4, 5, 6],\n" + "        \"measures\": [100001, 100002],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [],\n"
                        + "          \"mandatory_dims\": [3],\n" + "          \"joint_dims\": [\n"
                        + "            [1, 5],\n" + "            [4 ,6]\n" + "          ]\n" + "        }\n" + "}",
                NAggregationGroup.class);
        val group2 = JsonUtil.readValue("" + "      {\n" + "        \"includes\": [0, 1, 2, 3, 4, 5],\n"
                + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [[0, 1, 2]],\n"
                + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [\n" + "            [3 ,4]\n"
                + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Arrays.asList(group1, group2));

        val result = indexPlan.diffRuleBasedIndex(newRule);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getDecreaseLayouts())
                && CollectionUtils.isNotEmpty(result.getIncreaseLayouts()));
        Assert.assertTrue(result.getDecreaseLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet())
                .contains(30001L));
        Assert.assertTrue(result.getIncreaseLayouts().stream()
                .anyMatch(layoutEntity -> layoutEntity.getOrderedMeasures().containsKey(100001)
                        && layoutEntity.getOrderedMeasures().containsKey(100002)));
    }

    @Test
    public void testSetRuleBasedIndex() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");
        NDataflow df = dataflowManager.getDataflow(newPlan.getId());

        val seg1 = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val update1 = new NDataflowUpdate(df.getUuid());
        seg1.setStatus(SegmentStatusEnum.READY);
        update1.setToUpdateSegs(seg1);
        update1.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df, seg1.getId(), 30001L));
        dataflowManager.updateDataflow(update1);

        val newRule = new RuleBasedIndex();
        newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
        val group1 = JsonUtil.readValue("" //
                + "{\n" //
                + "        \"includes\": [1, 3, 4, 5, 6],\n" //
                + "        \"measures\": [100001, 100002],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [],\n" //
                + "          \"mandatory_dims\": [3],\n" //
                + "          \"joint_dims\": [\n" //
                + "            [1, 5],\n" //
                + "            [4 ,6]\n" //
                + "          ]\n" //
                + "        }\n" + "}", NAggregationGroup.class);
        val group2 = JsonUtil.readValue("" + "      {\n" //
                + "        \"includes\": [0, 1, 2, 3, 4, 5],\n" //
                + "        \"measures\": [\n" //
                + "      100001,\n" //
                + "      100002,\n" //
                + "      100003\n" //
                + "    ],\n" //
                + "        \"select_rule\": {\n" //
                + "          \"hierarchy_dims\": [[0, 1, 2]],\n" //
                + "          \"mandatory_dims\": [],\n" //
                + "          \"joint_dims\": [\n" // //
                + "            [3 ,4]\n" //
                + "          ]\n" //
                + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Arrays.asList(group1, group2));

        val newIndexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(newRule, false, true);
        });

        Assert.assertTrue(CollectionUtils.isNotEmpty(newIndexPlan.getToBeDeletedIndexes()));
        Assert.assertTrue(
                newIndexPlan.getToBeDeletedIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == 30000L));

        val onlyDeleteRule = new RuleBasedIndex();
        onlyDeleteRule.setDimensions(Arrays.asList(1, 3, 4, 5, 6));
        onlyDeleteRule
                .setAggregationGroups(Lists.newArrayList(JsonUtil.deepCopyQuietly(group1, NAggregationGroup.class)));
        val onlyDeleteIndexPlan = indexPlanManager.updateIndexPlan(newIndexPlan.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(30001L), true, true);
            copyForWrite.setRuleBasedIndex(onlyDeleteRule, false, true);
        });

        Assert.assertTrue(CollectionUtils.isEmpty(onlyDeleteIndexPlan.getToBeDeletedIndexes()));
    }

    @Test
    public void testSetRuleAgain() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");
        NDataflow df = dataflowManager.getDataflow(newPlan.getId());

        val seg1 = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val update1 = new NDataflowUpdate(df.getUuid());
        seg1.setStatus(SegmentStatusEnum.READY);
        update1.setToUpdateSegs(seg1);
        update1.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df, seg1.getId(), 30001L));
        dataflowManager.updateDataflow(update1);

        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1,3,4,5,6],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [3],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [1,5],\n" //
                        + "            [4,6]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" //
                        + "        \"includes\": [0,1,2,3,4,5],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [[0,1,2]],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [3,4]\n" //
                        + "          ]\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule, false, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        val copy = indexPlan.copy();
        copy.setRuleBasedIndex(copy.getRuleBasedIndex(), true);
        Assert.assertEquals(JsonUtil.writeValueAsIndentString(indexPlan), JsonUtil.writeValueAsIndentString(copy));
        Assert.assertTrue(CollectionUtils.isNotEmpty(indexPlan.getToBeDeletedIndexes()));
    }

    @Test
    public void testAddBlackListLayout() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());
        RuleBasedIndex oldRuleBasedIndex = newPlan.getRuleBasedIndex();
        val indexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            copyForWrite.addRuleBasedBlackList(oldRuleBasedIndex.getLayoutIdMapping().subList(0, 2));
        });

        Assert.assertEquals(2, indexPlan.getRuleBasedIndex().getLayoutBlackList().size());
        Assert.assertEquals(indexPlan.getAllLayouts().size() + 2, newPlan.getAllLayouts().size());
        Set<Long> originalPlanLayoutIds = newPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        Set<Long> newPlanLayoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        originalPlanLayoutIds.removeAll(newPlanLayoutIds);
        Assert.assertTrue(CollectionUtils.isEqualCollection(originalPlanLayoutIds,
                indexPlan.getRuleBasedIndex().getLayoutBlackList()));

        var newPlan2 = JsonUtil.readValue(getClass().getResourceAsStream("/rule_based_with_multi_order.json"),
                IndexPlan.class);
        newPlan2.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan2);
        newPlan2 = indexPlanManager.createIndexPlan(newPlan2);
        logLayouts(newPlan2.getAllLayouts());
        Assert.assertEquals(8, newPlan2.getAllLayouts().size());
        IndexPlan finalNewPlan = newPlan2;
        Assert.assertTrue(newPlan2.getAllLayouts().stream()
                .noneMatch(l -> finalNewPlan.getRuleBasedIndex().getLayoutBlackList().contains(l.getId())));
    }

    @Test
    public void testGenCuboidsOfNewSortingSet() throws IOException {
        val indexPlanManager = getIndexPlanManager();
        var indexPlan = getTmpTestIndexPlan("/ncube_rule_different_measure_2.json");
        val id = indexPlan.getId();
        indexPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), indexPlan);
        indexPlan = indexPlanManager.createIndexPlan(indexPlan);

        int i = 10;
        while (i >= 0) {
            indexPlan.initAfterReload(getTestConfig(), "default");
            assertCuboidIdMapping(id, 1, Lists.newArrayList(0, 100000));
            assertCuboidIdMapping(id, 10001, Lists.newArrayList(0, 100001));
            assertCuboidIdMapping(id, 20001, Lists.newArrayList(0, 100002));
            assertCuboidIdMapping(id, 30001, Lists.newArrayList(0, 100003));
            assertCuboidIdMapping(id, 40001, Lists.newArrayList(0, 100004));
            assertCuboidIdMapping(id, 50001, Lists.newArrayList(0, 100005));
            assertCuboidIdMapping(id, 60001, Lists.newArrayList(0, 100007));
            assertCuboidIdMapping(id, 70001, Lists.newArrayList(0, 100008));
            assertCuboidIdMapping(id, 80001, Lists.newArrayList(0, 100009));
            assertCuboidIdMapping(id, 90001, Lists.newArrayList(0, 100010));
            assertCuboidIdMapping(id, 100001, Lists.newArrayList(0, 100011));
            assertCuboidIdMapping(id, 110001, Lists.newArrayList(0, 100012));
            assertCuboidIdMapping(id, 120001, Lists.newArrayList(0, 100013));
            assertCuboidIdMapping(id, 130001, Lists.newArrayList(0, 100014));
            assertCuboidIdMapping(id, 140001, Lists.newArrayList(0, 100015));
            assertCuboidIdMapping(id, 150001, Lists.newArrayList(0, 100016));
            assertCuboidIdMapping(id, 160001, Lists.newArrayList(0, 100000, 100001, 100002, 100003, 100004, 100005,
                    100007, 100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016));
            i--;
        }
    }

    private void assertCuboidIdMapping(String indexPlanId, long layoutId, List<Integer> expectedColOrder) {
        val indexPlan = getIndexPlanManager().getIndexPlan(indexPlanId);
        val layoutEntity = indexPlan.getLayoutEntity(layoutId);
        val actualColOrder = layoutEntity.getColOrder();
        Assert.assertArrayEquals(actualColOrder.toArray(), expectedColOrder.toArray());
    }

    @Test
    public void testGenCuboidsForDifferentAggMeasures() throws IOException {
        val indexPlanManager = getIndexPlanManager();
        var indexPlan = getTmpTestIndexPlan("/ncube_rule_different_measure.json");
        indexPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), indexPlan);

        indexPlan = indexPlanManager.createIndexPlan(indexPlan);

        val colOrders = indexPlan.getAllLayouts().stream()
                .map(layout -> Lists.<Integer> newArrayList(layout.getColOrder())).collect(Collectors.toSet());

        testAgg1(colOrders);
        testAgg2(colOrders);
        testAgg3(colOrders);
        testAgg4(colOrders);
        testAgg5(colOrders);
        testAgg6(colOrders);

        // intersection case: (d1, d2, d3, m1), (d1, d2, d3, m2, m3)
        // (0, 1, 2, 3, 6) grows on agg 1, agg 2, agg 3
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100011, 100012, 100013, 100014)));
        // (16, 19, 20) grows on agg 4, agg 5, agg 6
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100013, 100014, 100015, 100016)));

        // base cuboid
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16,
                17, 18, 19, 20, 100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008, 100009, 100010, 100011,
                100012, 100013, 100014, 100015, 100016)));

    }

    private void testAgg1(Set<ArrayList<Integer>> colOrders) {
        // agg 1 contains
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 15, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 4, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 4, 5, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 7, 8, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 9, 13, 14, 100000, 100001, 100002, 100003)));

        // agg 1 not contains
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 5, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(4, 5, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(7, 8, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 7, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 13, 14, 100000, 100001, 100002, 100003)));

    }

    private void testAgg2(Set<ArrayList<Integer>> colOrders) {
        // agg 2 contains
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 14, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 15, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 6, 14, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 4, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 4, 6, 15, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 4, 5, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 9, 13, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 4, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 4, 5, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 4, 5, 6, 9, 13, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders.contains(
                Lists.<Integer> newArrayList(0, 4, 5, 6, 7, 8, 9, 13, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 100000,
                100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 100000,
                100001, 100004, 100005, 100016)));

        // agg 2 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(1, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(2, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 9, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 3, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 3, 4, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 5, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 5, 6, 9, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 9, 100000, 100001, 100004, 100005, 100016)));

    }

    private void testAgg3(Set<ArrayList<Integer>> colOrders) {
        // agg 3 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 8, 9, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 13, 14, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 8, 9, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 13, 14, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders.contains(
                Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders.contains(
                Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15,
                100011, 100012, 100013, 100014)));

        // agg 3 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(0, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 5, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 7, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 8, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 9, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 13, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 14, 100011, 100012, 100013, 100014)));

    }

    private void testAgg4(Set<ArrayList<Integer>> colOrders) {
        // agg 4 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 20, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(19, 20, 100005, 100007, 100008)));

        // agg 4 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(19, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 19, 20, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 19, 20, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 20, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 100005, 100007, 100008)));

    }

    private void testAgg5(Set<ArrayList<Integer>> colOrders) {
        // agg 5 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 20, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 20, 100000, 100001, 100002)));

        // agg 5 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(19, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 20, 100000, 100001, 100002)));

    }

    private void testAgg6(Set<ArrayList<Integer>> colOrders) {
        // agg 6 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 20, 100013, 100014, 100015, 100016)));

        // agg 6 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 18, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 18, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 19, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 19, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(18, 19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(17, 18, 19, 100013, 100014, 100015, 100016)));

    }

    @Test
    public void testGenCuboidWithoutBaseCuboid() throws IOException {
        getTestConfig().setProperty("kylin.cube.aggrgroup.is-base-cuboid-always-valid", "false");
        val indexPlanManager = getIndexPlanManager();
        var newPlan = getTmpTestIndexPlan("/ncude_rule_based.json");

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        Assert.assertEquals(11, newPlan.getAllLayouts().size());
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1,3,4,5,6],\n" //
                        + "        \"measures\": [100000, 100001],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [1],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [3,5],\n" //
                        + "            [4,6]\n" //
                        + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" //
                        + "        \"includes\": [1,2,3,4,5],\n" //
                        + "        \"measures\": [100002, 100003],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [[2,3,4]],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [1,5]\n" //
                        + "          ]\n" //
                        + "        }\n"

                        + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                log.error("Something wrong happened when update this IndexPlan.", e);
            }
        });

        val colOrders = indexPlan.getAllLayouts().stream().map(layout -> layout.getColOrder())
                .collect(Collectors.toSet());

        // does not contain base cuboid
        Assert.assertFalse(colOrders
                .contains(Lists.<Integer> newArrayList(1, 2, 3, 4, 5, 6, 100000, 100001, 100002, 100003, 100004, 100005,
                        100007, 100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016)));
    }

    @Test
    public void testGenCuboidWithUpdateBaseCuboid() throws IOException {
        val indexPlanManager = getIndexPlanManager();
        var newPlan = getTmpTestIndexPlan("/ncude_rule_based.json");
        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan.getRuleBasedIndex().setSchedulerVersion(2);
        newPlan = indexPlanManager.createIndexPlan(newPlan);
        RuleBasedIndex ruleBasedIndex = newPlan.getRuleBasedIndex();
        Assert.assertEquals(2, ruleBasedIndex.getSchedulerVersion());
        Assert.assertTrue(ruleBasedIndex.getBaseLayoutEnabled());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());
        IndexPlan indexPlan = updateBaseCuboid("false");
        ruleBasedIndex = indexPlan.getRuleBasedIndex();
        Assert.assertEquals(2, ruleBasedIndex.getSchedulerVersion());
        Assert.assertTrue(ruleBasedIndex.getBaseLayoutEnabled());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());
        Assert.assertEquals(150000, indexPlan.getNextAggregationIndexId());
        indexPlan = updateBaseCuboid("true");
        Assert.assertEquals(3, indexPlan.getAllLayouts().size());
        Assert.assertEquals(150000, indexPlan.getNextAggregationIndexId());
    }

    private IndexPlan updateBaseCuboid(String baseCuboid) {
        val indexPlanManager = getIndexPlanManager();
        getTestConfig().setProperty("kylin.cube.aggrgroup.is-base-cuboid-always-valid", baseCuboid);
        return indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setSchedulerVersion(2);
            newRule.setBaseLayoutEnabled(true);
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [3,1],\n" //
                        + "        \"measures\": [100000, 100001],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": [], \n" //
                        + "          \"dim_cap\": 1 }\n" //
                        + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                log.error("Something wrong happened when update this IndexPlan.", e);
            }
        });
    }

    @Test
    public void testCalculateDimSortedList() throws Exception {

        NAggregationGroup aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 5, 18 });
        aggregationGroup1.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule1 = new SelectRule();
        selectRule1.setMandatoryDims(new Integer[] {});
        aggregationGroup1.setSelectRule(selectRule1);

        NAggregationGroup aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18 });
        aggregationGroup2.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule2 = new SelectRule();
        selectRule2.setMandatoryDims(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18 });
        aggregationGroup2.setSelectRule(selectRule2);

        RuleBasedIndex ruleBasedIndex = new RuleBasedIndex();
        List<Integer> lists = ReflectionTestUtils.invokeMethod(ruleBasedIndex, "recomputeSortedDimensions",
                Lists.newArrayList(aggregationGroup1, aggregationGroup2));
        Assert.assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18]", lists.toString());
    }

    private NIndexPlanManager getIndexPlanManager() {
        return NIndexPlanManager.getInstance(getTestConfig(), "default");
    }

    private IndexPlan getTmpTestIndexPlan(String name) throws IOException {
        return JsonUtil.readValue(getClass().getResourceAsStream(name), IndexPlan.class);
    }

    private void logLayouts(List<LayoutEntity> layouts) {
        layouts.sort((o1, o2) -> (int) (o1.getId() - o2.getId()));
        for (LayoutEntity allCuboidLayout : layouts) {
            log.debug("id:{}, auto:{}, manual:{}, {}", allCuboidLayout.getId(), allCuboidLayout.isAuto(),
                    allCuboidLayout.isManual(), allCuboidLayout.getColOrder());
        }
    }

    private void checkIntersection(RuleBasedIndex oldRule, IndexPlan plan, List<List<Integer>> colOrders) {
        Set<LayoutEntity> originLayouts = oldRule.genCuboidLayouts();
        Set<LayoutEntity> targetLayouts = plan.getRuleBasedIndex().genCuboidLayouts();

        val intersection = Sets.intersection(originLayouts, targetLayouts).stream()
                .map(diffLayout -> diffLayout.getOrderedDimensions().keySet().asList()).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEqualCollection(intersection, colOrders));
    }
}
