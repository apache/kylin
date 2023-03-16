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

package org.apache.kylin.metadata.cube.cuboid;

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public class CuboidSchedulerTest extends NLocalFileMetadataTestCase {

    public static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void test2403_dimCap3() throws IOException {
        IndexPlan cube = utCube("2.1.0.20403", 3);

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(0);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(19, set.size());
        }

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(1);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(15, set.size());
        }

        {
            val set = cube.getRuleBasedIndex().getCuboidScheduler().getAllColOrders();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(31, set.size());
        }
    }

    @Test
    public void test2403_dimCap2() throws IOException {

        IndexPlan cube = utCube("2.1.0.20403", 2);

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(0);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(15, set.size());
        }

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(1);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(11, set.size());
        }

        {
            val set = cube.getRuleBasedIndex().getCuboidScheduler().getAllColOrders();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(24, set.size());
        }
    }

    @Test
    public void test2403_dimCap1() throws IOException {

        IndexPlan cube = utCube("2.1.0.20403", 1);

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(0);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(6, set.size());
        }

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(1);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(5, set.size());
        }

        {
            val set = cube.getRuleBasedIndex().getCuboidScheduler().getAllColOrders();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(11, set.size());
        }
    }

    @Test
    public void testMaskIsZero() throws IOException {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        IndexPlan cube = mgr.getIndexPlan("82fa7671-a935-45f5-8779-85703601f49a");
        cube = JsonUtil.deepCopy(cube, IndexPlan.class);
        cube.setIndexes(Lists.<IndexEntity> newArrayList());
        cube.initAfterReload(getTestConfig(), DEFAULT_PROJECT);
        val rule = new RuleBasedIndex();
        rule.setDimensions(Lists.<Integer> newArrayList());
        rule.setMeasures(Lists.<Integer> newArrayList());
        rule.setIndexPlan(cube);
        cube.setRuleBasedIndex(rule);
        val scheduler = (KECuboidSchedulerV1) cube.getRuleBasedIndex().getCuboidScheduler();
        Assert.assertEquals(0, scheduler.getAllColOrders().size());
    }

    private IndexPlan utCube(String resetVer, Integer resetDimCap) throws IOException {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        IndexPlan cube = mgr.getIndexPlan("82fa7671-a935-45f5-8779-85703601f49a");
        cube = JsonUtil.deepCopy(cube, IndexPlan.class);
        cube.setVersion(resetVer);

        if (resetDimCap != null) {
            for (NAggregationGroup g : cube.getRuleBasedIndex().getAggregationGroups())
                g.getSelectRule().dimCap = resetDimCap;
        }
        cube.initAfterReload(getTestConfig(), DEFAULT_PROJECT);
        return cube;
    }
}
