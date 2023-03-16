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

package org.apache.kylin.metadata.cube;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NIndexPlanManagerTest {
    private static final String DEFAULT_PROJECT = "default";
    private static final String TEST_DESCRIPTION = "test_description";

    @Before
    public void setUp() {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
    }

    @Test
    public void testCRUD() throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException,
            InvocationTargetException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        final String cubeName = RandomUtil.randomUUIDStr();
        //refect
        Class<? extends NIndexPlanManager> managerClass = manager.getClass();
        Constructor<? extends NIndexPlanManager> constructor = managerClass.getDeclaredConstructor(KylinConfig.class,
                String.class);
        Unsafe.changeAccessibleObject(constructor, true);
        final NIndexPlanManager refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);
        Assert.assertEquals(refectionManage.listAllIndexPlans().size(), manager.listAllIndexPlans().size());

        //create
        int cntBeforeCreate = manager.listAllIndexPlans().size();
        IndexPlan cube = new IndexPlan();
        cube.setUuid(cubeName);
        cube.setDescription(TEST_DESCRIPTION);
        CubeTestUtils.createTmpModel(config, cube);
        Assert.assertNotNull(manager.createIndexPlan(cube));

        // list
        List<IndexPlan> cubes = manager.listAllIndexPlans();
        Assert.assertEquals(cntBeforeCreate + 1, cubes.size());

        // get
        cube = manager.getIndexPlan(cubeName);
        Assert.assertNotNull(cube);

        // update
        try {
            cube.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        cube = manager.updateIndexPlan(cube.getUuid(), new NIndexPlanManager.NIndexPlanUpdater() {
            @Override
            public void modify(IndexPlan copyForWrite) {
                copyForWrite.setDescription("new_description");
            }
        });
        Assert.assertEquals("new_description", cube.getDescription());

        // delete
        manager.dropIndexPlan(cube);
        cube = manager.getIndexPlan(cubeName);
        Assert.assertNull(cube);
        Assert.assertEquals(cntBeforeCreate, manager.listAllIndexPlans().size());
    }

    @Test
    public void testRemoveLayouts_cleanupDataflow() {
        val config = KylinConfig.getInstanceFromEnv();
        val manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        var indexPlan = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dfManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        var df = dfManager.getDataflow(indexPlan.getId());

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        val seg1 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val seg2 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));

        val update2 = new NDataflowUpdate(df.getUuid());
        seg1.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(seg1);
        update2.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df, seg1.getId(), 1L),
                NDataLayout.newDataLayout(df, seg1.getId(), 10001L),
                NDataLayout.newDataLayout(df, seg1.getId(), 10002L));
        dfManager.updateDataflow(update2);

        manager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(10001L, 10002L), true, true);
        });

        df = dfManager.getDataflow(indexPlan.getId());
        Assert.assertNotNull(df.getSegment(seg1.getId()).getLayoutsMap().get(1L));
        Assert.assertNull(df.getSegment(seg1.getId()).getLayoutsMap().get(10001L));
        Assert.assertNull(df.getSegment(seg1.getId()).getLayoutsMap().get(10002L));
        Assert.assertEquals(0, df.getSegment(seg2.getId()).getLayoutsMap().size());
    }

    @Test
    public void testSaveWithManualLayouts() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        var cube = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val originCuboidSize = cube.getIndexes().size();

        cube = manager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getAllIndexes());
        });
        val savedCuboidSize = cube.getIndexes().size();

        Assert.assertEquals(originCuboidSize, savedCuboidSize);
    }

    @Test
    public void testRemoveLayout() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);

        var cube = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        logLayouts(cube);
        log.debug("-------------");
        val originalSize = cube.getAllLayouts().size();
        val layout = cube.getLayoutEntity(1000001L);
        Assert.assertTrue(layout.isAuto());
        Assert.assertTrue(layout.isManual());
        cube = manager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1000001L, 10002L), true, true);
        });
        logLayouts(cube);
        Assert.assertEquals(originalSize - 2, cube.getAllLayouts().size());
        Assert.assertEquals(1, cube.getRuleBasedIndex().getLayoutBlackList().size());
        val layout2 = cube.getLayoutEntity(1000001L);
        Assert.assertNull(layout2);
    }

    private void logLayouts(IndexPlan indexPlan) {
        for (LayoutEntity layout : indexPlan.getAllLayouts()) {
            log.debug("layout id:{} -- {}, auto:{}, manual:{}, col:{}, sort:{}", layout.getId(),
                    layout.getIndex().getId(), layout.isAuto(), layout.isManual(), layout.getColOrder(),
                    layout.getSortByColumns());
        }
    }

    @Test
    public void getIndexPlan_WithSelfBroken() {
        val project = "broken_test";
        val indexPlanId = "039eef32-9691-4c88-93ba-d65c58a1ab7a";
        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val indexPlan = indexPlanManager.getIndexPlan(indexPlanId);
        Assert.assertEquals(true, indexPlan.isBroken());

        Assert.assertEquals(indexPlanManager.listAllIndexPlans().size(), 1);
    }

    @Test
    public void getIndexPlanByModelAlias_WithSelfBrokenAndHealthModel() {
        val project = "broken_test";
        val indexPlanId = "039eef32-9691-4c88-93ba-d65c58a1ab7a";
        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val indexPlan = indexPlanManager.getIndexPlan(indexPlanId);

        val indexPlan2 = indexPlanManager.getIndexPlanByModelAlias("AUTO_MODEL_TEST_ACCOUNT_1");
        Assert.assertNotNull(indexPlan2);
        Assert.assertEquals(indexPlan.getId(), indexPlan2.getId());
    }

    @Test
    public void getIndexPlanByModelAlias_WithBrokenModel() {
        val project = "broken_test";
        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val indexPlan2 = indexPlanManager.getIndexPlanByModelAlias("AUTO_MODEL_TEST_COUNTRY_1");
        Assert.assertNull(indexPlan2);
    }

    @Test
    public void testChangeShardByCol_AfterDeleteAggIndex() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        manager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(20000020001L, 20000010001L, 20000000001L, 30001L, 20001L, 10002L,
                    10001L, 1000001L, 1060001L, 1050001L), true, true);
        });
        manager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.setAggShardByColumns(Lists.newArrayList(5));
        });

        val plan = manager.getIndexPlan(modelId);
        Assert.assertEquals(
                "[1, 1030002, 1140001, 1010001, 1080002, 1070002, 1090001, 1100001, 1020001, 1040001, 1150001, 1130001]",
                plan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList()).toString());
    }

    @Test
    public void testAvailableIndexesCount() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        long count = manager.getAvailableIndexesCount(DEFAULT_PROJECT, "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(5, count);

        long count1 = manager.getAvailableIndexesCount(DEFAULT_PROJECT, "741ca86a-1f13-46da-a59f-95fb68615e3b");
        Assert.assertEquals(0, count1);

        long count2 = manager.getAvailableIndexesCount(DEFAULT_PROJECT, "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        Assert.assertEquals(0, count2);
    }
}
