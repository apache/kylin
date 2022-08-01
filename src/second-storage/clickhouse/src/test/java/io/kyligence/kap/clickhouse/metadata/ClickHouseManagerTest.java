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
package io.kyligence.kap.clickhouse.metadata;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.MockSecondStorage;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TablePlan;

public class ClickHouseManagerTest {
    private static final String DEFAULT_PROJECT = "default";
    private static final String TEST_DESCRIPTION = "test_description";

    @Before
    public void setUp() throws IOException {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        Unsafe.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        MockSecondStorage.mock();
        SecondStorage.init(true);
        TestUtils.createEmptyClickHouseConfig();
    }

    @After
    public void tearDown() {
        Unsafe.clearProperty("kylin.second-storage.class");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCRUD() throws NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Manager<TablePlan> manager = SecondStorage.tablePlanManager(config, DEFAULT_PROJECT);
        Assert.assertNotNull(manager);

        // refect
        Class<? extends Manager<TablePlan>> managerClass =
                (Class<? extends Manager<TablePlan>>) manager.getClass();
        Constructor<? extends Manager<TablePlan>> constructor =
                managerClass.getDeclaredConstructor(KylinConfig.class, String.class);
        Unsafe.changeAccessibleObject(constructor, true);
        final Manager<TablePlan> refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);

        final String cubeName = RandomUtil.randomUUIDStr();

        TablePlan cube = TablePlan.builder()
                .setModel(cubeName)
                .setDescription(TEST_DESCRIPTION)
                .build();
        Assert.assertNotNull(manager.createAS(cube));

        // list
        List<TablePlan> cubes = manager.listAll();
        Assert.assertEquals(1, cubes.size());

        // get
        TablePlan cube2 = manager.get(cubeName).orElse(null);
        Assert.assertNotNull(cube2);

        // update
        try {
            cube2.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        TablePlan cube3 = manager.update(cube.getUuid(), copyForWrite -> copyForWrite.setDescription("new_description"));
        Assert.assertEquals("new_description", cube3.getDescription());

        // delete
        manager.delete(cube);
        TablePlan cube4 = manager.get(cubeName).orElse(null);
        Assert.assertNull(cube4);
        Assert.assertEquals(0, manager.listAll().size());
    }

    @Test
    public void testAddTableEntity() {
        final String cubeName = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        IndexPlan indexPlan = manager.getIndexPlan(cubeName);
        List<LayoutEntity> allLayouts = indexPlan.getAllLayouts();
        Assert.assertTrue(allLayouts.size() >=2);

        LayoutEntity layout0 = allLayouts.get(0);

        Manager<TablePlan> ckManager = SecondStorage.tablePlanManager(config, DEFAULT_PROJECT);
        TablePlan firstPlan = ckManager.makeSureRootEntity(cubeName);

        TablePlan plan = firstPlan.createTableEntityIfNotExists(layout0, true);

        Assert.assertNotNull(plan);
        Assert.assertEquals(1, plan.getTableMetas().size());
        Assert.assertEquals(TableEntity.DEFAULT_SHARD, plan.getTableMetas().get(0).getShardNumbers());

        LayoutEntity layout1 = allLayouts.get(1);
        TablePlan plan1 = plan.createTableEntityIfNotExists(layout1, true);

        Assert.assertNotNull(plan1);
        Assert.assertEquals(plan.getId(), plan1.getId());
        Assert.assertEquals(2, plan1.getTableMetas().size());
        Assert.assertSame(plan1, plan1.getTableMetas().get(0).getTablePlan());
        Assert.assertSame(plan1, plan1.getTableMetas().get(1).getTablePlan());

        TablePlan plan2 = plan1.createTableEntityIfNotExists(layout1, false);
        Assert.assertEquals(plan.getId(), plan2.getId());
        Assert.assertEquals(2, plan2.getTableMetas().size());
        Assert.assertSame(plan2, plan2.getTableMetas().get(0).getTablePlan());
        Assert.assertSame(plan2, plan2.getTableMetas().get(1).getTablePlan());

        final int updateShardNumbers = 6;
        TablePlan plan3 = ckManager.update(cubeName,
                copyForWrite -> copyForWrite.getTableMetas().get(0).setShardNumbers(updateShardNumbers));

        Assert.assertEquals(plan.getId(), plan3.getId());
        Assert.assertEquals(2, plan3.getTableMetas().size());
        Assert.assertEquals(updateShardNumbers, plan3.getTableMetas().get(0).getShardNumbers());
    }
}
