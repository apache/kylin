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
import java.util.Collections;
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
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;

public class ClickHouseFlowManagerTest {

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
        Manager<TableFlow> manager = SecondStorage.tableFlowManager(config, DEFAULT_PROJECT);
        Assert.assertNotNull(manager);

        // refect
        Class<? extends Manager<TableFlow>> managerClass = (Class<? extends Manager<TableFlow>>) manager.getClass();
        Constructor<? extends Manager<TableFlow>> constructor =
                managerClass.getDeclaredConstructor(KylinConfig.class, String.class);
        Unsafe.changeAccessibleObject(constructor, true);
        final Manager<TableFlow> refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);

        final String cubeName = RandomUtil.randomUUIDStr();

        TableFlow flow = TableFlow.builder()
                .setModel(cubeName)
                .setDescription(TEST_DESCRIPTION)
                .build();
        Assert.assertNotNull(manager.createAS(flow));

        // list
        List<TableFlow> flows = manager.listAll();
        Assert.assertEquals(1, flows.size());

        // get
        TableFlow flow2 = manager.get(cubeName).orElse(null);
        Assert.assertNotNull(flow2);

        // update
        try {
            flow2.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        TableFlow flow3 = manager.update(flow.getUuid(), copyForWrite -> copyForWrite.setDescription("new_description"));
        Assert.assertEquals("new_description", flow3.getDescription());

        // delete
        manager.delete(flow);
        TableFlow flow4 = manager.get(cubeName).orElse(null);
        Assert.assertNull(flow4);
        Assert.assertEquals(0, manager.listAll().size());
    }

    @Test
    public void testTableData() {
        final String cubeName = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<LayoutEntity> allLayouts = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT)
                .getIndexPlan(cubeName)
                .getAllLayouts();
        Assert.assertTrue(allLayouts.size() >= 2);
        LayoutEntity layout0 = allLayouts.get(0);
        TableFlow firstFlow = SecondStorage.tableFlowManager(config, DEFAULT_PROJECT)
                .makeSureRootEntity(cubeName);
        TableFlow flow1 = firstFlow.update(copied ->
            copied.upsertTableData(
                    layout0,
                    t -> {},
                    ()-> TableData.builder()
                            .setLayoutEntity(layout0)
                            .setPartitionType(PartitionType.FULL)
                            .build()
            ));
        Assert.assertNotNull(flow1);
        Assert.assertEquals(1, flow1.getTableDataList().size());
        Assert.assertEquals(PartitionType.FULL, flow1.getTableDataList().get(0).getPartitionType());
        Assert.assertSame(flow1.getTableDataList().get(0), flow1.getEntity(layout0).orElse(null));
        TableData data = flow1.getTableDataList().get(0);
        Assert.assertEquals(0, data.getPartitions().size());
        Assert.assertNull(data.getSchemaURL());
        Assert.assertNull(data.getShardJDBCURLs(DEFAULT_PROJECT, null));

        TableFlow flow2 = flow1.update(copied ->
                copied.upsertTableData(
                        layout0,
                        t -> t.addPartition(TablePartition.builder()
                                .setShardNodes(Collections.singletonList("xxx"))
                                .setSegmentId("yyy").build()),
                        ()-> null));
        Assert.assertNotNull(flow2);
        Assert.assertEquals(1, flow2.getTableDataList().size());
        Assert.assertEquals(PartitionType.FULL, flow2.getTableDataList().get(0).getPartitionType());
        Assert.assertSame(flow2.getTableDataList().get(0), flow2.getEntity(layout0).orElse(null));
        TableData data1 = flow2.getTableDataList().get(0);
        Assert.assertEquals(1, data1.getPartitions().size());
        Assert.assertEquals(1, data1.getPartitions().get(0).getShardNodes().size());
        Assert.assertEquals("yyy", data1.getPartitions().get(0).getSegmentId());
        Assert.assertEquals("xxx", data1.getPartitions().get(0).getShardNodes().get(0));
    }

}
