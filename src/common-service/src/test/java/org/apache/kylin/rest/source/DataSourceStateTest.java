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

package org.apache.kylin.rest.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.SourceTestCase;
import org.apache.spark.sql.SparderEnv;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class DataSourceStateTest extends SourceTestCase {

    private static final String PROJECT = "default";
    private static final String DATABASE = "SSB";

    @Test
    public void testLoadAllSourceInfoToCacheForcedTrue() {
        DataSourceState instance = DataSourceState.getInstance();

        instance.loadAllSourceInfoToCacheForced(PROJECT, true);
        List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
        Assert.assertFalse(cacheTables.isEmpty());
    }

    @Test
    public void testLoadAllSourceInfoToCacheForcedFalse() {
        DataSourceState instance = DataSourceState.getInstance();

        instance.loadAllSourceInfoToCacheForced(PROJECT, false);
        List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
        Assert.assertTrue(cacheTables.isEmpty());
    }

    @Test
    public void testLoadAllSourceInfoToCache() {
        DataSourceState instance = DataSourceState.getInstance();
        Thread thread = new Thread(instance);
        getTestConfig().setProperty("kylin.kerberos.project-level-enabled", "true");
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-seconds", "5");
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-interval-seconds", "1");
        thread.start();
        try {
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
                return !cacheTables.isEmpty();
            });
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(instance.getTables(PROJECT, DATABASE).isEmpty());
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-seconds", "900");
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-interval-seconds", "10");
        Thread thread2 = new Thread(instance);
        thread2.start();
        try {
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
                List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
                return !cacheTables.isEmpty();
            });
        } catch (Exception e) {
            // ignore
        }
        thread2.interrupt();
        getTestConfig().setProperty("kylin.kerberos.project-level-enabled", "false");
        instance.run();
        List<String> cacheTables1 = instance.getTables(PROJECT, DATABASE);
        Assert.assertFalse(cacheTables1.isEmpty());

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.source.load-hive-tablename-enable", "false");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectInstance.setPrincipal("test");
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        instance.run();
        List<String> cacheTables2 = instance.getTables(PROJECT, DATABASE);
        Assert.assertFalse(cacheTables2.isEmpty());
    }

    @Test
    public void testGetTablesJdbc() {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "8");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());

        DataSourceState instance = DataSourceState.getInstance();
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        Map<String, List<String>> testData = new HashMap<>();
        testData.put("t", Arrays.asList("aa", "ab", "bc"));
        sourceInfo.setTables(testData);
        instance.putCache("project#default", sourceInfo, Arrays.asList("aa", "ab", "bc"));
        Assert.assertFalse(instance.getTables(PROJECT, "t").isEmpty());

        instance.putCache("project#default", sourceInfo, Arrays.asList("t", "d"));
        List<String> tableList = instance.getTables(PROJECT, "t");
        Assert.assertFalse(tableList.isEmpty());
        Assert.assertEquals(3, tableList.size());

        Map<String, List<String>> testData1 = new HashMap<>();
        NHiveSourceInfo sourceInfo1 = new NHiveSourceInfo();
        testData1.put("d", Arrays.asList("aa", "cd"));
        sourceInfo1.setTables(testData1);
        instance.putCache("project#default", sourceInfo1, Arrays.asList("d"));
        List<String> tableList1 = instance.getTables(PROJECT, "t");
        Assert.assertFalse(tableList1.isEmpty());
        Assert.assertEquals(3, tableList1.size());

        Map<String, List<String>> testData2 = new HashMap<>();
        NHiveSourceInfo sourceInfo2 = new NHiveSourceInfo();
        testData2.put("t", Arrays.asList("aa", "cd"));
        sourceInfo2.setTables(testData2);
        instance.putCache("project#default", sourceInfo2, Arrays.asList("t"));
        List<String> tableList2 = instance.getTables(PROJECT, "t");
        Assert.assertFalse(tableList2.isEmpty());
        Assert.assertEquals(2, tableList2.size());
    }

    @Test
    public void testCheckIsAllNode() {
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.source.load-hive-tablename-enabled", "false");
        Assert.assertThrows(KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(DataSourceState.getInstance(), "checkIsAllNode"));
        config.setProperty("kylin.source.load-hive-tablename-enabled", "true");
    }

    @Test
    public void testStartSparder() {
        ReflectionTestUtils.invokeMethod(DataSourceState.getInstance(), "startSparder");
        Assert.assertFalse(SparderEnv.isSparkAvailable());
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.env", "mock");
        ReflectionTestUtils.invokeMethod(DataSourceState.getInstance(), "startSparder");
        Assert.assertTrue(SparderEnv.isSparkAvailable());
        SparderEnv.getSparkSession().stop();
    }
}
