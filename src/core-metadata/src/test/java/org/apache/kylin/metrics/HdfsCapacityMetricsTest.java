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

package org.apache.kylin.metrics;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HdfsCapacityMetricsTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    @Ignore("unstable")
    public void testRegisterHdfsMetrics() throws InterruptedException {
        overwriteSystemProp("kylin.storage.check-quota-enabled", "true");
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        // scheduledExecutor may like this
        // java.util.concurrent.ScheduledThreadPoolExecutor@4b5189ac[Running, pool size = 1, active threads = 1, queued tasks = 1, completed tasks = 0]
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(getTestConfig());
        Assert.assertEquals(1, hdfsCapacityMetrics.getPoolSize());
        Assert.assertTrue(hdfsCapacityMetrics.getActiveCount() <= 1);
    }

    @Test
    public void testRegisterHdfsMetricsQuotaStorageEnabledFalse() {
        overwriteSystemProp("kylin.storage.check-quota-enabled", "false");
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(getTestConfig());
        Assert.assertEquals(0, hdfsCapacityMetrics.getActiveCount());
    }

    @Test
    public void testHandleNodeHdfsMetrics() {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(getTestConfig());
        EpochManager.getInstance().tryUpdateEpoch(EpochManager.GLOBAL, true);
        hdfsCapacityMetrics.handleNodeHdfsMetrics();
        Assert.assertTrue(hdfsCapacityMetrics.getWorkingDirCapacity().size() > 0);
    }

    @Test
    public void testWriteHdfsMetrics() throws IOException {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        KylinConfig testConfig = getTestConfig();
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(testConfig);
        Path projectPath = new Path(testConfig.getWorkingDirectoryWithConfiguredFs("newten"));
        FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(projectPath)) {
            fs.mkdirs(projectPath);
            fs.createNewFile(projectPath);
        }
        Assert.assertTrue(hdfsCapacityMetrics.getWorkingDirCapacity().isEmpty());
        hdfsCapacityMetrics.writeHdfsMetrics();
        Assert.assertEquals(28, hdfsCapacityMetrics.getWorkingDirCapacity().size());

    }

    @Test
    public void testReadHdfsMetrics() throws IOException {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        KylinConfig testConfig = getTestConfig();
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(testConfig);
        Path projectPath = new Path(testConfig.getWorkingDirectoryWithConfiguredFs("newten"));
        FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(projectPath)) {
            fs.mkdirs(projectPath);
            fs.createNewFile(projectPath);
        }
        hdfsCapacityMetrics.writeHdfsMetrics();
        Assert.assertEquals(hdfsCapacityMetrics.getWorkingDirCapacity().size(), hdfsCapacityMetrics.readHdfsMetrics().size());
    }

    @Test
    public void testWriteAndReadHdfsMetrics() throws IOException {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        KylinConfig testConfig = getTestConfig();
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(testConfig);
        EpochManager.getInstance().tryUpdateEpoch(EpochManager.GLOBAL, true);
        Path projectPath = new Path(testConfig.getWorkingDirectoryWithConfiguredFs("newten"));
        FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(projectPath)) {
            fs.mkdirs(projectPath);
            fs.createNewFile(projectPath);
        }
        Thread t1 = new Thread(() -> {
            await().pollDelay(new Duration(1, TimeUnit.SECONDS)).until(() -> true);
            hdfsCapacityMetrics.readHdfsMetrics();
            Assert.assertTrue(hdfsCapacityMetrics.getWorkingDirCapacity().size() > 0);
        });
        t1.start();
        fs.deleteOnExit(projectPath);
        fs.deleteOnExit(hdfsCapacityMetrics.getHdfsCapacityMetricsPath());
    }

    @Test
    public void testGetHdfsCapacityByProject() {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        overwriteSystemProp("kylin.storage.check-quota-enabled", "true");
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(getTestConfig());
        Assert.assertEquals(0L, (long) hdfsCapacityMetrics.getHdfsCapacityByProject("kylin"));
    }
}