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

import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

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
    public void testRegisterHdfsMetricsFailed() {
        HdfsCapacityMetrics.registerHdfsMetrics();
        // scheduledExecutor may like this
        // java.util.concurrent.ScheduledThreadPoolExecutor@2d9caaeb[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
        String scheduledExecutor = HdfsCapacityMetrics.HDFS_METRICS_SCHEDULED_EXECUTOR.toString();
        String activeThreadStr = "active threads = ";
        int activeThreadIdx = scheduledExecutor.indexOf(activeThreadStr);
        String thread = scheduledExecutor.substring(activeThreadIdx + activeThreadStr.length(), activeThreadIdx + activeThreadStr.length() + 1);
        Assert.assertEquals(0, Integer.parseInt(thread));
    }

    @Test
    public void testRegisterHdfsMetrics() {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        HdfsCapacityMetrics.registerHdfsMetrics();
        // scheduledExecutor may like this
        // java.util.concurrent.ScheduledThreadPoolExecutor@4b5189ac[Running, pool size = 1, active threads = 1, queued tasks = 1, completed tasks = 0]
        String scheduledExecutor = HdfsCapacityMetrics.HDFS_METRICS_SCHEDULED_EXECUTOR.toString();
        String activeThreadStr = "active threads = ";
        int activeThreadIdx = scheduledExecutor.indexOf(activeThreadStr);
        String thread = scheduledExecutor.substring(activeThreadIdx + activeThreadStr.length(), activeThreadIdx + activeThreadStr.length() + 1);
        Assert.assertEquals(1, Integer.parseInt(thread));
    }

    @Test
    public void testHandleNodeHdfsMetrics() {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        EpochManager.getInstance().tryUpdateEpoch(EpochManager.GLOBAL, true);
        HdfsCapacityMetrics.handleNodeHdfsMetrics();
        Assert.assertTrue(HdfsCapacityMetrics.workingDirCapacity.size() > 0);
    }

    @Test
    public void testWriteHdfsMetrics() throws IOException {
        KylinConfig testConfig = getTestConfig();
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        Path projectPath = new Path(testConfig.getWorkingDirectoryWithConfiguredFs("newten"));
        FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(projectPath)) {
            fs.mkdirs(projectPath);
            fs.createNewFile(projectPath);
        }
        HdfsCapacityMetrics.writeHdfsMetrics();
    }

    @Test
    public void testReadHdfsMetrics() throws IOException {
        KylinConfig testConfig = getTestConfig();
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        Path projectPath = new Path(testConfig.getWorkingDirectoryWithConfiguredFs("newten"));
        FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(projectPath)) {
            fs.mkdirs(projectPath);
            fs.createNewFile(projectPath);
        }
        HdfsCapacityMetrics.writeHdfsMetrics();
        HdfsCapacityMetrics.readHdfsMetrics();
    }

    @Test
    public void testWriteAndReadHdfsMetrics() throws IOException {
        KylinConfig testConfig = getTestConfig();
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        EpochManager.getInstance().tryUpdateEpoch(EpochManager.GLOBAL, true);
        Path projectPath = new Path(testConfig.getWorkingDirectoryWithConfiguredFs("newten"));
        FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(projectPath)) {
            fs.mkdirs(projectPath);
            fs.createNewFile(projectPath);
        }
        HdfsCapacityMetrics.registerHdfsMetrics();

        Thread t1 = new Thread(() -> {
            await().pollDelay(new Duration(1, TimeUnit.SECONDS)).until(() -> true);
            HdfsCapacityMetrics.readHdfsMetrics();
            Assert.assertTrue(HdfsCapacityMetrics.workingDirCapacity.size() > 0);
        });
        t1.start();
        fs.deleteOnExit(projectPath);
        fs.deleteOnExit(HdfsCapacityMetrics.HDFS_CAPACITY_METRICS_PATH);
    }

    @Test
    public void testGetHdfsCapacityByProject() {
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        Assert.assertEquals(0L, (long) HdfsCapacityMetrics.getHdfsCapacityByProject("kylin"));
    }
}