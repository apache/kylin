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
package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LogOutputTestCase;
import org.apache.kylin.tool.util.ToolUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class InfluxDBToolTest extends LogOutputTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testDumpInfluxDBMetrics() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        InfluxDBTool.dumpInfluxDBMetrics(mainDir);

        Assert.assertTrue(new File(mainDir, "system_metrics").exists());
    }

    @Test
    public void testDumpInfluxDBMonitorMetrics() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        InfluxDBTool.dumpInfluxDBMonitorMetrics(mainDir);

        Assert.assertTrue(new File(mainDir, "monitor_metrics").exists());
    }

    @Test
    public void testDumpInfluxDBInPortUnavailable() throws Exception {
        int port = ToolUtilTest.getFreePort();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        String originHost = kapConfig.getMetricsRpcServiceBindAddress();
        kylinConfig.setProperty("kylin.metrics.influx-rpc-service-bind-address", "127.0.0.1:" + port);
        InfluxDBTool.dumpInfluxDBMetrics(mainDir);
        Assert.assertTrue(containsLog(
                String.format(Locale.ROOT, "Failed to Connect influxDB in 127.0.0.1:%s, skip dump.", port)));
        clearLogs();
        InfluxDBTool.dumpInfluxDBMonitorMetrics(mainDir);
        Assert.assertTrue(containsLog(
                String.format(Locale.ROOT, "Failed to Connect influxDB in 127.0.0.1:%s, skip dump.", port)));
        kylinConfig.setProperty("kylin.metrics.influx-rpc-service-bind-address", originHost);
    }

}
