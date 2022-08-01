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

package org.apache.kylin.metadata.query;

import static org.awaitility.Awaitility.await;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BigQueryThresholdUpdaterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void destroy() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testBigQueryThresholdInit() {
        int instance = 1;
        int core = 1;
        BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(instance, core);
        Assert.assertTrue(BigQueryThresholdUpdater.getBigQueryThreshold() > 0);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "100000000");
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(instance, core);
            Assert.assertEquals(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold(),
                    BigQueryThresholdUpdater.getBigQueryThreshold());
        }
    }

    @Test
    public void testCollectQueryScanRowsAndTime() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "100000000");
            BigQueryThresholdUpdater.resetBigQueryThreshold();
            int n = 1000;
            for (int i = 0; i < n; i++) {
                long duration = new Random().nextInt(10000) + KapConfig.getInstanceFromEnv().getBigQuerySecond() * 1000;
                if (i == n - 1) {
                    BigQueryThresholdUpdater.setLastUpdateTime(System.currentTimeMillis()
                            - KapConfig.getInstanceFromEnv().getBigQueryThresholdUpdateIntervalSecond() * 1000);
                }
                BigQueryThresholdUpdater.collectQueryScanRowsAndTime(new Random().nextInt(10000),
                        KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() - 1);
                BigQueryThresholdUpdater.collectQueryScanRowsAndTime(duration,
                        KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
            }
            long collectScanRows = KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1;
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                    () -> Assert.assertEquals(collectScanRows, BigQueryThresholdUpdater.getBigQueryThreshold()));
        }
    }
}
