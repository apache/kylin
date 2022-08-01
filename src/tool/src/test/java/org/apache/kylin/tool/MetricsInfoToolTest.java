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

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.tool.metrics.MetricsInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsInfoToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepare();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testMetricsInfoSuccess() throws Exception {
        MetricsInfoTool tool = new MetricsInfoTool();
        String date = DateFormat.formatToDateStr(System.currentTimeMillis(), DateFormat.COMPACT_DATE_PATTERN);
        tool.execute(new String[] { "-date", date });
        MetricsInfo info = tool.inPutMetricsInfo(date);
        Assert.assertNotNull(info);
        Assert.assertTrue(info.getProjectMetrics().size() > 0);
        Assert.assertNull(info.getProjectMetrics().get(0).getModelAddCount());

    }

    @Test
    public void testMetricsInfoFail() throws Exception {
        testMetricsInfoFail("20180101");
        testMetricsInfoFail("20210229");
        testMetricsInfoFail("20211301");
        String date = DateFormat.formatToDateStr(System.currentTimeMillis(), DateFormat.COMPACT_DATE_PATTERN);
        testMetricsInfoFail("0" + date);

    }

    private void testMetricsInfoFail(String date) {
        MetricsInfoTool tool = new MetricsInfoTool();
        boolean result = true;
        try {
            tool.execute(new String[] { "-date", date });
        } catch (Exception e) {
            result = false;
        }
        Assert.assertFalse(result);
    }

    private void prepare() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(config);
    }

}
