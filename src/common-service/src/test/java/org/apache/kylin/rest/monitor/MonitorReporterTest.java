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
package org.apache.kylin.rest.monitor;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.metrics.service.MonitorMetric;
import org.apache.kylin.common.metrics.service.QueryMonitorMetric;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

@Ignore("TODO: Expected to remove this MonitorReporter.")
public class MonitorReporterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    public QueryMonitorMetric mockQueryMonitorMetric() {
        QueryMonitorMetric monitorMetric = new QueryMonitorMetric();
        monitorMetric.setHost("127.0.0.1");
        monitorMetric.setPort("7070");
        monitorMetric.setPid("22333");
        monitorMetric.setNodeType("query");
        monitorMetric.setCreateTime(System.currentTimeMillis());

        monitorMetric.setLastResponseTime(29 * 1000L);
        monitorMetric.setErrorAccumulated(1);
        monitorMetric.setSparkRestarting(false);

        return monitorMetric;
    }

    @Test
    public void testBasic() {
        MonitorReporter monitorReporter = MonitorReporter.getInstance();
        monitorReporter.reportInitialDelaySeconds = 5;
        monitorReporter.startReporter();

        monitorReporter.submit(new AbstractMonitorCollectTask(
                Lists.newArrayList(ClusterConstant.ALL, ClusterConstant.QUERY, ClusterConstant.JOB)) {
            @Override
            protected MonitorMetric collect() {
                return mockQueryMonitorMetric();
            }
        });

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(Integer.valueOf(1), monitorReporter.getQueueSize()));

        Awaitility.await().atMost(8, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(Integer.valueOf(0), monitorReporter.getQueueSize()));

        monitorReporter.stopReporter();
    }

}
