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

package org.apache.kylin.rest.metrics;

import static org.apache.kylin.common.metrics.common.MetricsNameBuilder.buildCubeMetricPrefix;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kylin.common.metrics.common.Metrics;
import org.apache.kylin.common.metrics.common.MetricsConstant;
import org.apache.kylin.common.metrics.common.MetricsFactory;
import org.apache.kylin.common.metrics.common.MetricsNameBuilder;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Snapshot;

public class QueryMetrics2Test extends ServiceTestBase {

    private static MBeanServer mBeanServer;
    private static ObjectName objectName;

    @Before
    public void setup() throws Exception {
        super.setup();

        objectName = new ObjectName("Hadoop:service=Kylin,name=Server_Total");
    }

    @Test
    public void testQueryMetrics() throws Exception {
        System.setProperty("kylin.server.query-metrics2-enabled", "true");
        QueryMetrics2Facade.init();
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql("select * from TEST_KYLIN_FACT");
        sqlRequest.setProject("default");
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setDuration(10);
        sqlResponse.setCube("test_cube");
        sqlResponse.setIsException(false);
        sqlResponse.setTotalScanCount(100);
        List<String> list1 = new ArrayList<>();
        list1.add("111");
        list1.add("112");
        List<String> list2 = new ArrayList<>();
        list2.add("111");
        list2.add("112");
        List<List<String>> results = new ArrayList<>();
        results.add(list1);
        results.add(list2);
        sqlResponse.setResults(results);
        sqlResponse.setStorageCacheUsed(true);
        Metrics metrics = MetricsFactory.getInstance();
        QueryMetrics2Facade.updateMetrics(sqlRequest, sqlResponse);
        String prefix = buildCubeMetricPrefix(sqlRequest.getProject(), sqlResponse.getCube().replace("=", "->"));
        Thread.sleep(2000);

        Assert.assertEquals(1L,
                metrics.getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_COUNT)).getCount());
        Assert.assertEquals(1L,
                metrics.getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_SUCCESS_COUNT))
                        .getCount());
        Assert.assertEquals(0L, metrics
                .getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_FAIL_COUNT)).getCount());
        Assert.assertEquals(1L, metrics
                .getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_CACHE_COUNT)).getCount());
        //
        Snapshot queryScanSnapshot = metrics
                .getHistogram(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_SCAN_ROWCOUNT))
                .getSnapshot();
        Assert.assertEquals(100.0, queryScanSnapshot.getMean(), 0);
        Assert.assertEquals(100.0, queryScanSnapshot.getMax(), 0);
        Assert.assertEquals(100.0, queryScanSnapshot.getMin(), 0);

        Snapshot queryResultSnapshot = metrics
                .getHistogram(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_RESULT_ROWCOUNT))
                .getSnapshot();

        Assert.assertEquals(2.0, queryResultSnapshot.getMean(), 0);
        Assert.assertEquals(2.0, queryResultSnapshot.getMax(), 0);
        Assert.assertEquals(2.0, queryResultSnapshot.getMin(), 0);

        Snapshot queryLatencySnapshot = metrics
                .getTimer(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_DURATION)).getSnapshot();
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(10), queryLatencySnapshot.getMean(), 0);
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(10), queryLatencySnapshot.getMax(), 0);
        Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(10), queryLatencySnapshot.getMin(), 0);

        SQLResponse sqlResponse2 = new SQLResponse();
        sqlResponse2.setDuration(10);
        sqlResponse2.setCube("test_cube");
        sqlResponse2.setIsException(true);

        QueryMetrics2Facade.updateMetrics(sqlRequest, sqlResponse2);
        prefix = buildCubeMetricPrefix(sqlRequest.getProject(), sqlResponse.getCube().replace("=", "->"));
        Assert.assertEquals(2L,
                metrics.getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_COUNT)).getCount());
        Assert.assertEquals(1L,
                metrics.getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_SUCCESS_COUNT))
                        .getCount());
        Assert.assertEquals(1L, metrics
                .getCounter(MetricsNameBuilder.buildMetricName(prefix, MetricsConstant.QUERY_FAIL_COUNT)).getCount());
    }

}
