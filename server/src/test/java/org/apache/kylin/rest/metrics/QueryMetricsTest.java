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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryMetricsTest extends ServiceTestBase {

    private static MBeanServer mBeanServer;
    private static ObjectName objectName;

    @Before
    public void setup() throws Exception {
        super.setup();

        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectName = new ObjectName("Hadoop:service=Kylin,name=Server_Total");
    }

    @Test
    public void testQueryMetrics() throws Exception {
        System.setProperty("kylin.query.metrics.enabled", "true");
        QueryMetricsFacade.init();

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

        QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse);

        Thread.sleep(2000);

        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QueryCount"));
        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QuerySuccessCount"));
        Assert.assertEquals(0L, mBeanServer.getAttribute(objectName, "QueryFailCount"));
        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "CacheHitCount"));

        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "ScanRowCountNumOps"));
        Assert.assertEquals(100.0, mBeanServer.getAttribute(objectName, "ScanRowCountAvgTime"));
        Assert.assertEquals(100.0, mBeanServer.getAttribute(objectName, "ScanRowCountMaxTime"));
        Assert.assertEquals(100.0, mBeanServer.getAttribute(objectName, "ScanRowCountMinTime"));

        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "ResultRowCountNumOps"));
        Assert.assertEquals(2.0, mBeanServer.getAttribute(objectName, "ResultRowCountMaxTime"));
        Assert.assertEquals(2.0, mBeanServer.getAttribute(objectName, "ResultRowCountAvgTime"));
        Assert.assertEquals(2.0, mBeanServer.getAttribute(objectName, "ResultRowCountMinTime"));

        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QueryLatencyNumOps"));
        Assert.assertEquals(10.0, mBeanServer.getAttribute(objectName, "QueryLatencyMaxTime"));
        Assert.assertEquals(10.0, mBeanServer.getAttribute(objectName, "QueryLatencyAvgTime"));
        Assert.assertEquals(10.0, mBeanServer.getAttribute(objectName, "QueryLatencyMinTime"));

        SQLResponse sqlResponse2 = new SQLResponse();
        sqlResponse2.setDuration(10);
        sqlResponse2.setCube("test_cube");
        sqlResponse2.setIsException(true);

        QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse2);

        Thread.sleep(2000);

        Assert.assertEquals(2L, mBeanServer.getAttribute(objectName, "QueryCount"));
        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QuerySuccessCount"));
        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QueryFailCount"));

        System.clearProperty("kylin.query.metrics.enabled");
    }

}
