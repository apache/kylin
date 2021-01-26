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
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.metrics.QuerySparkMetrics;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.shaded.com.google.common.cache.RemovalListener;
import org.apache.kylin.shaded.com.google.common.cache.RemovalNotification;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryMetricsTest extends ServiceTestBase {

    private static MBeanServer mBeanServer;
    private static ObjectName objectName;
    private static AtomicInteger sparkMetricsReportCnt = new AtomicInteger(0);

    @Before
    public void setup() throws Exception {
        super.setup();

        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectName = new ObjectName("Hadoop:service=Kylin,name=Server_Total");
    }

    @Test
    public void testQueryMetrics() throws Exception {
        System.setProperty("kylin.server.query-metrics-enabled", "true");
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

        QueryMetricsFacade.updateMetrics("", sqlRequest, sqlResponse);

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

        QueryMetricsFacade.updateMetrics("", sqlRequest, sqlResponse2);

        Thread.sleep(2000);

        Assert.assertEquals(2L, mBeanServer.getAttribute(objectName, "QueryCount"));
        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QuerySuccessCount"));
        Assert.assertEquals(1L, mBeanServer.getAttribute(objectName, "QueryFailCount"));

        System.clearProperty("kylin.server.query-metrics-enabled");
    }

    @Test
    public void testQueryStatisticsResult() throws Exception {
        System.setProperty("kylin.metrics.reporter-query-enabled", "true");
        QueryMetricsFacade.init();

        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql("select * from TEST_KYLIN_FACT");
        sqlRequest.setProject("default");

        QueryContext context = QueryContextFacade.current();
        
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

        int ctxId = 0;
        context.addContext(ctxId, "OLAP", true);
        context.addRPCStatistics(ctxId, "sandbox", "test_cube", "20100101000000_20150101000000", 3L, 3L, 3L, null, 80L,
                0L, 2L, 2L, 0L, 30L);

        sqlResponse.setCubeSegmentStatisticsList(context.getCubeSegmentStatisticsResultList());

        QueryMetricsFacade.updateMetrics("", sqlRequest, sqlResponse);

        Thread.sleep(2000);

        System.clearProperty("kylin.server.query-metrics-enabled");
        System.out.println("------------testQueryStatisticsResult done------------");
    }

    @Test
    public void testQuerySparkMetrics() throws Exception {
        sparkMetricsReportCnt.set(0);
        System.setProperty("kylin.server.query-metrics-enabled", "true");
        QuerySparkMetrics.init(new QuerySparkMetricsTestRemovalListener());
        QueryMetricsFacade.init();

        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql("select * from TEST_KYLIN_FACT");
        sqlRequest.setProject("default");

        String queryId1 = "1";
        generateSparkMetrics(queryId1);

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setDuration(10);
        sqlResponse.setCube("test_cube");
        sqlResponse.setCuboidIds("12345");
        sqlResponse.setRealizationTypes("4");
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

        QueryMetricsFacade.updateMetrics(queryId1, sqlRequest, sqlResponse);

        Thread.sleep(3000);

        updateSparkMetrics(queryId1);

        Assert.assertTrue(QuerySparkMetrics.getInstance().getQueryExecutionMetrics(queryId1) != null);

        String queryId2 = "2";
        generateSparkMetrics(queryId2);

        Thread.sleep(3000);

        Assert.assertTrue(QuerySparkMetrics.getInstance().getQueryExecutionMetrics(queryId1) == null);
        Assert.assertTrue(QuerySparkMetrics.getInstance().getQueryExecutionMetrics(queryId2) != null);

        updateSparkMetrics(queryId2);

        SQLResponse sqlResponse2 = new SQLResponse();
        sqlResponse2.setDuration(10);
        sqlResponse2.setCube("test_cube");
        sqlResponse2.setIsException(true);

        QueryMetricsFacade.updateMetrics(queryId2, sqlRequest, sqlResponse2);

        Thread.sleep(5000);

        Assert.assertTrue(QuerySparkMetrics.getInstance().getQueryExecutionMetrics(queryId2) == null);
        Assert.assertEquals(2, sparkMetricsReportCnt.get());
        System.clearProperty("kylin.server.query-metrics-enabled");
    }

    public void generateSparkMetrics(String queryId) {
        Integer id = Integer.valueOf(queryId);
        long executionStartTime = 0;
        long jobStartTime= 0;
        long submitTime = 0;

        if (id == 1) {
            executionStartTime = 1610609727972L;
            jobStartTime = 1610609772880L;
            submitTime = 1610610480942L;
        } else if (id == 2) {
            executionStartTime = 1610609876545L;
            jobStartTime = 1610609901699L;
            submitTime = 16106105016542L;
        }
        QuerySparkMetrics.getInstance().onJobStart(queryId, "test-sparder_" + id, id,
                executionStartTime, id, jobStartTime);
        QuerySparkMetrics.getInstance().onSparkStageStart(queryId, id, id, "stageType_" + id,
                submitTime);
    }

    public void updateSparkMetrics(String queryId) {
        Integer id = Integer.valueOf(queryId);
        long jobEndTime = 0;
        long executionEndTime = 0;
        boolean jobIsSuccess = true;

        QuerySparkMetrics.SparkStageMetrics queryStageMetrics = new QuerySparkMetrics.SparkStageMetrics();
        if (id == 1) {
            jobEndTime = 1610610734401L;
            executionEndTime = 1610612655793L;
            jobIsSuccess = true;
            queryStageMetrics.setMetrics(10000, 10, 10, 100, 10, 1, 10, 1000, 1000, 100);
        } else if (id == 2) {
            jobEndTime = 1610610750397L;
            executionEndTime = 1610612685275L;
            jobIsSuccess = false;
            queryStageMetrics.setMetrics(20000, 20, 20, 200, 20, 2, 20, 2000, 2000, 200);
        }
        QuerySparkMetrics.getInstance().updateSparkStageMetrics(queryId, id, id, true,
                queryStageMetrics);
        QuerySparkMetrics.getInstance().updateSparkJobMetrics(queryId, id, jobEndTime, jobIsSuccess);
        QuerySparkMetrics.getInstance().updateExecutionMetrics(queryId, executionEndTime);
    }

    public static void verifyQuerySparkMetrics(String queryId,
                                               QuerySparkMetrics.QueryExecutionMetrics queryExecutionMetrics) {
        sparkMetricsReportCnt.getAndIncrement();
        Assert.assertTrue(StringUtils.isNotBlank(queryId));
        Assert.assertTrue(queryExecutionMetrics != null);
        // verify
        int id = Integer.valueOf(queryId);
        Assert.assertTrue(queryExecutionMetrics.getSparkJobMetricsMap().get(id) != null);
        Assert.assertTrue(queryExecutionMetrics.getSparkJobMetricsMap().get(id) != null);
        Assert.assertEquals(queryExecutionMetrics.getSparderName(), "test-sparder_" + id);
        Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id).getJobId(), id);

        if (id == 1) {
            //SqlResponse metrics
            Assert.assertEquals(queryExecutionMetrics.getUser(), "ADMIN");
            Assert.assertEquals(queryExecutionMetrics.getProject(), "DEFAULT");
            Assert.assertEquals(queryExecutionMetrics.getQueryType(), "CACHE");
            Assert.assertEquals(queryExecutionMetrics.getRealization(), "test_cube");
            Assert.assertEquals(queryExecutionMetrics.getRealizationTypes(), "4");
            Assert.assertEquals(queryExecutionMetrics.getCuboidIds(), "12345");
            Assert.assertEquals(queryExecutionMetrics.getTotalScanCount(), 100);
            Assert.assertEquals(queryExecutionMetrics.getResultCount(), 2);
            Assert.assertEquals(queryExecutionMetrics.getException(), "NULL");

            Assert.assertEquals(queryExecutionMetrics.getSparderName(), "test-sparder_" + id);
            Assert.assertEquals(queryExecutionMetrics.getException(), "NULL");

            //SparkExecution metrics
            Assert.assertEquals(queryExecutionMetrics.getStartTime(), 1610609727972L);
            Assert.assertEquals(queryExecutionMetrics.getEndTime(), 1610612655793L);
            Assert.assertTrue(queryExecutionMetrics.getSparkJobMetricsMap().get(id).isSuccess());
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id).getStartTime(), 1610609772880L);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id).getEndTime(), 1610610734401L);

            //SparkStage metrics
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().size(), 1);
            Assert.assertTrue(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id) != null);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getStageType(), "stageType_" + id);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getStageId(), id);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getResultSize(), 10000);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorDeserializeCpuTime(), 10);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorDeserializeTime(), 10);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorRunTime(), 100);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorCpuTime(), 10);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getJvmGCTime(), 1);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getResultSerializationTime(), 10);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getMemoryBytesSpilled(), 1000);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getDiskBytesSpilled(), 1000);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getPeakExecutionMemory(), 100);
        } else if (id == 2) {
            //SqlResponse metrics
            Assert.assertEquals(queryExecutionMetrics.getUser(), "ADMIN");
            Assert.assertEquals(queryExecutionMetrics.getProject(), "DEFAULT");
            Assert.assertEquals(queryExecutionMetrics.getQueryType(), "PARQUET");
            Assert.assertEquals(queryExecutionMetrics.getRealization(), "test_cube");
            Assert.assertEquals(queryExecutionMetrics.getRealizationTypes(), null);
            Assert.assertEquals(queryExecutionMetrics.getCuboidIds(), null);
            Assert.assertEquals(queryExecutionMetrics.getTotalScanCount(), 0);
            Assert.assertEquals(queryExecutionMetrics.getResultCount(), 0);
            Assert.assertEquals(queryExecutionMetrics.getException(), "NULL");

            //SparkExecution metrics
            Assert.assertEquals(queryExecutionMetrics.getStartTime(), 1610609876545L);
            Assert.assertEquals(queryExecutionMetrics.getEndTime(), 1610612685275L);
            Assert.assertFalse(queryExecutionMetrics.getSparkJobMetricsMap().get(id).isSuccess());
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id).getStartTime(), 1610609901699L);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id).getEndTime(), 1610610750397L);

            //SparkStage metrics
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().size(), 1);
            Assert.assertTrue(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id) != null);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getStageId(), id);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getResultSize(), 20000);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorDeserializeCpuTime(), 20);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorDeserializeTime(), 20);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorRunTime(), 200);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getExecutorCpuTime(), 20);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getJvmGCTime(), 2);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getResultSerializationTime(), 20);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getMemoryBytesSpilled(), 2000);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getDiskBytesSpilled(), 2000);
            Assert.assertEquals(queryExecutionMetrics.getSparkJobMetricsMap().get(id)
                    .getSparkStageMetricsMap().get(id).getPeakExecutionMemory(), 200);
        }
    }

    private static class QuerySparkMetricsTestRemovalListener implements RemovalListener<String,
            QuerySparkMetrics.QueryExecutionMetrics> {
        @Override
        public void onRemoval(RemovalNotification<String, QuerySparkMetrics.QueryExecutionMetrics> notification) {
            try {
                verifyQuerySparkMetrics(notification.getKey(), notification.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
