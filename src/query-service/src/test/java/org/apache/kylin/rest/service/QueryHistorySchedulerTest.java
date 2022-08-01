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
package org.apache.kylin.rest.service;

import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.query.util.SparkJobTraceMetric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class QueryHistorySchedulerTest extends NLocalFileMetadataTestCase {

    QueryHistoryScheduler queryHistoryScheduler;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        queryHistoryScheduler = QueryHistoryScheduler.getInstance();
        queryHistoryScheduler.queryMetricsQueue.clear();
    }

    @After
    public void destroy() throws Exception {
        cleanupTestMetadata();
        queryHistoryScheduler.queryMetricsQueue.clear();
        queryHistoryScheduler.shutdown();
    }

    @Test
    public void testWriteQueryHistoryAsynchronousNormal() throws Exception {
        // product
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(5578L);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(true);
        queryMetrics.setQueryTime(1584888338274L);
        queryMetrics.setProjectName("default");

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName("default");

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(realizationMetricsList);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);

        QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        Assert.assertEquals(2, queryHistoryScheduler.queryMetricsQueue.size());

        // consume
        queryHistoryScheduler.init();
        await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0, queryHistoryScheduler.queryMetricsQueue.size());
        });
    }

    @Test
    public void testWriteQueryHistoryAsynchronousIfBufferFull() throws Exception {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(5578L);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(true);
        queryMetrics.setQueryTime(1584888338274L);
        queryMetrics.setProjectName("default");

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName("default");

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        queryHistoryInfo.setRealizationMetrics(realizationMetricsList);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);

        QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);

        // insert 1500 to queryHistoryQueue
        for (long i = 0; i < 1500; i++) {
            queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        }
        // lost 500 queryHistory
        Assert.assertEquals(500, queryHistoryScheduler.queryMetricsQueue.size());
    }

    @Test
    public void testCollectedFinished() {
        QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
        String queryId = "12sy4s87-f912-6dw2-a1e1-8ff3234u2e6b1";
        long prepareAndSubmitJobMs = 10;
        long waitForExecutionMs = 200;
        long executionMs = 1200;
        long fetchResultMs = 20;
        SparkJobTraceMetric sparkJobTraceMetric = new SparkJobTraceMetric(prepareAndSubmitJobMs, waitForExecutionMs,
                executionMs, fetchResultMs);
        QueryMetrics queryMetrics = new QueryMetrics(queryId, "192.168.1.6:7070");
        queryMetrics.setQueryDuration(5578L);
        queryMetrics.setQueryTime(1584888338274L);
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
        List<QueryHistoryInfo.QueryTraceSpan> queryTraceSpans = new ArrayList<>();
        queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB,
                QueryTrace.SPAN_GROUPS.get(QueryTrace.WAIT_FOR_EXECUTION), 0));
        queryHistoryInfo.setTraces(queryTraceSpans);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
        Boolean isCollectedFinished = queryHistoryScheduler.isCollectedFinished(queryId, sparkJobTraceMetric,
                queryMetrics);
        Assert.assertTrue(isCollectedFinished);
        List<QueryHistoryInfo.QueryTraceSpan> newQueryTraceSpans = queryMetrics.getQueryHistoryInfo().getTraces();
        Assert.assertEquals(prepareAndSubmitJobMs, newQueryTraceSpans.get(0).getDuration());
        Assert.assertEquals(waitForExecutionMs, newQueryTraceSpans.get(1).getDuration());
        Assert.assertEquals(executionMs, newQueryTraceSpans.get(2).getDuration());
        Assert.assertTrue(newQueryTraceSpans.get(3).getDuration() > 0);
        queryTraceSpans = new ArrayList<>();
        queryMetrics.getQueryHistoryInfo().setTraces(queryTraceSpans);
        queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB,
                QueryTrace.SPAN_GROUPS.get(QueryTrace.WAIT_FOR_EXECUTION), 0));
        isCollectedFinished = queryHistoryScheduler.isCollectedFinished(queryId, null, queryMetrics);
        Assert.assertTrue(isCollectedFinished);
        newQueryTraceSpans = queryMetrics.getQueryHistoryInfo().getTraces();
        Assert.assertEquals(1, newQueryTraceSpans.size());
        queryMetrics.setQueryDuration(5000);
        queryMetrics.setQueryTime(System.currentTimeMillis());
        isCollectedFinished = queryHistoryScheduler.isCollectedFinished(queryId, null, queryMetrics);
        Assert.assertFalse(isCollectedFinished);
        Assert.assertEquals(1, queryHistoryScheduler.queryMetricsQueue.size());
    }
}
