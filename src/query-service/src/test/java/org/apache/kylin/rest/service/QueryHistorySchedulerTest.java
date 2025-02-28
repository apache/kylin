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

import static org.apache.kylin.metadata.query.QueryMetrics.QUERY_RESPONSE_TIME;
import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDaoTest.createQueryMetrics;
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

public class QueryHistorySchedulerTest extends NLocalFileMetadataTestCase {

    QueryHistoryScheduler queryHistoryScheduler;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
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
        QueryMetrics queryMetrics = createQueryMetrics(1584888338274L, 5578L, true, "default", true);

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
        QueryMetrics queryMetrics = createQueryMetrics(1584888338274L, 5578L, true, "default", true);

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

    @Test
    public void testInsertAndUpdate() throws Exception {
        // product
        QueryMetrics queryMetrics = createQueryMetrics(1584888338274L, 5578L, true, "default", true);

        QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        Assert.assertEquals(2, queryHistoryScheduler.queryMetricsQueue.size());

        QueryMetrics queryMetricsUpdate = new QueryMetrics(queryMetrics.getQueryId());
        QueryHistoryInfo info = new QueryHistoryInfo();
        info.getTraces().add(new QueryHistoryInfo.QueryTraceSpan(QUERY_RESPONSE_TIME, null, 123));
        queryMetricsUpdate.setQueryHistoryInfo(info);
        queryMetricsUpdate.setUpdateMetrics(true);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetricsUpdate);
        Assert.assertEquals(3, queryHistoryScheduler.queryMetricsQueue.size());

        // consume
        queryHistoryScheduler.init();
        await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0, queryHistoryScheduler.queryMetricsQueue.size());
        });
    }
}
