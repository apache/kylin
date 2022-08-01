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

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.query.util.SparkJobTrace;
import org.apache.kylin.query.util.SparkJobTraceMetric;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;

public class QueryHistoryScheduler {

    private static final Logger logger = LoggerFactory.getLogger("query");
    protected BlockingQueue<QueryMetrics> queryMetricsQueue;
    private ScheduledExecutorService writeQueryHistoryScheduler;

    private long sparkJobTraceTimeoutMs;
    private boolean isQuerySparkJobTraceEnabled;
    private boolean isSecondStorageQueryMetricCollect;

    public QueryHistoryScheduler() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        queryMetricsQueue = new LinkedBlockingQueue<>(kylinConfig.getQueryHistoryBufferSize());
        logger.debug("New NQueryHistoryScheduler created");
    }

    public static QueryHistoryScheduler getInstance() {
        return Singletons.getInstance(QueryHistoryScheduler.class);
    }

    public void init() throws Exception {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        sparkJobTraceTimeoutMs = kapConfig.getSparkJobTraceTimeoutMs();
        isQuerySparkJobTraceEnabled = kapConfig.isQuerySparkJobTraceEnabled();
        writeQueryHistoryScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("WriteQueryHistoryWorker"));
        KylinConfig kyinConfig = KylinConfig.getInstanceFromEnv();
        writeQueryHistoryScheduler.scheduleWithFixedDelay(new WriteQueryHistoryRunner(), 1,
                kyinConfig.getQueryHistorySchedulerInterval(), TimeUnit.SECONDS);
        isSecondStorageQueryMetricCollect = KylinConfig.getInstanceFromEnv().getSecondStorageQueryMetricCollect();
    }

    public void offerQueryHistoryQueue(QueryMetrics queryMetrics) {
        boolean offer = queryMetricsQueue.offer(queryMetrics);
        if (!offer) {
            logger.info("queryMetricsQueue is full");
        }
    }

    synchronized void shutdown() {
        logger.info("Shutting down NQueryHistoryScheduler ....");
        if (writeQueryHistoryScheduler != null) {
            ExecutorServiceUtil.forceShutdown(writeQueryHistoryScheduler);
        }
    }

    public class WriteQueryHistoryRunner implements Runnable {

        RDBMSQueryHistoryDAO queryHistoryDAO;

        WriteQueryHistoryRunner() {
            queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        }

        @Override
        public void run() {
            try {
                List<QueryMetrics> metrics = Lists.newArrayList();
                queryMetricsQueue.drainTo(metrics);
                List<QueryMetrics> insertMetrics;
                if (isQuerySparkJobTraceEnabled && metrics.size() > 0) {
                    insertMetrics = metrics.stream().filter(queryMetrics -> {
                        String queryId = queryMetrics.getQueryId();
                        SparkJobTraceMetric sparkJobTraceMetric = SparkJobTrace.getSparkJobTraceMetric(queryId);
                        return isCollectedFinished(queryId, sparkJobTraceMetric, queryMetrics);
                    }).collect(Collectors.toList());
                } else {
                    insertMetrics = metrics;
                }
                collectSecondStorageMetric(insertMetrics);
                queryHistoryDAO.insert(insertMetrics);
            } catch (Exception th) {
                logger.error("Error when write query history", th);
            }
        }

    }

    public boolean isCollectedFinished(String queryId, SparkJobTraceMetric sparkJobTraceMetric,
            QueryMetrics queryMetrics) {
        if (sparkJobTraceMetric != null) {
            // queryHistoryInfo collect asynchronous metrics
            List<QueryHistoryInfo.QueryTraceSpan> queryTraceSpans = queryMetrics.getQueryHistoryInfo().getTraces();
            AtomicLong timeCostSum = new AtomicLong(0);
            queryTraceSpans.forEach(span -> {
                if (QueryTrace.PREPARE_AND_SUBMIT_JOB.equals(span.getName())) {
                    span.setDuration(sparkJobTraceMetric.getPrepareAndSubmitJobMs());
                }
                timeCostSum.addAndGet(span.getDuration());
            });
            queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.WAIT_FOR_EXECUTION,
                    QueryTrace.SPAN_GROUPS.get(QueryTrace.WAIT_FOR_EXECUTION),
                    sparkJobTraceMetric.getWaitForExecutionMs()));
            timeCostSum.addAndGet(sparkJobTraceMetric.getWaitForExecutionMs());
            queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.EXECUTION,
                    QueryTrace.SPAN_GROUPS.get(QueryTrace.EXECUTION), sparkJobTraceMetric.getExecutionMs()));
            timeCostSum.addAndGet(sparkJobTraceMetric.getExecutionMs());
            queryTraceSpans.add(new QueryHistoryInfo.QueryTraceSpan(QueryTrace.FETCH_RESULT,
                    QueryTrace.SPAN_GROUPS.get(QueryTrace.FETCH_RESULT),
                    queryMetrics.getQueryDuration() - timeCostSum.get()));
            return true;
        } else if ((System.currentTimeMillis()
                - (queryMetrics.getQueryTime() + queryMetrics.getQueryDuration())) > sparkJobTraceTimeoutMs) {
            logger.warn(
                    "QueryMetrics timeout lost spark job trace kylin.query.spark-job-trace-timeout-ms={} queryId:{}",
                    sparkJobTraceTimeoutMs, queryId);
            return true;
        } else {
            offerQueryHistoryQueue(queryMetrics);
            return false;
        }
    }

    public void collectSecondStorageMetric(List<QueryMetrics> metrics) {
        if (!isSecondStorageQueryMetricCollect) {
            return;
        }

        if (!SecondStorageUtil.isGlobalEnable()) {
            return;
        }

        SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);

        for (QueryMetrics metric : metrics) {
            try {
                if (metric.isSecondStorage() && SecondStorageUtil.isProjectEnable(metric.getProjectName())) {
                    Map<String, Object> secondStorageMetrics = updater.getQueryMetric(metric.getProjectName(), metric.getQueryId());

                    if (secondStorageMetrics.containsKey(QueryMetrics.TOTAL_SCAN_BYTES)) {
                        metric.setTotalScanBytes((long) secondStorageMetrics.get(QueryMetrics.TOTAL_SCAN_BYTES));
                    }

                    if (secondStorageMetrics.containsKey(QueryMetrics.TOTAL_SCAN_COUNT)) {
                        metric.setTotalScanCount((long) secondStorageMetrics.get(QueryMetrics.TOTAL_SCAN_COUNT));
                    }
                }
            } catch (Exception e) {
                logger.error("Get tired storage metric fail. query_id: {}, message: {}", metric.getQueryId(), e.getMessage());
            }
        }
    }
}
