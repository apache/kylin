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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.query.util.SparkJobTrace;
import org.apache.kylin.query.util.SparkJobTraceMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHistoryScheduler {

    private static final Logger logger = LoggerFactory.getLogger("query");
    protected BlockingQueue<QueryMetrics> queryMetricsQueue;
    private ScheduledExecutorService writeQueryHistoryScheduler;

    private long sparkJobTraceTimeoutMs;
    private boolean isQuerySparkJobTraceEnabled;

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
                    insertMetrics = metrics.stream().filter(qm -> !qm.isUpdateMetrics()).filter(queryMetrics -> {
                        String queryId = queryMetrics.getQueryId();
                        SparkJobTraceMetric sparkJobTraceMetric = SparkJobTrace.getSparkJobTraceMetric(queryId);
                        return isCollectedFinished(queryId, sparkJobTraceMetric, queryMetrics);
                    }).collect(Collectors.toList());
                } else {
                    insertMetrics = metrics.stream().filter(qm -> !qm.isUpdateMetrics()).collect(Collectors.toList());
                }
                if (CollectionUtils.isNotEmpty(insertMetrics)) {
                    queryHistoryDAO.insert(insertMetrics);
                }
                List<QueryMetrics> updateMetrics = metrics.stream().filter(QueryMetrics::isUpdateMetrics)
                        .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(updateMetrics)) {
                    updateQueryMetricsTrace(updateMetrics);
                }
            } catch (Exception th) {
                logger.error("Error when write query history", th);
            }
        }

        private void updateQueryMetricsTrace(List<QueryMetrics> updateMetrics) {
            List<String> queryIds = updateMetrics.stream().map(QueryMetrics::getQueryId).collect(Collectors.toList());
            Map<String, List<QueryMetrics>> metricsMap = updateMetrics.stream()
                    .collect(Collectors.groupingBy(QueryMetrics::getQueryId));

            List<QueryHistory> queryHistories = queryHistoryDAO.getByQueryIds(queryIds);

            List<Pair<Long, QueryHistoryInfo>> idToQHInfoList = queryHistories.stream().map(hs -> {
                Optional<QueryHistoryInfo.QueryTraceSpan> maxQueryTraceSpan = metricsMap.get(hs.getQueryId()).stream()
                        .flatMap(qm -> qm.getQueryHistoryInfo().getTraces().stream())
                        .filter(span -> QUERY_RESPONSE_TIME.equals(span.getName()))
                        .max(Comparator.comparingLong(QueryHistoryInfo.QueryTraceSpan::getDuration));

                if (maxQueryTraceSpan.isPresent()) {
                    QueryHistoryInfo.QueryTraceSpan trace = maxQueryTraceSpan.get();
                    QueryHistoryInfo qhi = hs.getQueryHistoryInfo();
                    qhi.getTraces().add(trace);
                    return new Pair<>(hs.getId(), qhi);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());

            queryHistoryDAO.batchUpdateQueryHistoriesInfo(idToQHInfoList);
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
}
