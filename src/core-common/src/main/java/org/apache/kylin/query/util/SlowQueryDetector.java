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

package org.apache.kylin.query.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class SlowQueryDetector extends Thread {

    private static final Logger logger = LoggerFactory.getLogger("query");

    @Getter
    private static final ConcurrentHashMap<Thread, QueryEntry> runningQueries = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, CanceledSlowQueryStatus> canceledSlowQueriesStatus = Maps
            .newConcurrentMap();
    private final int detectionIntervalMs;
    private final int queryTimeoutMs;

    public SlowQueryDetector() {
        super("SlowQueryDetector");
        this.setDaemon(true);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        this.detectionIntervalMs = kylinConfig.getSlowQueryDefaultDetectIntervalSeconds() * 1000;
        this.queryTimeoutMs = kylinConfig.getQueryTimeoutSeconds() * 1000;
    }

    // just for test.
    public SlowQueryDetector(int detectionIntervalMs, int queryTimeoutMs) {
        super("SlowQueryDetector");
        this.setDaemon(true);
        this.detectionIntervalMs = detectionIntervalMs;
        this.queryTimeoutMs = queryTimeoutMs;
    }

    public static ConcurrentMap<String, CanceledSlowQueryStatus> getCanceledSlowQueriesStatus() {
        return canceledSlowQueriesStatus;
    }

    @VisibleForTesting
    public static void addCanceledSlowQueriesStatus(ConcurrentMap<String, CanceledSlowQueryStatus> slowQueriesStatus) {
        canceledSlowQueriesStatus.putAll(slowQueriesStatus);
    }

    @VisibleForTesting
    public static void clearCanceledSlowQueriesStatus() {
        canceledSlowQueriesStatus.clear();
    }

    public void queryStart(String stopId) {
        runningQueries.put(currentThread(), new QueryEntry(System.currentTimeMillis(), currentThread(),
                QueryContext.current().getQueryId(), QueryContext.current().getUserSQL(), stopId, false,
                QueryContext.current().getQueryTagInfo().isAsyncQuery(), null, CancelFlag.getContextCancelFlag()));
    }

    public void addJobIdForAsyncQueryJob(String jobId) {
        QueryEntry queryEntry = runningQueries.get(currentThread());
        if (queryEntry != null) {
            queryEntry.setJobId(jobId);
        }

    }

    public void stopQuery(String id) {
        for (SlowQueryDetector.QueryEntry e : SlowQueryDetector.getRunningQueries().values()) {
            if ((e.isAsyncQuery() && id.equals(e.getQueryId())) || (!e.isAsyncQuery() && id.equals(e.getStopId()))) {
                e.setStopByUser(true);
                doStopQuery(e);
                break;
            }
        }
    }

    private void doStopQuery(QueryEntry e) {
        if (e.getJobId() == null) {
            e.getPlannerCancelFlag().requestCancel();
            logger.error("Trying to cancel query: {}", e.getThread().getName());
            e.getThread().interrupt();
        } else {
            logger.error("Trying to cancel query job : {},{}", e.getThread().getName(), e.getJobId());
            EventBusFactory.getInstance().postSync(new CliCommandExecutor.JobKilled(e.getJobId()));
        }
    }

    public void queryEnd() {
        QueryEntry entry = runningQueries.remove(currentThread());
        if (null != entry && null != canceledSlowQueriesStatus.get(entry.queryId)) {
            canceledSlowQueriesStatus.remove(entry.queryId);
            logger.debug("Remove query [{}] from canceledSlowQueriesStatus", entry.queryId);
        }
    }

    @Override
    public void run() {
        while (true) {
            checkStopByUser();
            checkTimeout();
            try {
                Thread.sleep(detectionIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // stop detection and exit
                return;
            }
        }
    }

    private void checkStopByUser() {
        // interrupt query thread if Stop By User but running
        for (QueryEntry e : runningQueries.values()) {
            if (e.isStopByUser) {
                doStopQuery(e);
            }
        }
    }

    private void checkTimeout() {
        // interrupt query thread if timeout
        for (QueryEntry e : runningQueries.values()) {
            if (!e.setInterruptIfTimeout()) {
                continue;
            }

            try {
                CanceledSlowQueryStatus canceledSlowQueryStatus = canceledSlowQueriesStatus.get(e.getQueryId());
                if (null == canceledSlowQueryStatus) {
                    canceledSlowQueriesStatus.putIfAbsent(e.getQueryId(), new CanceledSlowQueryStatus(e.getQueryId(), 1,
                            System.currentTimeMillis(), e.getRunningTime()));
                    logger.debug("Query [{}] has been canceled 1 times, put to canceledSlowQueriesStatus", e.queryId);
                } else {
                    int canceledTimes = canceledSlowQueryStatus.getCanceledTimes() + 1;
                    canceledSlowQueriesStatus.put(e.getQueryId(), new CanceledSlowQueryStatus(e.getQueryId(),
                            canceledTimes, System.currentTimeMillis(), e.getRunningTime()));
                    logger.debug("Query [{}] has been canceled {} times", e.getQueryId(), canceledTimes);
                }
            } catch (Exception ex) {
                logger.error("Record slow query status failed!", ex);
            }
        }
    }

    @Getter
    @AllArgsConstructor
    public static class CanceledSlowQueryStatus {
        public final String queryId;
        public final int canceledTimes;
        public final long lastCanceledTime;
        public final float queryDurationTime;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class QueryEntry {
        final long startTime;
        final Thread thread;
        final String queryId;
        final String sql;
        final String stopId;
        boolean isStopByUser;
        final boolean isAsyncQuery;
        String jobId;
        final CancelFlag plannerCancelFlag;

        public long getRunningTime() {
            return (System.currentTimeMillis() - startTime) / 1000;
        }

        private boolean setInterruptIfTimeout() {
            if (isAsyncQuery) {
                return false;
            }
            long runningMs = System.currentTimeMillis() - startTime;
            if (runningMs >= queryTimeoutMs) {
                plannerCancelFlag.requestCancel();
                thread.interrupt();
                logger.error("Trying to cancel query: {}", thread.getName());
                return true;
            }

            return false;
        }
    }
}
