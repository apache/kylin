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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.query.util.QueryInfoCollector;
import org.apache.kylin.rest.request.SQLRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class BadQueryDetector extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(BadQueryDetector.class);
    public static final int ONE_MB = 1024 * 1024;

    private final ConcurrentMap<Thread, Entry> runningQueries = Maps.newConcurrentMap();
    private final long detectionInterval;
    private final int alertMB;
    private final int alertRunningSec;
    private KylinConfig kylinConfig;
    private ArrayList<Notifier> notifiers = new ArrayList<>();
    private int queryTimeoutSeconds;

    public BadQueryDetector() {
        super("BadQueryDetector");
        this.setDaemon(true);
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.detectionInterval = kylinConfig.getBadQueryDefaultDetectIntervalSeconds() * 1000L;
        this.alertMB = 100;
        this.alertRunningSec = kylinConfig.getBadQueryDefaultAlertingSeconds();
        this.queryTimeoutSeconds = kylinConfig.getQueryTimeoutSeconds();

        initNotifiers();
    }

    public BadQueryDetector(long detectionInterval, int alertMB, int alertRunningSec, int queryTimeoutSeconds) {
        super("BadQueryDetector");
        this.setDaemon(true);
        this.detectionInterval = detectionInterval;
        this.alertMB = alertMB;
        this.alertRunningSec = alertRunningSec;
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.queryTimeoutSeconds = queryTimeoutSeconds;

        initNotifiers();
    }

    public static long getSystemAvailBytes() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
        long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
        long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
        long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
        long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used
        return availableMemory;
    }

    public static int getSystemAvailMB() {
        return (int) (getSystemAvailBytes() / ONE_MB);
    }

    private void initNotifiers() {
        this.notifiers.add(new LoggerNotifier());
        if (kylinConfig.getBadQueryPersistentEnabled()) {
            this.notifiers.add(new PersistenceNotifier());
        }
    }

    public void registerNotifier(Notifier notifier) {
        notifiers.add(notifier);
    }

    private void notify(String adj, Entry e) {
        float runningSec = (float) (System.currentTimeMillis() - e.startTime) / 1000;

        for (Notifier notifier : notifiers) {
            try {
                notifier.badQueryFound(adj, runningSec, //
                        e.startTime, e.sqlRequest.getProject(), e.sqlRequest.getSql(), e.user, e.thread, e.queryId, e.collector);
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
    }

    public void queryStart(Thread thread, SQLRequest sqlRequest, String user, String queryId) {
        runningQueries.put(thread, new Entry(sqlRequest, user, thread, queryId, QueryInfoCollector.current()));
    }

    public void queryEnd(Thread thread) {
        queryEnd(thread, null);
    }

    public void queryEnd(Thread thread, String badReason) {
        Entry entry = runningQueries.remove(thread);
        
        if (badReason != null)
            notify(badReason, entry);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(detectionInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // stop detection and exit
                return;
            }

            try {
                detectBadQuery();
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
    }

    private void detectBadQuery() {
        logger.debug("Detect bad query.");
        long now = System.currentTimeMillis();
        ArrayList<Entry> entries = new ArrayList<Entry>(runningQueries.values());
        Collections.sort(entries);

        // report if query running long
        for (Entry e : entries) {
            float runningSec = (float) (now - e.startTime) / 1000;
            setQueryThreadInterrupted(e, runningSec);

            if (runningSec >= alertRunningSec) {
                notify(BadQueryEntry.ADJ_SLOW, e);
                dumpStackTrace(e.thread, e.queryId);
            }
        }

        // report if low memory
        if (getSystemAvailMB() < alertMB) {
            logger.info("System free memory less than " + alertMB + " MB. " + entries.size() + " queries running.");
        }
    }

    private void setQueryThreadInterrupted(Entry e, float runningSec) {
        if (queryTimeoutSeconds != 0 && runningSec >= queryTimeoutSeconds) {
            e.thread.interrupt();
            logger.error("Query running "+ runningSec + "s, Trying to cancel query:" + e.thread.getName());
        }
    }

    // log the stack trace of bad query thread for further analysis
    private void dumpStackTrace(Thread t, String queryId) {
        int maxStackTraceDepth = kylinConfig.getBadQueryStackTraceDepth();
        int current = 0;

        StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder buf = new StringBuilder(
                "Problematic thread 0x" + Long.toHexString(t.getId()) + " " + t.getName() + ", query id: " + queryId);
        buf.append("\n");
        for (StackTraceElement e : stackTrace) {
            if (++current > maxStackTraceDepth) {
                break;
            }
            buf.append("\t").append("at ").append(e.toString()).append("\n");
        }
        logger.info(buf.toString());
    }

    public interface Notifier {
        void badQueryFound(String adj, float runningSec, long startTime, String project, String sql, String user,
                Thread t, String queryId, QueryInfoCollector collector);
    }

    private class LoggerNotifier implements Notifier {
        @Override
        public void badQueryFound(String adj, float runningSec, long startTime, String project, String sql, String user,
                Thread t, String queryId, QueryInfoCollector collector) {
            logger.info("{} query has been running {} seconds (project:{}, thread: 0x{}, user:{}, query id:{}) -- {}",
                    adj, runningSec, project, Long.toHexString(t.getId()), user, queryId, sql);
        }
    }

    private class PersistenceNotifier implements Notifier {
        BadQueryHistoryManager badQueryManager = BadQueryHistoryManager.getInstance(kylinConfig);
        String serverHostname;

        public PersistenceNotifier() {
            try {
                serverHostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                serverHostname = "Unknow";
                logger.warn("Error in get current hostname.", e);
            }
        }

        @Override
        public void badQueryFound(String adj, float runningSec, long startTime, String project, String sql, String user,
                Thread t, String queryId, QueryInfoCollector collector) {
            try {
                BadQueryEntry entry = new BadQueryEntry(sql, adj, startTime, runningSec, serverHostname, t.getName(),
                        user, queryId, collector.getCubeNameString());
                badQueryManager.upsertEntryToProject(entry, project);
            } catch (IOException e) {
                logger.error("Error in bad query persistence.", e);
            }
        }
    }

    private class Entry implements Comparable<Entry> {
        final SQLRequest sqlRequest;
        final long startTime;
        final Thread thread;
        final String user;
        final String queryId;
        final QueryInfoCollector collector;

        Entry(SQLRequest sqlRequest, String user, Thread thread, String queryId, QueryInfoCollector collector) {
            this.sqlRequest = sqlRequest;
            this.startTime = System.currentTimeMillis();
            this.thread = thread;
            this.user = user;
            this.queryId = queryId;
            this.collector = collector;
        }

        @Override
        public int compareTo(Entry o) {
            return (int) (this.startTime - o.startTime);
        }
    }

}
