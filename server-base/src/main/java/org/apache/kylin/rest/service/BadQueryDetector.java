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
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.rest.request.SQLRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class BadQueryDetector extends Thread {

    public static final int ONE_MB = 1024 * 1024;
    private static final Logger logger = LoggerFactory.getLogger(BadQueryDetector.class);
    private final ConcurrentMap<Thread, Entry> runningQueries = Maps.newConcurrentMap();
    private final long detectionInterval;
    private final int alertMB;
    private final int alertRunningSec;
    private KylinConfig kylinConfig;
    private ArrayList<Notifier> notifiers = new ArrayList<Notifier>();

    public BadQueryDetector() {
        super("BadQueryDetector");
        this.setDaemon(true);
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.detectionInterval = kylinConfig.getBadQueryDefaultDetectIntervalSeconds() * 1000;
        this.alertMB = 100;
        this.alertRunningSec = kylinConfig.getBadQueryDefaultAlertingSeconds();

        initNotifiers();
    }

    public BadQueryDetector(long detectionInterval, int alertMB, int alertRunningSec) {
        super("BadQueryDetector");
        this.setDaemon(true);
        this.detectionInterval = detectionInterval;
        this.alertMB = alertMB;
        this.alertRunningSec = alertRunningSec;
        this.kylinConfig = KylinConfig.getInstanceFromEnv();

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

    private void notify(String adj, float runningSec, long startTime, String project, String sql, String user, Thread t) {
        for (Notifier notifier : notifiers) {
            try {
                notifier.badQueryFound(adj, runningSec, startTime, project, sql, user, t);
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    public void queryStart(Thread thread, SQLRequest sqlRequest, String user) {
        runningQueries.put(thread, new Entry(sqlRequest, user, thread));
    }

    public void queryEnd(Thread thread) {
        runningQueries.remove(thread);
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(detectionInterval);
            } catch (InterruptedException e) {
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
        long now = System.currentTimeMillis();
        ArrayList<Entry> entries = new ArrayList<Entry>(runningQueries.values());
        Collections.sort(entries);

        // report if query running long
        for (Entry e : entries) {
            float runningSec = (float) (now - e.startTime) / 1000;
            if (runningSec >= alertRunningSec) {
                notify("Slow", runningSec, e.startTime, e.sqlRequest.getProject(), e.sqlRequest.getSql(), e.user, e.thread);
                dumpStackTrace(e.thread);
            } else {
                break; // entries are sorted by startTime
            }
        }

        // report if low memory
        if (getSystemAvailMB() < alertMB) {
            logger.info("System free memory less than " + alertMB + " MB. " + entries.size() + " queries running.");
        }
    }

    // log the stack trace of bad query thread for further analysis
    private void dumpStackTrace(Thread t) {
        int maxStackTraceDepth = kylinConfig.getBadQueryStackTraceDepth();
        int current = 0;

        StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder buf = new StringBuilder("Problematic thread 0x" + Long.toHexString(t.getId()));
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
        void badQueryFound(String adj, float runningSec, long startTime, String project, String sql, String user, Thread t);
    }

    private class LoggerNotifier implements Notifier {
        @Override
        public void badQueryFound(String adj, float runningSec, long startTime, String project, String sql, String user, Thread t) {
            logger.info("{} query has been running {} seconds (project:{}, thread: 0x{}, user:{}) -- {}", adj, runningSec, project, Long.toHexString(t.getId()), user, sql);
        }
    }

    private class PersistenceNotifier implements Notifier {
        BadQueryHistoryManager badQueryManager = BadQueryHistoryManager.getInstance(kylinConfig);
        String serverHostname;
        NavigableSet<Pair<Long, String>> cacheQueue = new TreeSet<>(new Comparator<Pair<Long, String>>() {
            @Override
            public int compare(Pair<Long, String> o1, Pair<Long, String> o2) {
                if (o1.equals(o2)) {
                    return 0;
                } else if (o1.getFirst().equals(o2.getFirst())) {
                    return o2.getSecond().compareTo(o2.getSecond());
                } else {
                    return (int) (o1.getFirst() - o2.getFirst());
                }
            }
        });

        public PersistenceNotifier() {
            try {
                serverHostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                serverHostname = "Unknow";
                logger.warn("Error in get current hostname.", e);
            }
        }

        @Override
        public void badQueryFound(String adj, float runningSec, long startTime, String project, String sql, String user, Thread t) {
            try {
                long cachingSeconds = (kylinConfig.getBadQueryDefaultAlertingSeconds() + 1) * 30;
                Pair<Long, String> sqlPair = new Pair<>(startTime, sql);
                if (!cacheQueue.contains(sqlPair)) {
                    badQueryManager.addEntryToProject(sql, startTime, adj, runningSec, serverHostname, t.getName(), user, project);
                    cacheQueue.add(sqlPair);
                    while (!cacheQueue.isEmpty() && (System.currentTimeMillis() - cacheQueue.first().getFirst() > cachingSeconds * 1000 || cacheQueue.size() > kylinConfig.getBadQueryHistoryNum() * 3)) {
                        cacheQueue.pollFirst();
                    }
                } else {
                    badQueryManager.updateEntryToProject(sql, startTime, adj, runningSec, serverHostname, t.getName(), user, project);
                }
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

        Entry(SQLRequest sqlRequest, String user, Thread thread) {
            this.sqlRequest = sqlRequest;
            this.startTime = System.currentTimeMillis();
            this.thread = thread;
            this.user = user;
        }

        @Override
        public int compareTo(Entry o) {
            return (int) (this.startTime - o.startTime);
        }
    }

}
