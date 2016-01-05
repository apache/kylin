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

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.rest.request.SQLRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class BadQueryDetector extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(BadQueryDetector.class);

    private final ConcurrentMap<Thread, Entry> runningQueries = Maps.newConcurrentMap();
    private final long detectionInterval;
    private final int alertMB;
    private final int alertRunningSec;

    private ArrayList<Notifier> notifiers = new ArrayList<Notifier>();

    public BadQueryDetector() {
        this(60 * 1000, 100, 90); // 1 minute, 100 MB, 90 seconds
    }

    public BadQueryDetector(long detectionInterval, int alertMB, int alertRunningSec) {
        super("BadQueryDetector");
        this.setDaemon(true);
        this.detectionInterval = detectionInterval;
        this.alertMB = alertMB;
        this.alertRunningSec = alertRunningSec;

        this.notifiers.add(new Notifier() {
            @Override
            public void badQueryFound(String adj, int runningSec, String sql, Thread t) {
                logger.info(adj + " query has been running " + runningSec + " seconds (thread id 0x" + Long.toHexString(t.getId()) + ") -- " + sql);
            }
        });
    }

    public void registerNotifier(Notifier notifier) {
        notifiers.add(notifier);
    }

    private void notify(String adj, int runningSec, String sql, Thread t) {
        for (Notifier notifier : notifiers) {
            try {
                notifier.badQueryFound(adj, runningSec, sql, t);
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    public interface Notifier {
        void badQueryFound(String adj, int runningSec, String sql, Thread t);
    }

    public void queryStart(Thread thread, SQLRequest sqlRequest) {
        runningQueries.put(thread, new Entry(sqlRequest, thread));
    }

    public void queryEnd(Thread thread) {
        runningQueries.remove(thread);
    }

    private class Entry implements Comparable<Entry> {
        final SQLRequest sqlRequest;
        final long startTime;
        final Thread thread;

        Entry(SQLRequest sqlRequest, Thread thread) {
            this.sqlRequest = sqlRequest;
            this.startTime = System.currentTimeMillis();
            this.thread = thread;
        }

        @Override
        public int compareTo(Entry o) {
            return (int) (this.startTime - o.startTime);
        }
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
            int runningSec = (int) ((now - e.startTime) / 1000);
            if (runningSec >= alertRunningSec) {
                notify("Slow", runningSec, e.sqlRequest.getSql(), e.thread);
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
        StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder buf = new StringBuilder("Problematic thread 0x" + Long.toHexString(t.getId()));
        buf.append("\n");
        for (StackTraceElement e : stackTrace) {
            buf.append("\t").append("at ").append(e.toString()).append("\n");
        }
        logger.info(buf.toString());
    }

    public static final int ONE_MB = 1024 * 1024;

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

}
