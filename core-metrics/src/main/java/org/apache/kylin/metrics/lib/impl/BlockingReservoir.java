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

package org.apache.kylin.metrics.lib.impl;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class BlockingReservoir extends AbstractActiveReservoir {

    private static final Logger logger = LoggerFactory.getLogger(BlockingReservoir.class);

    private final BlockingQueue<Record> recordsQueue;
    private final Thread scheduledReporter;
    private final int MIN_REPORT_SIZE;
    private final int MAX_REPORT_SIZE;
    private final long MAX_REPORT_TIME;
    private List<Record> records;

    public BlockingReservoir() {
        this(1, 100);
    }

    public BlockingReservoir(int minReportSize, int maxReportSize) {
        this(minReportSize, maxReportSize, 10);
    }

    public BlockingReservoir(int minReportSize, int maxReportSize, int MAX_REPORT_TIME) {
        this.MAX_REPORT_SIZE = maxReportSize;
        this.MIN_REPORT_SIZE = minReportSize;
        this.MAX_REPORT_TIME = MAX_REPORT_TIME * 60 * 1000L;

        this.recordsQueue = new LinkedBlockingQueue<>();
        this.listeners = Lists.newArrayList();

        this.records = Lists.newArrayListWithExpectedSize(MAX_REPORT_SIZE);

        scheduledReporter = new ThreadFactoryBuilder().setNameFormat("metrics-blocking-reservoir-scheduler-%d").build()
                .newThread(new ReporterRunnable());
    }

    public void update(Record record) {
        if (!isReady) {
            logger.info("Current reservoir is not ready for update record");
            return;
        }
        try {
            recordsQueue.put(record);
        } catch (InterruptedException e) {
            logger.warn("Thread is interrupted during putting value to blocking queue. \n" + e.toString());
        }
    }

    public int size() {
        return recordsQueue.size();
    }

    private void onRecordUpdate(boolean ifAll) {
        if (ifAll) {
            records = Lists.newArrayList();
            recordsQueue.drainTo(records);
        } else {
            records.clear();
            recordsQueue.drainTo(records, MAX_REPORT_SIZE);
        }

        boolean ifSucceed = true;
        for (ActiveReservoirListener listener : listeners) {
            if (!notifyListenerOfUpdatedRecord(listener, records)) {
                ifSucceed = false;
                logger.warn("It fails to notify listener " + listener.toString() + " of updated records "
                        + records.toString());
            }
        }
        if (!ifSucceed) {
            notifyListenerHAOfUpdatedRecord(records);
        }
    }

    private boolean notifyListenerOfUpdatedRecord(ActiveReservoirListener listener, List<Record> records) {
        return listener.onRecordUpdate(records);
    }

    private boolean notifyListenerHAOfUpdatedRecord(List<Record> records) {
        logger.info("The HA listener " + listenerHA.toString() + " for updated records " + records.toString()
                + " will be started");
        if (!notifyListenerOfUpdatedRecord(listenerHA, records)) {
            logger.error("The HA listener also fails!!!");
            return false;
        }
        return true;
    }

    public void start() {
        super.start();
        scheduledReporter.start();
    }

    public void stop() {
        super.stop();
        scheduledReporter.interrupt();
        try {
            scheduledReporter.join();
        } catch (InterruptedException e) {
            logger.warn("Interrupted during join");
            throw new RuntimeException(e);
        }
    }

    class ReporterRunnable implements Runnable {

        public void run() {
            long startTime = System.currentTimeMillis();
            while (isReady) {
                if (size() <= 0) {
                    logger.info("There's no record in the blocking queue.");
                    sleep();
                    startTime = System.currentTimeMillis();
                    continue;
                } else if (size() < MIN_REPORT_SIZE && (System.currentTimeMillis() - startTime < MAX_REPORT_TIME)) {
                    logger.info("The number of records in the blocking queue is less than " + MIN_REPORT_SIZE + //
                            " and the duration from last reporting is less than " + MAX_REPORT_TIME
                            + "ms. Will delay to report!");
                    sleep();
                    continue;
                }

                onRecordUpdate(false);
                startTime = System.currentTimeMillis();
            }
            onRecordUpdate(true);
            logger.info("Reporter finishes reporting metrics.");
        }

        private void sleep() {
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
                logger.warn("Interrupted during running");
            }
        }
    }
}
