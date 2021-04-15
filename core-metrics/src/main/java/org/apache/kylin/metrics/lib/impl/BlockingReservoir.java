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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A Reservoir which staged metrics message in memory, and emit them in fixed rate.
 * This will help to reduce pressure to underlying resource.
 */
public class BlockingReservoir extends AbstractActiveReservoir {

    private static final Logger logger = LoggerFactory.getLogger(BlockingReservoir.class);
    private static final int MAX_QUEUE_SIZE = 500000;

    /**
     * Cache for metrics message with max size is maxReportSize
     */
    private final BlockingQueue<Record> recordsQueue;
    private final Thread scheduledReporter;
    private final int minReportSize;
    private final int maxReportSize;
    private final long maxReportTime;
    private List<Record> records;

    public BlockingReservoir() {
        this(100, 500);
    }

    public BlockingReservoir(int minReportSize, int maxReportSize) {
        this(minReportSize, maxReportSize, 10);
    }

    public BlockingReservoir(int minReportSize, int maxReportSize, int maxReportTime) {
        this(minReportSize, maxReportSize, maxReportTime, MAX_QUEUE_SIZE);
    }

    public BlockingReservoir(int minReportSize, int maxReportSize, int maxReportTime, int maxQueueSize) {
        Preconditions.checkArgument(minReportSize > 0, "minReportSize should be larger than 0");
        Preconditions.checkArgument(maxReportSize >= minReportSize,
                "maxReportSize should not be less than minBatchSize");
        Preconditions.checkArgument(maxReportTime > 0, "maxReportTime should be larger than 0");
        this.maxReportSize = maxReportSize;
        this.maxReportTime = maxReportTime * 60 * 1000L;

        this.recordsQueue = maxQueueSize <= 0 ? new LinkedBlockingQueue<>() : new LinkedBlockingQueue<>(maxQueueSize);
        this.minReportSize = minReportSize;
        this.listeners = Lists.newArrayList();

        this.records = Lists.newArrayListWithExpectedSize(this.maxReportSize);
        scheduledReporter = new ThreadFactoryBuilder().setNameFormat("metrics-blocking-reservoir-scheduler-%d").build()
                .newThread(new ReporterRunnable());
    }

    /**
     * put record into queue but wait if queue is full
     */
    public void update(Record record) {
        if (!isReady) {
            logger.info("Current reservoir is not ready for update record");
            return;
        }
        try {
            recordsQueue.put(record);
        } catch (InterruptedException e) {
            logger.warn("Thread is interrupted during putting value to blocking queue.", e);
        } catch (IllegalArgumentException e) {
            logger.warn("The record queue may be full");
        }
    }

    public int size() {
        return recordsQueue.size();
    }

    private void onRecordUpdate(boolean ifAll) {
        if (ifAll) {
            records = Lists.newArrayList();
            recordsQueue.drainTo(records);
            logger.info("Will report {} metrics records", records.size());
        } else {
            records.clear();
            recordsQueue.drainTo(records, maxReportSize);
            logger.info("Will report {} metrics records, remaining {} records", records.size(), size());
        }

        boolean ifSucceed = true;
        for (ActiveReservoirListener listener : listeners) {
            if (!notifyListenerOfUpdatedRecord(listener, records)) {
                ifSucceed = false;
                logger.warn("It fails to notify listener " + listener.toString() + " of updated record size "
                        + records.size());
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
        logger.info("The HA listener " + listenerHA.toString() + " for updated record size " + records.size()
                + " will be started");
        if (!notifyListenerOfUpdatedRecord(listenerHA, records)) {
            logger.error("The HA listener also fails!!!");
            return false;
        }
        return true;
    }

    @VisibleForTesting
    void notifyUpdate() {
        onRecordUpdate(false);
    }

    @VisibleForTesting
    void setReady() {
        super.start();
    }

    public void start() {
        setReady();
        scheduledReporter.start();
    }

    @Override
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

    /**
     * A thread which try to check if staged message queue has meet size threshold and wait duration threshold
     * and notify listener every one minute
     */
    class ReporterRunnable implements Runnable {

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            while (isReady) {
                if (size() <= 0) {
                    logger.info("There's no record in the blocking queue.");
                    sleep();
                    startTime = System.currentTimeMillis();
                    continue;
                } else if (size() < minReportSize && (System.currentTimeMillis() - startTime < maxReportTime)) {
                    logger.info(
                            "The number of records in the blocking queue is less than {} and "
                                    + "the duration from last reporting is less than {} ms. " + "Will delay to report!",
                            minReportSize, maxReportTime);
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
                Thread.sleep(60 * 1000L);
            } catch (InterruptedException e) {
                logger.warn("Interrupted during running");
            }
        }
    }
}
