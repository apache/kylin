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

package org.apache.kylin.stream.core.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.metrics.StreamingMetrics;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.model.stats.ConsumerStats;
import org.apache.kylin.stream.core.model.stats.PartitionConsumeStats;
import org.apache.kylin.stream.core.source.MessageFormatException;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class StreamingConsumerChannel implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(StreamingConsumerChannel.class);
    protected volatile boolean stopped;
    protected volatile boolean paused;
    protected volatile boolean hasStoppedConsuming;
    private volatile CountDownLatch pauseLatch;
    private volatile CountDownLatch stopLatch;

    private Thread consumerThread;
    private String cubeName;
    private IStreamingConnector connector;
    private StreamingSegmentManager cubeSegmentManager;
    private volatile IStopConsumptionCondition stopCondition;
    private volatile long minAcceptEventTime;
    private AtomicLong parseEventErrorCnt = new AtomicLong(0);
    private AtomicLong addEventErrorCnt = new AtomicLong(0);
    private AtomicLong incomingEventCnt = new AtomicLong(0);
    private AtomicLong dropEventCnt = new AtomicLong(0);
    private Map<Integer, Meter> eventConsumeMeters;

    public StreamingConsumerChannel(String cubeName, IStreamingConnector connector,
            StreamingSegmentManager cubeSegmentManager, IStopConsumptionCondition stopCondition) {
        this.connector = connector;
        this.cubeName = cubeName;
        this.cubeSegmentManager = cubeSegmentManager;
        this.stopCondition = stopCondition;
        this.stopLatch = new CountDownLatch(1);
        this.eventConsumeMeters = Maps.newHashMap();
        this.minAcceptEventTime = 0;
    }

    public void setStopCondition(IStopConsumptionCondition stopCondition) {
        this.stopCondition = stopCondition;
        this.stopCondition.init(connector.getConsumePartitions());
    }

    public void setMinAcceptEventTime(long minAcceptEventTime) {
        this.minAcceptEventTime = minAcceptEventTime;
    }

    public void start() {
        this.consumerThread = new Thread(this, cubeName + "_channel");
        consumerThread.setPriority(Thread.MAX_PRIORITY); // Improve the priority of consumer thread to make ingest rate stable
        connector.open();
        consumerThread.start();
    }

    @Override
    public void run() {
        while (!stopped) {
            if (!paused) {
                StreamingMessage event = null;
                try {
                    event = connector.nextEvent();
                    if (event == null) {
                        Thread.sleep(100);
                        continue;
                    }
                    incomingEventCnt.incrementAndGet();
                    recordConsumeMetric(event.getSourcePosition().getPartition(), event.getParams());
                    if (!stopCondition.isSatisfied(event)) {
                        if (!isFilter(event)) {
                            cubeSegmentManager.addEvent(event);
                        }
                    } else {
                        logger.warn("The latest event trigger stopCondition, event = " + event);
                        this.stopped = true;
                    }
                } catch (MessageFormatException mfe) {
                    long countValue = parseEventErrorCnt.incrementAndGet();
                    if (countValue % 1000 < 3) {
                        logger.error(mfe.getMessage(), mfe);
                    }
                } catch (InterruptedException ie) {
                    logger.warn("interrupted!");
                    stopped = true;
                } catch (Exception e) {
                    long countValue = addEventErrorCnt.incrementAndGet();
                    if (countValue % 1000 < 3) {
                        logger.error("error happens when save event:" + event, e);
                    }
                }
            } else {
                try {
                    if (pauseLatch != null) {
                        pauseLatch.countDown();
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.warn("interrupted!");
                    stopped = true;
                }
            }
        }
        hasStoppedConsuming = true;
        logger.warn("Exit from main event loop, start to close cubeSegmentManager.");
        try {
            cubeSegmentManager.close();
            removeMetrics();
        } finally {
            connector.close();
            stopLatch.countDown();
        }
    }

    private void removeMetrics() {
        for (Map.Entry<Integer, Meter> meterEntry : eventConsumeMeters.entrySet()) {
            StreamingMetrics.getInstance().getMetricRegistry().remove(MetricRegistry
                    .name(StreamingMetrics.CONSUME_RATE_PFX, cubeName, String.valueOf(meterEntry.getKey())));
        }
    }

    protected void recordConsumeMetric(Integer partitionId, Map<String, Object> eventParams) {
        Meter partitionEventConsumeMeter = eventConsumeMeters.get(partitionId);
        if (partitionEventConsumeMeter == null) {
            partitionEventConsumeMeter = StreamingMetrics.newMeter(
                    MetricRegistry.name(StreamingMetrics.CONSUME_RATE_PFX, cubeName, String.valueOf(partitionId)));
            eventConsumeMeters.put(partitionId, partitionEventConsumeMeter);
        }
        partitionEventConsumeMeter.mark();
    }

    private boolean isFilter(StreamingMessage event) {
        if (minAcceptEventTime != 0 && event.getTimestamp() < minAcceptEventTime) {
            long countValue = dropEventCnt.incrementAndGet();
            // drop events is less than the min accepted event time
            if (countValue % 1000 <= 1) {
                logger.warn("event dropped, event time {}, min event accept time {}", event.getTimestamp(),
                        minAcceptEventTime);
            }
            return true;
        }

        if (event.isFiltered()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Called by another thread.
     */
    public void stop(long timeoutInMs) {
        this.stopped = true;
        waitConsumerStop(timeoutInMs);
    }

    private void waitConsumerStop(long timeoutInMs) {
        try {
            boolean stoppedSucceed = stopLatch.await(timeoutInMs, TimeUnit.MILLISECONDS);
            if (stoppedSucceed) {
                return;
            }
            if (!hasStoppedConsuming) {
                logger.warn(
                        "Consumer not stopped normally, close it forcedly, but the thread may not be stopped correctly");
                connector.wakeup();
            }
            stoppedSucceed = stopLatch.await(timeoutInMs, TimeUnit.MILLISECONDS);
            if (stoppedSucceed) {
                return;
            }
            if (hasStoppedConsuming) {
                logger.warn("Consumer has been stopped, but cube data store is not closed");
            } else {
                logger.warn("Consumer is still not stopped");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted!", e);
            Thread.interrupted();
        } catch (Exception e) {
            logger.error("Exception throws when wait consumer stopped", e);
        }
    }

    /**
     * Called by another thread.
     */
    public void pause(boolean wait) {
        this.paused = true;
        if (wait) {
            this.pauseLatch = new CountDownLatch(1);
            try {
                pauseLatch.await(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);
                Thread.interrupted();
            }
        }
    }

    /**
     * Called by another thread.
     */
    public void resumeToStopCondition(IStopConsumptionCondition newStopCondition) {
        this.paused = false;
        if (newStopCondition != IStopConsumptionCondition.NEVER_STOP) {
            setStopCondition(newStopCondition);
            try {
                boolean stoppedSucceed = stopLatch.await(120 * 1000L, TimeUnit.MILLISECONDS);
                if (!stoppedSucceed) {
                    throw new StreamingException("consumer stop failed for stopCondition:" + newStopCondition);
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);
            }
        }
    }

    /**
     * Called by another thread.
     */
    public void resume() {
        this.paused = false;
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isPaused() {
        return paused;
    }

    public String getSourceConsumeInfo() {
        return cubeSegmentManager.getConsumePositionStr();
    }

    public ConsumerStats getConsumerStats() {
        Map<Integer, Long> consumeLagMap = connector.getSource().calConsumeLag(cubeName,
                cubeSegmentManager.getConsumePosition());
        long totalLag = 0L;
        Map<Integer, PartitionConsumeStats> partitionConsumeStatsMap = Maps.newHashMap();
        for (Map.Entry<Integer, Meter> meterEntry : eventConsumeMeters.entrySet()) {
            Meter meter = meterEntry.getValue();
            PartitionConsumeStats stats = new PartitionConsumeStats();
            stats.setAvgRate(meter.getMeanRate());
            stats.setOneMinRate(meter.getOneMinuteRate());
            stats.setFiveMinRate(meter.getFiveMinuteRate());
            stats.setFifteenMinRate(meter.getFifteenMinuteRate());
            stats.setTotalConsume(meter.getCount());
            long thisLag = consumeLagMap.getOrDefault(meterEntry.getKey(), 0L);
            totalLag += thisLag;
            stats.setConsumeLag(thisLag);
            partitionConsumeStatsMap.put(meterEntry.getKey(), stats);
        }

        List<Partition> allPartitions = getConsumePartitions();
        for (Partition partition : allPartitions) {
            if (!partitionConsumeStatsMap.containsKey(partition.getPartitionId())) {
                partitionConsumeStatsMap.put(partition.getPartitionId(), new PartitionConsumeStats());
            }
        }
        ConsumerStats stats = new ConsumerStats();
        stats.setStopped(stopped);
        stats.setPaused(paused);
        stats.setTotalIncomingEvents(incomingEventCnt.get());
        stats.setTotalExceptionEvents(parseEventErrorCnt.get() + addEventErrorCnt.get());
        stats.setPartitionConsumeStatsMap(partitionConsumeStatsMap);
        stats.setConsumeOffsetInfo(getSourceConsumeInfo());
        stats.setConsumeLag(totalLag);
        return stats;
    }

    public IStreamingConnector getConnector() {
        return connector;
    }

    public List<Partition> getConsumePartitions() {
        return getConnector().getConsumePartitions();
    }
}
