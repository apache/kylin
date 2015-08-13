/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class StreamBuilder implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

    private StreamParser streamParser = StringStreamParser.instance;

    private final List<BlockingQueue<StreamMessage>> streamMessageQueues;

    private final MicroStreamBatchConsumer consumer;

    private final String streaming;

    private final long startTimestamp;

    private final long batchInterval;

    private final int batchSize;

    public static final StreamBuilder newPeriodicalStreamBuilder(String streaming, List<BlockingQueue<StreamMessage>> inputs, MicroStreamBatchConsumer consumer, long startTimestamp, long batchInterval) {
        return new StreamBuilder(streaming, inputs, consumer, startTimestamp, batchInterval);
    }

    public static final StreamBuilder newLimitedSizeStreamBuilder(String streaming, BlockingQueue<StreamMessage> input, MicroStreamBatchConsumer consumer, long startTimestamp, int batchSize) {
        return new StreamBuilder(streaming, input, consumer, startTimestamp, batchSize);
    }

    private StreamBuilder(String streaming, List<BlockingQueue<StreamMessage>> inputs, MicroStreamBatchConsumer consumer, long startTimestamp, long batchInterval) {
        Preconditions.checkArgument(inputs.size() > 0);
        this.streaming = streaming;
        this.streamMessageQueues = Lists.newArrayList();
        this.consumer = Preconditions.checkNotNull(consumer);
        this.startTimestamp = startTimestamp;
        this.batchInterval = batchInterval;
        this.batchSize = -1;
        init(inputs);
    }

    private StreamBuilder(String streaming, BlockingQueue<StreamMessage> input, MicroStreamBatchConsumer consumer, long startTimestamp, int batchSize) {
        this.streaming = streaming;
        this.streamMessageQueues = Lists.newArrayList();
        this.consumer = Preconditions.checkNotNull(consumer);
        this.startTimestamp = startTimestamp;
        this.batchInterval = -1L;
        this.batchSize = batchSize;
        init(Preconditions.checkNotNull(input));
    }

    private void init(BlockingQueue<StreamMessage> input) {
        this.streamMessageQueues.add(input);
    }

    private void init(List<BlockingQueue<StreamMessage>> inputs) {
        this.streamMessageQueues.addAll(inputs);
    }

    private BatchCondition generateBatchCondition(long startTimestamp) {
        if (batchInterval > 0) {
            return new TimePeriodCondition(startTimestamp, startTimestamp + batchInterval);
        } else {
            return new LimitedSizeCondition(batchSize);
        }
    }

    @Override
    public void run() {
        try {
            final int inputCount = streamMessageQueues.size();
            final ExecutorService executorService = Executors.newFixedThreadPool(inputCount);
            long start = startTimestamp;
            List<Integer> partitions = Lists.newArrayList();
            for (int i = 0, partitionCount = streamMessageQueues.size(); i < partitionCount; i++) {
                partitions.add(i);
            }
            while (true) {
                CountDownLatch countDownLatch = new CountDownLatch(inputCount);
                ArrayList<Future<MicroStreamBatch>> futures = Lists.newArrayListWithExpectedSize(inputCount);
                int partitionId = 0;
                for (BlockingQueue<StreamMessage> streamMessageQueue : streamMessageQueues) {
                    futures.add(executorService.submit(new StreamFetcher(partitionId++, streamMessageQueue, countDownLatch, generateBatchCondition(start), getStreamParser())));
                }
                countDownLatch.await();
                ArrayList<MicroStreamBatch> batches = Lists.newArrayListWithExpectedSize(inputCount);
                for (Future<MicroStreamBatch> future : futures) {
                    if (future.get() != null) {
                        batches.add(future.get());
                    } else {
                        //EOF occurs, stop consumer
                        consumer.stop();
                        return;
                    }
                }
                MicroStreamBatch batch = batches.get(0);
                if (batches.size() > 1) {
                    for (int i = 1; i < inputCount; i++) {
                        if (batches.get(i).size() > 0) {
                            batch = MicroStreamBatch.union(batch, batches.get(i));
                        }
                    }
                }
                if (batches.size() > 1) {
                    batch.getTimestamp().setFirst(start);
                    batch.getTimestamp().setSecond(start + batchInterval);
                }
                if (batchInterval > 0) {
                    start += batchInterval;
                }

                if (batch.size() == 0) {
                    logger.info("nothing to build, skip to next iteration after sleeping 10s");
                    Thread.sleep(10000);
                    continue;
                } else {
                    logger.info("Consuming {} messages, covering from {} to {}", new String[] { String.valueOf(batch.size()), DateFormat.formatToTimeStr(batch.getTimestamp().getFirst()), DateFormat.formatToTimeStr(batch.getTimestamp().getSecond()) });
                    long startTime = System.currentTimeMillis();
                    consumer.consume(batch);
                    logger.info("Batch build costs {} milliseconds", System.currentTimeMillis() - startTime);
                    if (batches.size() > 1) {
                        final HashMap<Integer, Long> offset = Maps.newHashMap();
                        for (MicroStreamBatch microStreamBatch : batches) {
                            offset.put(microStreamBatch.getPartitionId(), microStreamBatch.getOffset().getSecond());
                        }
                        StreamingManager.getInstance(KylinConfig.getInstanceFromEnv()).updateOffset(streaming, offset);
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("stream fetcher thread should not be interrupted", e);
        } catch (ExecutionException e) {
            logger.error("stream fetch thread encountered exception", e);
            throw new RuntimeException("stream fetch thread encountered exception", e);
        } catch (Exception e) {
            logger.error("consumer encountered exception", e);
            throw new RuntimeException("consumer encountered exception", e);
        }
    }

    public final StreamParser getStreamParser() {
        return streamParser;
    }

    public final void setStreamParser(StreamParser streamParser) {
        this.streamParser = streamParser;
    }

}
