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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

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

    public static final StreamBuilder newPeriodicalStreamBuilder(String streaming,
                                                                 List<BlockingQueue<StreamMessage>> inputs,
                                                                 MicroStreamBatchConsumer consumer,
                                                                 long startTimestamp,
                                                                 long batchInterval) {
        return new StreamBuilder(streaming, inputs, consumer, startTimestamp, batchInterval);
    }

    public static final StreamBuilder newLimitedSizeStreamBuilder(String streaming,
                                                                 BlockingQueue<StreamMessage> input,
                                                                 MicroStreamBatchConsumer consumer,
                                                                 long startTimestamp,
                                                                 int batchSize) {
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
                    futures.add(executorService.submit(new StreamFetcher(partitionId++, streamMessageQueue, countDownLatch, generateBatchCondition(start))));
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
                        if (batches.get(i).size() > 0)
                            batch = MicroStreamBatch.union(batch, batches.get(i));
                    }
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

    private class StreamFetcher implements Callable<MicroStreamBatch> {

        private final BlockingQueue<StreamMessage> streamMessageQueue;
        private final CountDownLatch countDownLatch;
        private final int partitionId;
        private final BatchCondition condition;

        public StreamFetcher(int partitionId, BlockingQueue<StreamMessage> streamMessageQueue, CountDownLatch countDownLatch, BatchCondition condition) {
            this.partitionId = partitionId;
            this.streamMessageQueue = streamMessageQueue;
            this.countDownLatch = countDownLatch;
            this.condition = condition;
        }

        private void clearCounter() {
        }

        private StreamMessage peek(BlockingQueue<StreamMessage> queue, long timeout) {
            long t = System.currentTimeMillis();
            while (true) {
                final StreamMessage peek = queue.peek();
                if (peek != null) {
                    return peek;
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.warn("stream queue should not be interrupted", e);
                        return null;
                    }
                    if (System.currentTimeMillis() - t > timeout) {
                        break;
                    }
                }
            }
            return queue.peek();
        }

        @Override
        public MicroStreamBatch call() throws Exception {
            try {
                MicroStreamBatch microStreamBatch = null;
                while (true) {
                    if (microStreamBatch == null) {
                        microStreamBatch = new MicroStreamBatch(partitionId);
                        clearCounter();
                    }
                    StreamMessage streamMessage = peek(streamMessageQueue, 30000);
                    if (streamMessage == null) {
                        logger.info("The stream queue is drained, current available stream count: " + microStreamBatch.size());
                        if (!microStreamBatch.isEmpty()) {
                            return microStreamBatch;
                        } else {
                            continue;
                        }
                    }
                    if (streamMessage.getOffset() < 0) {
                        consumer.stop();
                        logger.warn("streaming encountered EOF, stop building");
                        return null;
                    }

                    microStreamBatch.incRawMessageCount();
                    final ParsedStreamMessage parsedStreamMessage = getStreamParser().parse(streamMessage);
                    if (parsedStreamMessage == null) {
                        throw new RuntimeException("parsedStreamMessage of " + new String(streamMessage.getRawData()) + " is null");
                    }

                    final BatchCondition.Result result = condition.apply(parsedStreamMessage);
                    if (parsedStreamMessage.isAccepted()) {
                        if (result == BatchCondition.Result.ACCEPT) {
                            streamMessageQueue.take();
                            microStreamBatch.add(parsedStreamMessage);
                        } else if (result == BatchCondition.Result.DISCARD) {
                            streamMessageQueue.take();
                        } else if (result == BatchCondition.Result.REJECT) {
                            return microStreamBatch;
                        }
                    } else {
                        streamMessageQueue.take();
                    }
                }
            } catch (Exception e) {
                logger.error("build stream error, stop building", e);
                throw new RuntimeException("build stream error, stop building", e);
            } finally {
                logger.info("one partition sign off");
                countDownLatch.countDown();
            }
        }
    }

    public final StreamParser getStreamParser() {
        return streamParser;
    }

    public final void setStreamParser(StreamParser streamParser) {
        this.streamParser = streamParser;
    }

}
