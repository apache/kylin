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
    private final long startTimestamp;

    private StreamParser streamParser = StringStreamParser.instance;

    private final List<BlockingQueue<StreamMessage>> streamMessageQueues;

    private final MicroStreamBatchConsumer consumer;

    private final MicroBatchCondition condition;

    private final String streaming;

    public StreamBuilder(String streaming, List<BlockingQueue<StreamMessage>> inputs, MicroBatchCondition condition, MicroStreamBatchConsumer consumer, long startTimestamp) {
        Preconditions.checkArgument(inputs.size() > 0);
        this.streaming = streaming;
        this.streamMessageQueues = Lists.newArrayList();
        this.consumer = Preconditions.checkNotNull(consumer);
        this.condition = condition;
        this.startTimestamp = startTimestamp;
        init(inputs);
    }

    public StreamBuilder(String streaming, BlockingQueue<StreamMessage> input, MicroBatchCondition condition, MicroStreamBatchConsumer consumer, long startTimestamp) {
        this.streaming = streaming;
        this.streamMessageQueues = Lists.newArrayList();
        this.consumer = Preconditions.checkNotNull(consumer);
        this.condition = condition;
        this.startTimestamp = startTimestamp;
        init(Preconditions.checkNotNull(input));
    }

    private void init(BlockingQueue<StreamMessage> input) {
        this.streamMessageQueues.add(input);
    }

    private void init(List<BlockingQueue<StreamMessage>> inputs) {
        this.streamMessageQueues.addAll(inputs);
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
                    futures.add(executorService.submit(new StreamFetcher(partitionId++, streamMessageQueue, countDownLatch, start, start + condition.getBatchInterval())));
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
                        batch = MicroStreamBatch.union(batch, batches.get(i));
                    }
                }
                batch.getTimestamp().setFirst(start);
                batch.getTimestamp().setSecond(start + condition.getBatchInterval());
                start += condition.getBatchInterval();

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
        private long startTimestamp;
        private long endTimestamp;

        public StreamFetcher(int partitionId, BlockingQueue<StreamMessage> streamMessageQueue, CountDownLatch countDownLatch, long startTimestamp, long endTimestamp) {
            this.partitionId = partitionId;
            this.streamMessageQueue = streamMessageQueue;
            this.countDownLatch = countDownLatch;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
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

                    final long timestamp = parsedStreamMessage.getTimestamp();
                    if (timestamp < startTimestamp) {
                        //TODO properly handle late megs
                        streamMessageQueue.take();
                    } else if (timestamp < endTimestamp) {
                        streamMessageQueue.take();
                        if (parsedStreamMessage.isAccepted()) {
                            microStreamBatch.add(parsedStreamMessage);
                            if (microStreamBatch.size() >= condition.getBatchSize()) {
                                return microStreamBatch;
                            }
                        } else {
                            //ignore pruned stream message
                        }
                    } else {
                        return microStreamBatch;
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
