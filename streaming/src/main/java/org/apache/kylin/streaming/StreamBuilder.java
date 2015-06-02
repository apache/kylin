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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 */
public class StreamBuilder implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

    private StreamParser streamParser = StringStreamParser.instance;

    private StreamFilter streamFilter = DefaultStreamFilter.instance;

    private final List<BlockingQueue<StreamMessage>> streamMessageQueues;

    private final MicroStreamBatchConsumer consumer;

    private final MicroBatchCondition condition;

    public StreamBuilder(List<BlockingQueue<StreamMessage>> inputs, MicroBatchCondition condition, MicroStreamBatchConsumer consumer) {
        Preconditions.checkArgument(inputs.size() > 0);
        this.streamMessageQueues = Lists.newArrayList();
        this.consumer = Preconditions.checkNotNull(consumer);
        this.condition = condition;
        init(inputs);
    }

    public StreamBuilder(BlockingQueue<StreamMessage> input, MicroBatchCondition condition, MicroStreamBatchConsumer consumer) {
        this.streamMessageQueues = Lists.newArrayList();
        this.consumer = Preconditions.checkNotNull(consumer);
        this.condition = condition;
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
            while (true) {
                CountDownLatch countDownLatch = new CountDownLatch(inputCount);
                ArrayList<Future<MicroStreamBatch>> futures = Lists.newArrayListWithExpectedSize(inputCount);
                for (BlockingQueue<StreamMessage> streamMessageQueue : streamMessageQueues) {
                    futures.add(executorService.submit(new StreamFetcher(streamMessageQueue, countDownLatch)));
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
                consumer.consume(batches.get(0));
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
        private long lastBuildTime = System.currentTimeMillis();
        private long lastBatchTimestamp = -1;

        public StreamFetcher(BlockingQueue<StreamMessage> streamMessageQueue, CountDownLatch countDownLatch) {
            this.streamMessageQueue = streamMessageQueue;
            this.countDownLatch = countDownLatch;
        }

        private void clearCounter() {
            lastBuildTime = System.currentTimeMillis();
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
                    if (System.currentTimeMillis() - t <= timeout) {
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
                        microStreamBatch = new MicroStreamBatch();
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

                    final ParsedStreamMessage parsedStreamMessage = getStreamParser().parse(streamMessage);
                    if (parsedStreamMessage.getTimestamp() - microStreamBatch.getTimestamp().getFirst() > condition.getBatchInterval()) {
                        streamMessageQueue.take();
                        if (getStreamFilter().filter(parsedStreamMessage)) {
                            if (microStreamBatch.size() >= condition.getBatchSize()) {
                                return microStreamBatch;
                            } else {
                                microStreamBatch.add(parsedStreamMessage);
                            }
                        } else {
                            //ignore unfiltered stream message
                        }
                    } else {
                        return microStreamBatch;
                    }
                }
            } catch (Exception e) {
                logger.error("build stream error, stop building", e);
                throw new RuntimeException("build stream error, stop building", e);
            } finally {
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

    public final StreamFilter getStreamFilter() {
        return streamFilter;
    }

    public final void setStreamFilter(StreamFilter streamFilter) {
        this.streamFilter = streamFilter;
    }

}
