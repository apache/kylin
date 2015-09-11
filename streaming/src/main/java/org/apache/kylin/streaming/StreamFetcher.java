package org.apache.kylin.streaming;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 */
public class StreamFetcher implements Callable<MicroStreamBatch> {
    private static final Logger logger = LoggerFactory.getLogger(StreamFetcher.class);

    private final BlockingQueue<StreamMessage> streamMessageQueue;
    private final CountDownLatch countDownLatch;
    private final int partitionId;
    private final BatchCondition condition;
    private final StreamParser streamParser;

    public StreamFetcher(int partitionId, BlockingQueue<StreamMessage> streamMessageQueue, CountDownLatch countDownLatch, BatchCondition condition, StreamParser streamParser) {
        this.partitionId = partitionId;
        this.streamMessageQueue = streamMessageQueue;
        this.countDownLatch = countDownLatch;
        this.condition = condition;
        this.streamParser = streamParser;
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
                }
                StreamMessage streamMessage = peek(streamMessageQueue, 60000);
                if (streamMessage == null) {
                    logger.info("The stream queue is drained, current available stream count: " + microStreamBatch.size());
                    return microStreamBatch;
                }
                if (streamMessage.getOffset() < 0) {
                    logger.warn("streaming encountered EOF, stop building");
                    return null;
                }

                microStreamBatch.incRawMessageCount();
                final ParsedStreamMessage parsedStreamMessage = streamParser.parse(streamMessage);
                if (parsedStreamMessage == null) {
                    throw new RuntimeException("parsedStreamMessage of " + new String(streamMessage.getRawData()) + " is null");
                }

                final BatchCondition.Result result = condition.apply(parsedStreamMessage);
                if (parsedStreamMessage.isAccepted()) {
                    if (result == BatchCondition.Result.ACCEPT) {
                        streamMessageQueue.take();
                        microStreamBatch.add(parsedStreamMessage);
                    } else if (result == BatchCondition.Result.LAST_ACCEPT_FOR_BATCH) {
                        streamMessageQueue.take();
                        microStreamBatch.add(parsedStreamMessage);
                        break;
                    } else if (result == BatchCondition.Result.DISCARD) {
                        streamMessageQueue.take();
                    } else if (result == BatchCondition.Result.REJECT) {
                        logger.info("Partition :" + partitionId + " rejecting message at " + parsedStreamMessage.getOffset());
                        break;
                    }
                } else {
                    streamMessageQueue.take();
                }
            }

            Preconditions.checkArgument(microStreamBatch != null, "microStreamBatch is null!");
            logger.info(String.format("Partition %d contributing %d filtered messages out from %d raw messages"//
                    , partitionId, microStreamBatch.getFilteredMessageCount(), microStreamBatch.getRawMessageCount()));
            return microStreamBatch;

        } catch (Exception e) {
            logger.error("build stream error, stop building", e);
            throw new RuntimeException("build stream error, stop building", e);
        } finally {
            logger.info("partition {} sign off", partitionId);
            countDownLatch.countDown();
        }
    }
}
