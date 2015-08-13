package org.apache.kylin.streaming;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * get the margin for a single partition
 */
public class PartitionMarginCalculator implements Callable<PartitionMargin> {

    /**
     * use the WINDOW_SIZE messages prior to a message to represent its show time
     */
    private class MovingAverage {

        private static final int WINDOW_SIZE = 20;

        private Queue<Long> q = Lists.newLinkedList();
        private long totalSum = 0;

        public long addNewElementAndGetAvg(long e) {
            if (q.size() < WINDOW_SIZE) {
                q.add(e);
                totalSum += e;
                return totalSum / q.size();
            }

            long head = q.remove();
            q.add(e);
            return (totalSum - head + e) / WINDOW_SIZE;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(PartitionMarginCalculator.class);

    private final BlockingQueue<StreamMessage> streamMessageQueue;
    private final CountDownLatch countDownLatch;
    private final int partitionId;
    private final long lastOffset;
    private final StreamParser streamParser;

    //runtime calculations
    private final HashMap<Long, Long> earliestOffsets = Maps.newHashMap();//for each second in context, record its earliest show offset in kafka
    private long maxMargin = 0;
    private final MovingAverage average = new MovingAverage();

    public PartitionMarginCalculator(int partitionId, BlockingQueue<StreamMessage> streamMessageQueue, CountDownLatch countDownLatch, long lastOffset, StreamParser streamParser) {
        this.partitionId = partitionId;
        this.streamMessageQueue = streamMessageQueue;
        this.countDownLatch = countDownLatch;
        this.streamParser = streamParser;
        this.lastOffset = lastOffset;
    }

    @Override
    public PartitionMargin call() throws Exception {
        try {
            while (true) {
                StreamMessage streamMessage = streamMessageQueue.poll(30, TimeUnit.SECONDS);
                if (streamMessage == null) {
                    logger.info("The stream queue for partition {} is drained", partitionId);
                    continue;
                }

                final ParsedStreamMessage parsedStreamMessage = streamParser.parse(streamMessage);
                if (parsedStreamMessage == null) {
                    throw new RuntimeException("parsedStreamMessage of " + new String(streamMessage.getRawData()) + " is null");
                }

                if (parsedStreamMessage.getOffset() >= lastOffset) {
                    logger.info("The final max margin for partition {} is {} ", partitionId, maxMargin);
                    return new PartitionMargin(maxMargin, maxMargin);
                }

                long timestamp = parsedStreamMessage.getTimestamp();
                long wallTime = average.addNewElementAndGetAvg(timestamp);
                long formalizedTs = timestamp / 1000 * 1000;

                if (earliestOffsets.containsKey(formalizedTs)) {
                    this.maxMargin = Math.max(this.maxMargin, Math.abs(earliestOffsets.get(formalizedTs) - wallTime));
                } else {
                    earliestOffsets.put(formalizedTs, wallTime);
                }

            }
        } catch (Exception e) {
            logger.error("partition margin calculation stream error, stopping", e);
            throw new RuntimeException("partition margin calculation stream error, stopping", e);
        } finally {
            logger.info("one partition sign off");
            countDownLatch.countDown();
        }
    }
}
