package org.apache.kylin.job.streaming;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.streaming.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

/**
 */
public class PeriodicalStreamBuilderTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(PeriodicalStreamBuilderTest.class);

    @Before
    public void setup() {
        this.createTestMetadata();

    }

    @After
    public void clear() {
        this.cleanupTestMetadata();
    }

    private List<StreamMessage> prepareTestData(long start, long end, int count) {
        double step = (double)(end - start) / (count - 1);
        long ts = start;
        int offset = 0;
        ArrayList<StreamMessage> result = Lists.newArrayList();
        for (int i = 0; i < count - 1; ++i) {
            result.add(new StreamMessage(offset++, String.valueOf(ts).getBytes()));
            ts += step;
        }
        result.add(new StreamMessage(offset++, String.valueOf(end).getBytes()));
        assertEquals(count, result.size());
        assertEquals(start+"", new String(result.get(0).getRawData()));
        assertEquals(end+"", new String(result.get(count - 1).getRawData()));
        return result;
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        List<BlockingQueue<StreamMessage>> queues = Lists.newArrayList();
        queues.add(new LinkedBlockingQueue<StreamMessage>());
        queues.add(new LinkedBlockingQueue<StreamMessage>());

        final long interval = 3000L;
        final long nextPeriodStart = TimeUtil.getNextPeriodStart(System.currentTimeMillis(), interval);

        final List<Integer> partitionIds = Lists.newArrayList();
        for (int i = 0; i < queues.size(); i++) {
            partitionIds.add(i);
        }

        final MicroStreamBatchConsumer consumer = new MicroStreamBatchConsumer() {
            @Override
            public void consume(MicroStreamBatch microStreamBatch) throws Exception {
                logger.info("consuming batch:" + microStreamBatch.getPartitionId() + " count:" + microStreamBatch.size() + " timestamp:" + microStreamBatch.getTimestamp() + " offset:" + microStreamBatch.getOffset());
            }

            @Override
            public void stop() {
                logger.info("consumer stopped");
            }
        };
        final StreamBuilder streamBuilder = StreamBuilder.newPeriodicalStreamBuilder("test", queues, consumer, nextPeriodStart, interval);

        streamBuilder.setStreamParser(new StreamParser() {
            @Override
            public ParsedStreamMessage parse(StreamMessage streamMessage) {
                return new ParsedStreamMessage(Collections.<String>emptyList(), streamMessage.getOffset(), Long.parseLong(new String(streamMessage.getRawData())), true);
            }
        });

        Future<?> future = Executors.newSingleThreadExecutor().submit(streamBuilder);
        long timeout = nextPeriodStart + interval;
        int messageCount = 0;
        int inPeriodMessageCount = 0;
        int expectedOffset = 0;
        logger.info("prepare to add StreamMessage");
        while (true) {
            long ts = System.currentTimeMillis();
            if (ts > timeout + interval) {
                break;
            }
            if (ts >= nextPeriodStart && ts < timeout) {
                inPeriodMessageCount++;
            }
            for (BlockingQueue<StreamMessage> queue : queues) {
                queue.put(new StreamMessage(messageCount, String.valueOf(ts).getBytes()));
            }
            if (expectedOffset == 0 && ts > timeout) {
                expectedOffset = messageCount - 1;
            }
            messageCount++;
            Thread.sleep(10);
        }
        logger.info("totally put " + messageCount + " StreamMessages");
        logger.info("totally in period " + inPeriodMessageCount + " StreamMessages");

        for (BlockingQueue<StreamMessage> queue : queues) {
            queue.put(StreamMessage.EOF);
        }

        future.get();

        for (BlockingQueue<StreamMessage> queue : queues) {
            queue.take();
        }

        final Map<Integer, Long> offsets = StreamingManager.getInstance(KylinConfig.getInstanceFromEnv()).getOffset("test", partitionIds);
        logger.info("offset:" + offsets);
        for (Long offset : offsets.values()) {
            assertEquals(expectedOffset, offset.longValue());
        }

    }
}
