package org.apache.kylin.streaming;

import java.util.List;
import java.util.concurrent.*;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 */
public class MarginCalculator implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MarginCalculator.class);


    private final String streaming;
    private final List<BlockingQueue<StreamMessage>> queues;
    private final List<Pair<Long,Long>> firstAndLastOffsets;
    private StreamParser streamParser;

    public MarginCalculator(String streaming, List<BlockingQueue<StreamMessage>> queues, StreamParser streamParser, List<Pair<Long,Long>> firstAndLastOffsets) {
        Preconditions.checkArgument(queues.size() > 0);
        this.streaming = streaming;
        this.queues = queues;
        this.streamParser = streamParser;
        this.firstAndLastOffsets = firstAndLastOffsets;
    }

    @Override
    public void run() {
        try {
            final int inputCount = queues.size();
            final ExecutorService executorService = Executors.newFixedThreadPool(inputCount);
            final CountDownLatch countDownLatch = new CountDownLatch(inputCount);
            final List<Future<MicroStreamBatch>> futures = Lists.newLinkedList();
            int partitionId = 0;
            for (BlockingQueue<StreamMessage> queue : queues) {
                futures.add(executorService.submit(new StreamFetcher(partitionId, queue, countDownLatch, batchCondition, streamParser)));
            }
            countDownLatch.await();
            List<MicroStreamBatch> batches = Lists.newLinkedList();
            for (Future<MicroStreamBatch> future : futures) {
                if (future.get() != null) {
                    batches.add(future.get());
                } else {
                    logger.warn("EOF encountered, stop streaming");
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
            batch.getTimestamp().setFirst(batchCondition.getStartTime());
            batch.getTimestamp().setSecond(batchCondition.getEndTime());

            logger.info("Consuming {} messages, covering from {} to {}", new String[] { String.valueOf(batch.size()), DateFormat.formatToTimeStr(batch.getTimestamp().getFirst()), DateFormat.formatToTimeStr(batch.getTimestamp().getSecond()) });
            long startTime = System.currentTimeMillis();
            consumer.consume(batch);
            logger.info("Batch build costs {} milliseconds", System.currentTimeMillis() - startTime);
        } catch (InterruptedException ie) {
            throw new RuntimeException("this thread should not be interrupted", ie);
        } catch (ExecutionException ee) {
            logger.error("fetch stream error", ee);
            throw new RuntimeException(ee);
        } catch (Exception e) {
            logger.error("error consume batch", e);
            throw new RuntimeException("error consume batch", e);
        }


    }

}
