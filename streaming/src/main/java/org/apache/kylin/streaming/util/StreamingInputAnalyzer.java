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

package org.apache.kylin.streaming.util;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.api.OffsetRequest;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.KafkaClusterConfig;
import org.apache.kylin.streaming.KafkaConsumer;
import org.apache.kylin.streaming.KafkaRequester;
import org.apache.kylin.streaming.ParsedStreamMessage;
import org.apache.kylin.streaming.StreamMessage;
import org.apache.kylin.streaming.StreamParser;
import org.apache.kylin.streaming.StreamingConfig;
import org.apache.kylin.streaming.StreamingManager;
import org.apache.kylin.streaming.StreamingUtil;
import org.apache.kylin.streaming.TimedJsonStreamParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Continuously run this as a daemon to discover how "disordered" the kafka queue is.
 * This daemon only store a digest so it should not be space-consuming
 */
public class StreamingInputAnalyzer extends AbstractHadoopJob {
    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING = OptionBuilder.withArgName("streaming").hasArg().isRequired(true).withDescription("Name of the streaming").create("streaming");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TASK = OptionBuilder.withArgName("task").hasArg().isRequired(true).withDescription("get delay or get disorder degree").create("task");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TSCOLNAME = OptionBuilder.withArgName("tsColName").hasArg().isRequired(true).withDescription("field name of the ts").create("tsColName");

    private static final Logger logger = LoggerFactory.getLogger(StreamingInputAnalyzer.class);

    private StreamParser parser;
    private StreamingConfig streamingConfig;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        logger.info("jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_STREAMING);
            options.addOption(OPTION_TASK);
            options.addOption(OPTION_TSCOLNAME);
            parseOptions(options, args);
            logger.info("options: '" + getOptionsAsString() + "'");
        } catch (Exception e) {
            e.printStackTrace();
            printUsage(options);
            return -1;
        }

        String streaming = getOptionValue(OPTION_STREAMING);
        String task = getOptionValue(OPTION_TASK);
        String tsColName = getOptionValue(OPTION_TSCOLNAME);

        streamingConfig = StreamingManager.getInstance(KylinConfig.getInstanceFromEnv()).getStreamingConfig(streaming);
        parser = new TimedJsonStreamParser(Lists.<TblColRef> newArrayList(), "formatTs=true;tsColName=" + tsColName);

        if ("disorder".equalsIgnoreCase(task)) {
            analyzeDisorder();
        } else if ("delay".equalsIgnoreCase(task)) {
            analyzeLatency();
        } else {
            printUsage(options);
            return -1;
        }

        return 0;
    }

    private List<BlockingQueue<StreamMessage>> consume(final int clusterID, final KafkaClusterConfig kafkaClusterConfig, final int partitionCount, long whichtime) {
        List<BlockingQueue<StreamMessage>> result = Lists.newArrayList();
        for (int partitionId = 0; partitionId < partitionCount; ++partitionId) {
            final kafka.cluster.Broker leadBroker = StreamingUtil.getLeadBroker(kafkaClusterConfig, partitionId);
            long streamingOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, whichtime, leadBroker, kafkaClusterConfig);
            logger.info("starting offset:" + streamingOffset + " cluster id:" + clusterID + " partitionId:" + partitionId);
            KafkaConsumer consumer = new KafkaConsumer(clusterID, kafkaClusterConfig.getTopic(), partitionId, streamingOffset, kafkaClusterConfig.getBrokers(), kafkaClusterConfig);
            Executors.newSingleThreadExecutor(new DaemonThreadFactory()).submit(consumer);
            result.add(consumer.getStreamQueue(0));
        }
        return result;
    }

    private List<BlockingQueue<StreamMessage>> consumeAll(long whichtime) {
        int clusterId = 0;
        final List<BlockingQueue<StreamMessage>> queues = Lists.newLinkedList();

        for (final KafkaClusterConfig kafkaClusterConfig : streamingConfig.getKafkaClusterConfigs()) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            final List<BlockingQueue<StreamMessage>> oneClusterQueue = consume(clusterId, kafkaClusterConfig, partitionCount, whichtime);
            queues.addAll(oneClusterQueue);
            logger.info("Cluster {} with {} partitions", clusterId, oneClusterQueue.size());
            clusterId++;
        }
        return queues;
    }

    private void analyzeLatency() throws InterruptedException {
        long[] intervals = new long[] { 1, 5, 60, 300, 1800 };
        final List<BlockingQueue<StreamMessage>> allPartitionData = consumeAll(OffsetRequest.LatestTime());
        final List<TimeHistogram> allHistograms = Lists.newArrayList();
        final TimeHistogram overallHistogram = new TimeHistogram(intervals, "overall");

        ExecutorService executorService = Executors.newFixedThreadPool(allPartitionData.size(), new DaemonThreadFactory());
        for (int i = 0; i < allPartitionData.size(); ++i) {
            final int index = i;
            allHistograms.add(new TimeHistogram(intervals, "" + i));
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            StreamMessage message = allPartitionData.get(index).take();
                            ParsedStreamMessage parsedStreamMessage = parser.parse(message);
                            long t = parsedStreamMessage.getTimestamp();
                            allHistograms.get(index).processMillis(System.currentTimeMillis() - t);
                            overallHistogram.processMillis(System.currentTimeMillis() - t);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        while (true) {
            System.out.println("Printing status at : " + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime()));

            for (TimeHistogram histogram : allHistograms) {
                histogram.printStatus();
            }
            overallHistogram.printStatus();
            Thread.sleep(300000);
        }
    }

    private void analyzeDisorder() throws InterruptedException {
        final List<BlockingQueue<StreamMessage>> allPartitionData = consumeAll(OffsetRequest.EarliestTime());

        final List<Long> wallClocks = Lists.newArrayList();
        final List<Long> wallOffset = Lists.newArrayList();
        final List<Long> maxDisorderTime = Lists.newArrayList();
        final List<Long> maxDisorderOffset = Lists.newArrayList();
        final List<Long> processedMessages = Lists.newArrayList();

        for (int i = 0; i < allPartitionData.size(); i++) {
            wallClocks.add(0L);
            wallOffset.add(0L);
            maxDisorderTime.add(0L);
            maxDisorderOffset.add(0L);
            processedMessages.add(0L);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(allPartitionData.size(), new DaemonThreadFactory());
        final CountDownLatch countDownLatch = new CountDownLatch(allPartitionData.size());
        for (int i = 0; i < allPartitionData.size(); ++i) {
            final int index = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            StreamMessage message = allPartitionData.get(index).poll(60, TimeUnit.SECONDS);
                            if (message == null) {
                                System.out.println(String.format("Thread %d is exiting", index));
                                break;
                            }
                            ParsedStreamMessage parsedStreamMessage = parser.parse(message);
                            long t = parsedStreamMessage.getTimestamp();
                            long offset = parsedStreamMessage.getOffset();
                            if (t < wallClocks.get(index)) {
                                maxDisorderTime.set(index, Math.max(wallClocks.get(index) - t, maxDisorderTime.get(index)));
                                maxDisorderOffset.set(index, Math.max(offset - wallOffset.get(index), maxDisorderOffset.get(index)));
                            } else {
                                wallClocks.set(index, t);
                                wallOffset.set(index, offset);
                            }
                            processedMessages.set(index, processedMessages.get(index) + 1);

                            if (processedMessages.get(index) % 10000 == 1) {
                                System.out.println(String.format("Thread %d processed %d messages. Max disorder time is %d , max disorder offset is %d",//
                                        index, processedMessages.get(index), maxDisorderTime.get(index), maxDisorderOffset.get(index)));
                            }
                        }

                        System.out.println(String.format("Thread %d finishes after %d messages. Max disorder time is %d , max disorder offset is %d",//
                                index, processedMessages.get(index), maxDisorderTime.get(index), maxDisorderOffset.get(index)));
                        countDownLatch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        countDownLatch.await();
    }

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new StreamingInputAnalyzer(), args);
        System.exit(exitCode);
    }
}
