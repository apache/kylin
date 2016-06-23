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

package org.apache.kylin.source.kafka.diagnose;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.StreamingParser;
import org.apache.kylin.source.kafka.TimedJsonStreamParser;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.util.KafkaRequester;
import org.apache.kylin.source.kafka.util.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.message.MessageAndOffset;

/**
 * Continuously run this as a daemon to discover how "disordered" the kafka queue is.
 * This daemon only store a digest so it should not be space-consuming
 */
public class KafkaInputAnalyzer extends AbstractApplication {

    public class KafkaMessagePuller implements Runnable {

        private final String topic;
        private final int partitionId;
        private final KafkaClusterConfig streamingConfig;
        private final LinkedBlockingQueue<StreamingMessage> streamQueue;
        private final StreamingParser streamingParser;
        private final Broker leadBroker;
        private long offset;

        protected final Logger logger;

        public KafkaMessagePuller(int clusterID, String topic, int partitionId, long startOffset, Broker leadBroker, KafkaClusterConfig kafkaClusterConfig, StreamingParser streamingParser) {
            this.topic = topic;
            this.partitionId = partitionId;
            this.streamingConfig = kafkaClusterConfig;
            this.offset = startOffset;
            this.logger = LoggerFactory.getLogger(topic + "_cluster_" + clusterID + "_" + partitionId);
            this.streamQueue = new LinkedBlockingQueue<StreamingMessage>(10000);
            this.streamingParser = streamingParser;
            this.leadBroker = leadBroker;
        }

        public BlockingQueue<StreamingMessage> getStreamQueue() {
            return streamQueue;
        }

        @Override
        public void run() {
            try {
                int consumeMsgCount = 0;
                int fetchRound = 0;
                while (true) {
                    int consumeMsgCountAtBeginning = consumeMsgCount;
                    fetchRound++;

                    logger.info("fetching topic {} partition id {} offset {} leader {}", topic, String.valueOf(partitionId), String.valueOf(offset), leadBroker.toString());

                    final FetchResponse fetchResponse = KafkaRequester.fetchResponse(topic, partitionId, offset, leadBroker, streamingConfig);
                    if (fetchResponse.errorCode(topic, partitionId) != 0) {
                        logger.warn("fetch response offset:" + offset + " errorCode:" + fetchResponse.errorCode(topic, partitionId));
                        Thread.sleep(30000);
                        continue;
                    }

                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)) {
                        offset++;
                        consumeMsgCount++;

                        final StreamingMessage streamingMessage = streamingParser.parse(messageAndOffset.message().payload());
                        streamingMessage.setOffset(messageAndOffset.offset());
                        if (streamingParser.filter(streamingMessage)) {
                            streamQueue.add(streamingMessage);
                        }

                    }
                    logger.info("Number of messages consumed: " + consumeMsgCount + " offset is: " + offset + " total fetch round: " + fetchRound);

                    if (consumeMsgCount == consumeMsgCountAtBeginning) {//nothing this round 
                        Thread.sleep(30000);
                    }
                }
            } catch (Exception e) {
                logger.error("consumer has encountered an error", e);
            }
        }

    }

    @SuppressWarnings("static-access")
    private static final Option OPTION_STREAMING = OptionBuilder.withArgName("streaming").hasArg().isRequired(true).withDescription("Name of the streaming").create("streaming");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TASK = OptionBuilder.withArgName("task").hasArg().isRequired(true).withDescription("get delay or get disorder degree").create("task");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TSCOLNAME = OptionBuilder.withArgName("tsColName").hasArg().isRequired(true).withDescription("field name of the ts").create("tsColName");

    private static final Logger logger = LoggerFactory.getLogger(KafkaInputAnalyzer.class);

    private StreamingParser parser;
    private KafkaConfig kafkaConfig;

    private Options options;

    public KafkaInputAnalyzer() {
        options = new Options();
        options.addOption(OPTION_STREAMING);
        options.addOption(OPTION_TASK);
        options.addOption(OPTION_TSCOLNAME);

    }

    private List<BlockingQueue<StreamingMessage>> consume(final int clusterID, final KafkaClusterConfig kafkaClusterConfig, final int partitionCount, long whichtime) {
        List<BlockingQueue<StreamingMessage>> result = Lists.newArrayList();
        for (int partitionId = 0; partitionId < partitionCount; ++partitionId) {
            final kafka.cluster.Broker leadBroker = KafkaUtils.getLeadBroker(kafkaClusterConfig, partitionId);
            long streamingOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, whichtime, leadBroker, kafkaClusterConfig);
            logger.info("starting offset:" + streamingOffset + " cluster id:" + clusterID + " partitionId:" + partitionId);
            KafkaMessagePuller consumer = new KafkaMessagePuller(clusterID, kafkaClusterConfig.getTopic(), partitionId, streamingOffset, leadBroker, kafkaClusterConfig, parser);
            Executors.newSingleThreadExecutor(new DaemonThreadFactory()).submit(consumer);
            result.add(consumer.getStreamQueue());
        }
        return result;
    }

    private List<BlockingQueue<StreamingMessage>> consumeAll(long whichtime) {
        int clusterId = 0;
        final List<BlockingQueue<StreamingMessage>> queues = Lists.newLinkedList();

        for (final KafkaClusterConfig kafkaClusterConfig : kafkaConfig.getKafkaClusterConfigs()) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            final List<BlockingQueue<StreamingMessage>> oneClusterQueue = consume(clusterId, kafkaClusterConfig, partitionCount, whichtime);
            queues.addAll(oneClusterQueue);
            logger.info("Cluster {} with {} partitions", clusterId, oneClusterQueue.size());
            clusterId++;
        }
        return queues;
    }

    private void analyzeLatency() throws InterruptedException {
        long[] intervals = new long[] { 1, 5, 60, 300, 1800 };
        final List<BlockingQueue<StreamingMessage>> allPartitionData = consumeAll(OffsetRequest.LatestTime());
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
                            StreamingMessage message = allPartitionData.get(index).take();
                            long t = message.getTimestamp();
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
        final List<BlockingQueue<StreamingMessage>> allPartitionData = consumeAll(OffsetRequest.EarliestTime());

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
                            StreamingMessage message = allPartitionData.get(index).poll(60, TimeUnit.SECONDS);
                            if (message == null) {
                                System.out.println(String.format("Thread %d is exiting", index));
                                break;
                            }
                            long t = message.getTimestamp();
                            long offset = message.getOffset();
                            if (t < wallClocks.get(index)) {
                                maxDisorderTime.set(index, Math.max(wallClocks.get(index) - t, maxDisorderTime.get(index)));
                                maxDisorderOffset.set(index, Math.max(offset - wallOffset.get(index), maxDisorderOffset.get(index)));
                            } else {
                                wallClocks.set(index, t);
                                wallOffset.set(index, offset);
                            }
                            processedMessages.set(index, processedMessages.get(index) + 1);

                            if (processedMessages.get(index) % 10000 == 1) {
                                System.out.println(String.format("Thread %d processed %d messages. Max disorder time is %d , max disorder offset is %d", //
                                        index, processedMessages.get(index), maxDisorderTime.get(index), maxDisorderOffset.get(index)));
                            }
                        }

                        System.out.println(String.format("Thread %d finishes after %d messages. Max disorder time is %d , max disorder offset is %d", //
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

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {

        String streaming = optionsHelper.getOptionValue(OPTION_STREAMING);
        String task = optionsHelper.getOptionValue(OPTION_TASK);
        String tsColName = optionsHelper.getOptionValue(OPTION_TSCOLNAME);

        kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig(streaming);
        parser = new TimedJsonStreamParser(Lists.<TblColRef> newArrayList(), "formatTs=true;tsColName=" + tsColName);

        if ("disorder".equalsIgnoreCase(task)) {
            analyzeDisorder();
        } else if ("delay".equalsIgnoreCase(task)) {
            analyzeLatency();
        } else {
            optionsHelper.printUsage(this.getClass().getName(), options);
        }
    }

    public static void main(String[] args) {
        KafkaInputAnalyzer analyzer = new KafkaInputAnalyzer();
        analyzer.execute(args);
    }
}
