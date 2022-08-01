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

package org.apache.kylin.kafka;

import static org.apache.kylin.common.exception.ServerErrorCode.BROKER_TIMEOUT_MESSAGE;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.kafka.util.KafkaClient;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CollectKafkaStats {

    public static final String JSON_MESSAGE = "json";
    public static final String BINARY_MESSAGE = "binary";
    private static final Logger logger = LoggerFactory.getLogger(CollectKafkaStats.class);
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    private static final String IP_PATTERN = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";
    private static final int SAMPLE_MSG_COUNT = 10;
    private static final long POLL_MESSAGE_TIMEOUT = KylinConfig.getInstanceFromEnv().getKafkaPollMessageTimeout();
    private static final String DEFAULT_CONSUMER_GROUP = "sample";
    private static final int CLIENT_LIST_TOPICS_TIMEOUT = 5000;
    private static final Long CONSUMER_LIST_TOPICS_TIMEOUT = 30000L;

    /**
     * get broken brokers
     *
     * @param kafkaConfig kafkaConfig
     * @return broken broker list
     */
    public static List<String> getBrokenBrokers(KafkaConfig kafkaConfig) {

        // broken broker list
        List<String> failList = new ArrayList<>();
        List<AdminClient> adminClientList = new ArrayList<>();
        Map<String, ListTopicsResult> futureMap = new HashMap<>();

        // AdminClient is Kafka management tool client
        Arrays.stream(kafkaConfig.getKafkaBootstrapServers().split(",")).forEach(broker -> {
            AdminClient kafkaAdminClient = KafkaClient.getKafkaAdminClient(broker, DEFAULT_CONSUMER_GROUP);
            ListTopicsResult listTopicsResult = kafkaAdminClient
                    .listTopics(new ListTopicsOptions().timeoutMs(CLIENT_LIST_TOPICS_TIMEOUT));
            futureMap.put(broker, listTopicsResult);
            adminClientList.add(kafkaAdminClient);
        });

        futureMap.forEach((broker, result) -> {
            try {
                // Get a list of topics
                // If an exception is thrown, the broker marked as failed
                result.names().get();
            } catch (ExecutionException | org.apache.kafka.common.errors.TimeoutException e) {
                failList.add(broker);
                logger.warn("Broker [{}] cannot be connected, marked as failed", broker);
            } catch (InterruptedException e) {
                logger.error("The current thread is interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
        // close all AdminClient
        adminClientList.forEach(AdminClient::close);

        return failList;
    }

    //List topics
    public static Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, String fuzzyTopic) {
        Map<String, List<String>> clusterTopics = Maps.newTreeMap();

        int index = 0;
        index++;
        List<String> topics = Lists.newArrayList();
        Consumer consumer = KafkaClient.getKafkaConsumer(kafkaConfig.getKafkaBootstrapServers(),
                DEFAULT_CONSUMER_GROUP);
        Map<String, List<PartitionInfo>> topicsMap = new HashMap<>();
        try {
            topicsMap.putAll(consumer.listTopics(Duration.ofMillis(CONSUMER_LIST_TOPICS_TIMEOUT)));
        } catch (TimeoutException e) {
            throw new KylinException(BROKER_TIMEOUT_MESSAGE, MsgPicker.getMsg().getBrokerTimeoutMessage());
        }

        for (String topic : topicsMap.keySet()) {
            if (isUsefulTopic(topic)) {
                if (fuzzyTopic == null || topic.toLowerCase(Locale.ROOT).contains(fuzzyTopic)) {
                    topics.add(topic);
                }
            }
        }

        consumer.close();

        Collections.sort(topics);
        clusterTopics.put("kafka-cluster-" + index, topics);
        return clusterTopics;
    }

    public static List<ByteBuffer> getMessages(KafkaConfig kafkaConfig, int clusterIndex) {
        logger.info("Start to get sample messages from Kafka.");
        String topic = kafkaConfig.getSubscribe();

        String brokers = kafkaConfig.getKafkaBootstrapServers();
        List<ByteBuffer> samples = new ArrayList<>();

        logger.info("Trying to get messages from brokers: {}", brokers);
        Consumer consumer;
        ConsumerRecords<String, ByteBuffer> records;
        consumer = KafkaClient.getKafkaConsumer(brokers, DEFAULT_CONSUMER_GROUP);

        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

        if (partitionInfos == null || partitionInfos.isEmpty()) {
            logger.warn("There are no partitions in topic: {}", topic);
            return samples;
        }

        final List<TopicPartition> partitions = partitionInfos.stream()
                .map(info -> new TopicPartition(topic, info.partition())).collect(Collectors.toList());

        final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

        for (TopicPartition topicPartition : partitions) {
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seekToEnd(Arrays.asList(topicPartition));
            long beginOffset = beginningOffsets.get(topicPartition);
            long offset = consumer.position(topicPartition);
            long count = offset - beginOffset;
            if (count <= 0) {
                continue;
            } else if (count < SAMPLE_MSG_COUNT) {
                consumer.seek(topicPartition, beginOffset);
            } else {
                consumer.seek(topicPartition, offset - SAMPLE_MSG_COUNT);
            }

            logger.info("Ready to poll messages. Topic: {}, Partition: {}, Partition beginning offset: {}, Offset: {}",
                    topic, topicPartition.partition(), beginOffset, offset);
            records = consumer.poll(POLL_MESSAGE_TIMEOUT);

            if (!records.isEmpty()) {
                for (ConsumerRecord<String, ByteBuffer> record : records) {
                    if (samples.size() >= SAMPLE_MSG_COUNT)
                        break;
                    samples.add(record.value());
                }
                break;
            }
        }
        consumer.close();

        logger.info("Get sample message size is: {}", samples.size());
        return samples;
    }

    private static boolean isUsefulTopic(String topic) {
        final Pattern UUId_PATTERN = Pattern.compile(UUID_PATTERN);
        if (UUId_PATTERN.matcher(topic).matches()) {
            return false;
        }

        if ("__consumer_offsets".equals(topic)) {
            return false;
        }
        return true;
    }
}
