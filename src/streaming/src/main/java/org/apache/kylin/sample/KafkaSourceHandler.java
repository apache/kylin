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

package org.apache.kylin.sample;

import static org.apache.kylin.common.exception.ServerErrorCode.BROKER_TIMEOUT_MESSAGE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.STREAMING_PARSE_MESSAGE_ERROR;
import static org.apache.kylin.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.kafka.util.KafkaUtils;
import org.apache.kylin.loader.ParserClassLoaderState;
import org.apache.kylin.metadata.jar.JarInfoManager;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.parser.AbstractDataParser;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaSourceHandler implements StreamingSourceHandler {

    private static final String DEFAULT_CONSUMER_GROUP = "sample";
    private static final String DEFAULT_TOPIC = "__consumer_offsets";
    public static final String DEFAULT_PARSER = "org.apache.kylin.parser.TimedJsonStreamParser";

    private static final String COL_PATTERN = "^(?!\\d+|_)([0-9a-zA-Z_]{1,}$)";
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    private static final int SAMPLE_MSG_COUNT = 10;
    private static final Long CONSUMER_LIST_TOPICS_TIMEOUT = 30000L;
    private static final int CLIENT_LIST_TOPICS_TIMEOUT = 5000;

    @Override
    public Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, String fuzzyTopic) {
        int index = 0;
        index++;
        List<String> topics = Lists.newArrayList();
        try (val consumer = KafkaUtils.getKafkaConsumer(kafkaConfig.getKafkaBootstrapServers(),
                DEFAULT_CONSUMER_GROUP)) {
            Map<String, List<PartitionInfo>> topicsMap = Maps.newHashMap();
            try {
                topicsMap.putAll(consumer.listTopics(Duration.ofMillis(CONSUMER_LIST_TOPICS_TIMEOUT)));
            } catch (TimeoutException e) {
                throw new KylinException(BROKER_TIMEOUT_MESSAGE, MsgPicker.getMsg().getBrokerTimeoutMessage());
            }
            boolean isEmptyFuzzy = StringUtils.isEmpty(fuzzyTopic);
            topicsMap.keySet().stream()//
                    .filter(this::isUsefulTopic)//
                    .filter(topic -> isEmptyFuzzy
                            || StringUtils.containsIgnoreCase(topic.toLowerCase(Locale.ROOT), fuzzyTopic))
                    .forEach(topics::add);
        }
        Collections.sort(topics);
        Map<String, List<String>> clusterTopics = Maps.newTreeMap();
        clusterTopics.put("kafka-cluster-" + index, topics);
        return clusterTopics;
    }

    @Override
    public List<String> getBrokenBrokers(KafkaConfig kafkaConfig) {
        // broken broker list
        List<String> failList = new ArrayList<>();
        List<AdminClient> clientList = new ArrayList<>();
        Map<String, ListTopicsResult> futureMap = new HashMap<>();

        // AdminClient is Kafka management tool client
        Arrays.stream(kafkaConfig.getKafkaBootstrapServers().split(",")).forEach(broker -> {
            AdminClient kafkaAdminClient = KafkaUtils.getKafkaAdminClient(broker, DEFAULT_CONSUMER_GROUP);
            ListTopicsResult listTopicsResult = kafkaAdminClient
                    .listTopics(new ListTopicsOptions().timeoutMs(CLIENT_LIST_TOPICS_TIMEOUT));
            futureMap.put(broker, listTopicsResult);
            clientList.add(kafkaAdminClient);
        });

        futureMap.forEach((broker, result) -> {
            try {
                // Get a list of topics
                // If an exception is thrown, the broker marked as failed
                result.names().get();
            } catch (ExecutionException | org.apache.kafka.common.errors.TimeoutException e) {
                failList.add(broker);
                log.warn("Broker [{}] cannot be connected, marked as failed", broker);
            } catch (InterruptedException e) {
                log.error("The current thread is interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
        // close all client
        clientList.forEach(AdminClient::close);
        return failList;
    }

    private boolean isUsefulTopic(String topic) {
        final Pattern pattern = Pattern.compile(UUID_PATTERN);
        if (pattern.matcher(topic).matches()) {
            return false;
        }
        return !StringUtils.equals(topic, DEFAULT_TOPIC);
    }

    @Override
    public List<ByteBuffer> getMessages(KafkaConfig kafkaConfig) {
        log.info("Start to get sample messages from Kafka.");
        long pollMsgTimeout = KylinConfig.getInstanceFromEnv().getKafkaPollMessageTimeout();
        String topic = kafkaConfig.getSubscribe();
        String brokers = kafkaConfig.getKafkaBootstrapServers();
        long startTime = System.currentTimeMillis();

        log.info("Trying to get messages from brokers: {}", brokers);
        List<ByteBuffer> samples = Lists.newArrayList();
        try (val consumer = KafkaUtils.getKafkaConsumer(brokers, DEFAULT_CONSUMER_GROUP)) {
            val partitionInfos = consumer.partitionsFor(topic);
            if (CollectionUtils.isEmpty(partitionInfos)) {
                log.warn("There are no partitions in topic: {}", topic);
                return samples;
            }
            val partitions = partitionInfos.stream()//
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))//
                    .collect(Collectors.toList());
            val beginningOffsets = consumer.beginningOffsets(partitions);

            for (val partition : partitions) {
                if (samples.size() >= SAMPLE_MSG_COUNT) {
                    break;
                }
                Long beginOffset = beginningOffsets.get(partition);
                pollMsg(partition, beginOffset, consumer, topic, pollMsgTimeout, samples);
            }
        }
        log.info("Get sample message size is: {}, cost: {}ms", samples.size(), System.currentTimeMillis() - startTime);
        return samples;
    }

    private void pollMsg(TopicPartition partition, Long beginOffset, Consumer<String, ByteBuffer> consumer,
            String topic, Long pollMsgTimeout, List<ByteBuffer> samples) {
        List<TopicPartition> partitionList = Collections.singletonList(partition);
        consumer.assign(partitionList);
        consumer.seekToEnd(partitionList);
        long maxOffset = consumer.position(partition);
        long count = maxOffset - beginOffset;
        if (count <= 0) {
            return;
        }
        consumer.seek(partition, count < SAMPLE_MSG_COUNT ? beginOffset : maxOffset - SAMPLE_MSG_COUNT);

        log.info("Ready to poll messages. Topic: {}, Partition: {}, Partition beginning offset: {}, Offset: {}", topic,
                partition.partition(), beginOffset, maxOffset);
        val records = consumer.poll(Duration.ofMillis(pollMsgTimeout));
        if (records.isEmpty()) {
            return;
        }
        for (val record : records) {
            if (samples.size() >= SAMPLE_MSG_COUNT) {
                break;
            }
            samples.add(record.value());
        }
    }

    @Override
    public Map<String, Object> parserMessage(KafkaConfig kafkaConfig, String msg) {
        Map<String, Object> result;
        String parserName = kafkaConfig.getParserName();
        String project = kafkaConfig.getProject();
        String topic = kafkaConfig.getSubscribe();
        try {
            ParserClassLoaderState loaderState = ParserClassLoaderState.getInstance(project);
            checkParserRegister(parserName, project, loaderState);
            result = AbstractDataParser.getDataParser(parserName, loaderState.getClassLoader())
                    .process(StandardCharsets.UTF_8.encode(msg));
        } catch (Exception e) {
            throw new KylinException(STREAMING_PARSE_MESSAGE_ERROR, e, parserName, topic);
        }
        checkColName(result);
        return result;
    }

    public void checkParserRegister(String parserName, String project, ParserClassLoaderState loaderState) {
        if (StringUtils.equals(DEFAULT_PARSER, parserName)) {
            return;
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val parserInfo = DataParserManager.getInstance(config, project).getDataParserInfo(parserName);
        val jarInfo = JarInfoManager.getInstance(config, project).getJarInfo(STREAMING_CUSTOM_PARSER,
                parserInfo.getJarName());
        if (loaderState.getLoadedJars().contains(jarInfo.getJarPath())) {
            return;
        }
        loaderState.registerJars(Sets.newHashSet(jarInfo.getJarPath()));
    }

    private static void checkColName(Map<String, Object> inputParserMap) {
        final Pattern pattern = Pattern.compile(COL_PATTERN);
        for (String colName : inputParserMap.keySet()) {
            if (!pattern.matcher(colName).matches()) {
                throw new KylinException(CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED);
            }
        }
    }
}
