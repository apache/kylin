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
package org.apache.kylin.source.kafka.util;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class KafkaClient {

    public static KafkaConsumer getKafkaConsumer(String brokers, String consumerGroup, Properties properties) {
        Properties props = constructDefaultKafkaConsumerProperties(brokers, consumerGroup, properties);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static Properties constructDefaultKafkaConsumerProperties(String brokers, String consumerGroup, Properties properties) {
        Properties props = new Properties();
        if (properties != null) {
            for (Map.Entry entry : properties.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }
        props.put("bootstrap.servers", brokers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "false");
        return props;
    }

    public static String getKafkaBrokers(KafkaConfig kafkaConfig) {
        String brokers = null;
        for (KafkaClusterConfig clusterConfig : kafkaConfig.getKafkaClusterConfigs()) {
            for (BrokerConfig brokerConfig : clusterConfig.getBrokerConfigs()) {
                if (brokers == null) {
                    brokers = brokerConfig.getHost() + ":" + brokerConfig.getPort();
                } else {
                    brokers = brokers + "," + brokerConfig.getHost() + ":" + brokerConfig.getPort();
                }
            }
        }

        if (StringUtils.isEmpty(brokers)) {
            throw new IllegalArgumentException("No cluster info in Kafka config '" + kafkaConfig.getName() + "'");
        }
        return brokers;
    }

    public static long getEarliestOffset(KafkaConsumer consumer, String topic, int partitionId) {

        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(Arrays.asList(topicPartition));

        return consumer.position(topicPartition);
    }

    public static long getLatestOffset(KafkaConsumer consumer, String topic, int partitionId) {

        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));

        return consumer.position(topicPartition);
    }

    public static Map<Integer, Long> getLatestOffsets(final CubeInstance cubeInstance) {
        final KafkaConfig kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig(cubeInstance.getRootFactTable());

        final String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
        final String topic = kafkaConfig.getTopic();

        Map<Integer, Long> startOffsets = Maps.newHashMap();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers, cubeInstance.getName(), null)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                long latest = getLatestOffset(consumer, topic, partitionInfo.partition());
                startOffsets.put(partitionInfo.partition(), latest);
            }
        }
        return startOffsets;
    }


    public static Map<Integer, Long> getEarliestOffsets(final CubeInstance cubeInstance) {
        final KafkaConfig kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig(cubeInstance.getRootFactTable());

        final String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
        final String topic = kafkaConfig.getTopic();

        Map<Integer, Long> startOffsets = Maps.newHashMap();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers, cubeInstance.getName(), null)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                long latest = getEarliestOffset(consumer, topic, partitionInfo.partition());
                startOffsets.put(partitionInfo.partition(), latest);
            }
        }
        return startOffsets;
    }
}
