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

package org.apache.kylin.streaming.kafka;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Created by qianzhou on 2/15/15.
 */
public final class Requester {

    private static final Logger logger = LoggerFactory.getLogger(Requester.class);

    public static TopicMeta getKafkaTopicMeta(TopicConfig topicConfig, ConsumerConfig consumerConfig) {
        SimpleConsumer consumer;
        for (Broker broker : topicConfig.getBrokers()) {
            consumer = new SimpleConsumer(broker.host(), broker.port(), consumerConfig.getTimeout(), consumerConfig.getBufferSize(), "topic_meta_lookup");
            List<String> topics = Collections.singletonList(topicConfig.getTopic());
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            TopicMetadataResponse resp = consumer.send(req);
            final List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
            if (topicMetadatas.size() != 1) {
                break;
            }
            final TopicMetadata topicMetadata = topicMetadatas.get(0);
            if (topicMetadata.errorCode() != 0) {
                break;
            }
            List<Integer> partitionIds = Lists.transform(topicMetadata.partitionsMetadata(), new Function<PartitionMetadata, Integer>() {
                @Nullable
                @Override
                public Integer apply(PartitionMetadata partitionMetadata) {
                    return partitionMetadata.partitionId();
                }
            });
            return new TopicMeta(topicConfig.getTopic(), partitionIds);
        }
        logger.debug("cannot find topic:" + topicConfig.getTopic());
        return null;
    }

    public static PartitionMetadata getPartitionMetadata(String topic, int partitionId, List<Broker> brokers, ConsumerConfig consumerConfig) {
        SimpleConsumer consumer;
        for (Broker broker : brokers) {
            consumer = new SimpleConsumer(broker.host(), broker.port(), consumerConfig.getTimeout(), consumerConfig.getBufferSize(), "topic_meta_lookup");
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            TopicMetadataResponse resp = consumer.send(req);
            final List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
            if (topicMetadatas.size() != 1) {
                logger.warn("invalid topicMetadata size:" + topicMetadatas.size());
                break;
            }
            final TopicMetadata topicMetadata = topicMetadatas.get(0);
            if (topicMetadata.errorCode() != 0) {
                logger.warn("fetching topicMetadata with errorCode:" + topicMetadata.errorCode());
                break;
            }
            for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                if (partitionMetadata.partitionId() == partitionId) {
                    return partitionMetadata;
                }
            }
        }
        logger.debug("cannot find PartitionMetadata, topic:" + topic + " partitionId:" + partitionId);
        return null;
    }

    public static FetchResponse fetchResponse(String topic, int partitionId, long offset, Broker broker, ConsumerConfig consumerConfig) {
        final String clientName = "client_" + topic + "_" + partitionId;
        SimpleConsumer consumer = new SimpleConsumer(broker.host(), broker.port(), consumerConfig.getTimeout(), consumerConfig.getBufferSize(), clientName);
        kafka.api.FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                .addFetch(topic, partitionId, offset, consumerConfig.getTimeout()) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                .build();
        return consumer.fetch(req);
    }
}
