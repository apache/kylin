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

package org.apache.kylin.job.streaming;

import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.kylin.source.kafka.config.KafkaConfig;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Load prepared data into kafka(for test use)
 */
public class KafkaDataLoader extends StreamDataLoader {
    List<KafkaClusterConfig> kafkaClusterConfigs;

    public KafkaDataLoader(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
        this.kafkaClusterConfigs = kafkaConfig.getKafkaClusterConfigs();
    }

    public void loadIntoKafka(List<String> messages) {

        KafkaClusterConfig clusterConfig = kafkaClusterConfigs.get(0);
        String brokerList = StringUtils.join(Collections2.transform(clusterConfig.getBrokerConfigs(), new Function<BrokerConfig, String>() {
            @Nullable
            @Override
            public String apply(BrokerConfig brokerConfig) {
                return brokerConfig.getHost() + ":" + brokerConfig.getPort();
            }
        }), ",");
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("retry.backoff.ms", "1000");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        List<KeyedMessage<String, String>> keyedMessages = Lists.newArrayList();
        for (int i = 0; i < messages.size(); ++i) {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(clusterConfig.getTopic(), String.valueOf(i), messages.get(i));
            keyedMessages.add(keyedMessage);
        }
        producer.send(keyedMessages);
        producer.close();
    }

}
