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
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Collections2;

/**
 * Load prepared data into kafka(for test use)
 */
public class Kafka10DataLoader extends StreamDataLoader {
    private static final Logger logger = LoggerFactory.getLogger(Kafka10DataLoader.class);
    List<KafkaClusterConfig> kafkaClusterConfigs;

    public Kafka10DataLoader(KafkaConfig kafkaConfig) {
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

        KafkaProducer producer = getKafkaProducer(brokerList, null);

        for (int i = 0; i < messages.size(); i++) {
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<String, String>(clusterConfig.getTopic(), String.valueOf(i), messages.get(i));
            producer.send(keyedMessage);
        }
        logger.info("sent " + messages.size() + " messages to " + this.toString());
        producer.close();
    }

    public static KafkaProducer getKafkaProducer(String brokers, Properties properties) {
        Properties props = constructDefaultKafkaProducerProperties(brokers, properties);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    public static Properties constructDefaultKafkaProducerProperties(String brokers, Properties properties) {
        Properties props = new Properties();
        props.put("retry.backoff.ms", "1000");
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "1");
        props.put("buffer.memory", 33554432);
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 50);
        props.put("request.timeout.ms", "30000");
        if (properties != null) {
            for (Map.Entry entry : properties.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }
        return props;
    }

}
