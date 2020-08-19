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

package org.apache.kylin.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaMsgProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMsgProducer.class);

    private static Producer<String, String> producer;
    private static Map<String, String> kafkaConfig;
    private static String TOPIC_NAME;

    private static final Properties kafkaProperties = new Properties();

    static {
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "-1");
        kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
        kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 10000);
        kafkaProperties.put("max.in.flight.requests.per.connection", 1);
    }


    private KafkaMsgProducer() {
        init();
    }

    private static class BasicProducerHolder {
        private static final KafkaMsgProducer INSTANCE = new KafkaMsgProducer();
    }

    public static final KafkaMsgProducer getInstance() {
        return BasicProducerHolder.INSTANCE;
    }

    public void init() {
        if (null == kafkaConfig) {
            kafkaConfig = KylinConfig.getInstanceFromEnv().getJobStatusKafkaConfig();
        }
        if (null == producer) {
            kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.get("bootstrap.servers"));
            for(Map.Entry<String, String> entry : kafkaConfig.entrySet()){
                kafkaProperties.put(entry.getKey(), entry.getValue());
            }
            producer = new KafkaProducer<>(kafkaProperties);
        }
        if (null == TOPIC_NAME) {
            TOPIC_NAME = kafkaConfig.get("topic.name");
        }
    }

    public void sendJobStatusMessage(String message) {
        sendMessage(message);
    }

    private void sendMessage(final String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
        producer.send(record, (recordMetadata, exception) -> {
            if (null != exception) {
                logger.error("kafka send message error.", exception);
            }
        });
    }

}
