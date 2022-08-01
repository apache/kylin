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

package org.apache.kylin.kafka.util;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.streaming.jobs.StreamingJobUtils;

public class KafkaClient {
    /**
     * load jaas file's text into kafkaJaasTextPair to avoid multiple read file
     */
    private static final Pair<Boolean, String> kafkaJaasTextPair = new Pair<>(false, null);
    private static Consumer<Object, Object> mockup;

    private KafkaClient() {
        throw new IllegalStateException("Utility class");
    }

    public static Consumer<Object, Object> getKafkaConsumer(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = constructDefaultKafkaConsumerProperties(brokers, consumerGroup, properties);
        if (mockup != null) {
            return mockup;
        } else {
            return new KafkaConsumer<>(props);
        }
    }

    public static Consumer<Object, Object> getKafkaConsumer(String brokers, String consumerGroup) {
        return getKafkaConsumer(brokers, consumerGroup, new Properties());
    }

    public static AdminClient getKafkaAdminClient(String brokers, String consumerGroup) {
        return getKafkaAdminClient(brokers, consumerGroup, new Properties());
    }

    public static AdminClient getKafkaAdminClient(String brokers, String consumerGroup, Properties properties) {
        Properties props = constructDefaultKafkaAdminClientProperties(brokers, consumerGroup, properties);
        return AdminClient.create(props);
    }

    public static Properties constructDefaultKafkaAdminClientProperties(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = new Properties();
        setSaslJaasConf(props);
        props.put("bootstrap.servers", brokers);
        props.put("group.id", consumerGroup);
        if (properties != null) {
            props.putAll(properties);
        }
        return props;
    }

    public static Properties constructDefaultKafkaConsumerProperties(String brokers, String consumerGroup,
            Properties properties) {
        Properties props = new Properties();
        setSaslJaasConf(props);
        props.put("bootstrap.servers", brokers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteBufferDeserializer.class.getName());
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "false");
        if (properties != null) {
            props.putAll(properties);
        }
        return props;
    }

    private static void setSaslJaasConf(Properties props) {
        props.putAll(KylinConfig.getInstanceFromEnv().getStreamingKafkaConfigOverride());
        synchronized (kafkaJaasTextPair) {
            if (!kafkaJaasTextPair.getFirst().booleanValue()) {
                kafkaJaasTextPair.setSecond(StreamingJobUtils.extractKafkaSaslJaasConf());
                kafkaJaasTextPair.setFirst(true);
            }
        }
        if (StringUtils.isNotEmpty(kafkaJaasTextPair.getSecond())) {
            props.put(SASL_JAAS_CONFIG, kafkaJaasTextPair.getSecond());
        }
    }
}
