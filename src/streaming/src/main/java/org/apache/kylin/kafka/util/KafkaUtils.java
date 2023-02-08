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

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.streaming.jobs.StreamingJobUtils;

public class KafkaUtils {

    /**
     * load jaas file's text into kafkaJaasTextPair to avoid multiple read file
     */
    private static final Pair<Boolean, String> kafkaJaasTextPair = new Pair<>(false, null);

    // for test
    private static Consumer<String, ByteBuffer> mockup;

    private static final String BOOTSTRAP_SERVER = "bootstrap.servers";
    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String GROUP_ID = "group.id";
    private static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";

    private KafkaUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static AdminClient getKafkaAdminClient(String brokers, String groupId) {
        return getKafkaAdminClient(brokers, groupId, new Properties());
    }

    public static AdminClient getKafkaAdminClient(String brokers, String groupId, Properties properties) {
        Properties props = getAdminClientProperties(brokers, groupId, properties);
        return AdminClient.create(props);
    }

    public static Consumer<String, ByteBuffer> getKafkaConsumer(String brokers, String groupId) {
        return getKafkaConsumer(brokers, groupId, new Properties());
    }

    public static Consumer<String, ByteBuffer> getKafkaConsumer(String brokers, String groupId, Properties properties) {
        Properties props = getConsumerProperties(brokers, groupId, properties);
        if (mockup != null) {
            return mockup;
        } else {
            return new KafkaConsumer<>(props);
        }
    }

    public static Properties getAdminClientProperties(String brokers, String consumerGroup, Properties properties) {
        Properties props = new Properties();
        setSaslJaasConf(props);
        props.put(BOOTSTRAP_SERVER, brokers);
        props.put(GROUP_ID, consumerGroup);
        if (MapUtils.isNotEmpty(properties)) {
            props.putAll(properties);
        }
        return props;
    }

    public static Properties getConsumerProperties(String brokers, String consumerGroup, Properties properties) {
        Properties props = new Properties();
        setSaslJaasConf(props);
        props.put(BOOTSTRAP_SERVER, brokers);
        props.put(KEY_DESERIALIZER, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER, ByteBufferDeserializer.class.getName());
        props.put(GROUP_ID, consumerGroup);
        props.put(ENABLE_AUTO_COMMIT, "false");
        if (MapUtils.isNotEmpty(properties)) {
            props.putAll(properties);
        }
        return props;
    }

    private static void setSaslJaasConf(Properties props) {
        props.putAll(KylinConfig.getInstanceFromEnv().getStreamingKafkaConfigOverride());
        synchronized (kafkaJaasTextPair) {
            if (Boolean.FALSE.equals(kafkaJaasTextPair.getFirst())) {
                kafkaJaasTextPair.setSecond(StreamingJobUtils.extractKafkaJaasConf(true));
                kafkaJaasTextPair.setFirst(true);
            }
        }
        if (StringUtils.isNotEmpty(kafkaJaasTextPair.getSecond())) {
            props.put(SASL_JAAS_CONFIG, kafkaJaasTextPair.getSecond());
        }
    }
}
