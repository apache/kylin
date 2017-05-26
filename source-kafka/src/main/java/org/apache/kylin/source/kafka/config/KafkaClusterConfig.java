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

package org.apache.kylin.source.kafka.config;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import kafka.cluster.Broker;

/**
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class KafkaClusterConfig extends RootPersistentEntity {
    public static Serializer<KafkaClusterConfig> SERIALIZER = new JsonSerializer<KafkaClusterConfig>(KafkaClusterConfig.class);

    @JsonProperty("brokers")
    private List<BrokerConfig> brokerConfigs;

    @JsonBackReference
    private KafkaConfig kafkaConfig;

    public String getTopic() {
        return kafkaConfig.getTopic();
    }

    public int getTimeout() {
        return kafkaConfig.getTimeout();
    }

    public List<BrokerConfig> getBrokerConfigs() {
        return brokerConfigs;
    }

    public List<Broker> getBrokers() {
        return Lists.transform(brokerConfigs, new Function<BrokerConfig, Broker>() {
            @Nullable
            @Override
            public Broker apply(BrokerConfig input) {
                return new Broker(input.getId(), input.getHost(), input.getPort(), SecurityProtocol.PLAINTEXT);
            }
        });
    }
}
