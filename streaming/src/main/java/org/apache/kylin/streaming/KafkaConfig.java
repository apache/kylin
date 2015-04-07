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

package org.apache.kylin.streaming;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;

import javax.annotation.Nullable;
import java.io.*;
import java.util.List;

/**
 * Created by qianzhou on 3/2/15.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class KafkaConfig extends RootPersistentEntity {

    public static Serializer<KafkaConfig> SERIALIZER = new JsonSerializer<KafkaConfig>(KafkaConfig.class);

    @JsonProperty("name")
    private String name;

    @JsonProperty("brokers")
    private List<BrokerConfig> brokerConfigs;

    @JsonProperty("zookeeper")
    private String zookeeper;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("timeout")
    private int timeout;

    @JsonProperty("maxReadCount")
    private int maxReadCount;

    @JsonProperty("bufferSize")
    private int bufferSize;

    @JsonProperty("iiName")
    private String iiName;

    @JsonProperty("parserName")
    private String parserName;

    @JsonProperty("partition")
    private int partition;

    public String getParserName() {
        return parserName;
    }

    public void setParserName(String parserName) {
        this.parserName = parserName;
    }


    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getMaxReadCount() {
        return maxReadCount;
    }

    public void setMaxReadCount(int maxReadCount) {
        this.maxReadCount = maxReadCount;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setBrokerConfigs(List<BrokerConfig> brokerConfigs) {
        this.brokerConfigs = brokerConfigs;
    }

    public List<Broker> getBrokers() {
        return Lists.transform(brokerConfigs, new Function<BrokerConfig, Broker>() {
            @Nullable
            @Override
            public Broker apply(BrokerConfig input) {
                return new Broker(input.getId(), input.getHost(), input.getPort());
            }
        });
    }

    public String getIiName() {
        return iiName;
    }

    public void setIiName(String iiName) {
        this.iiName = iiName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public KafkaConfig clone() {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SERIALIZER.serialize(this, new DataOutputStream(baos));
            return SERIALIZER.deserialize(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        } catch (IOException e) {
            return null;//in mem, should not happen
        }
    }
}
