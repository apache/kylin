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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.streaming.invertedindex.BrokerConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

/**
 * Created by qianzhou on 3/2/15.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class KafkaConfig extends RootPersistentEntity {

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

    private int partitionId;

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

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
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

    //    public static KafkaConfig load(KafkaConfig config) {
//        KafkaConfig result = new KafkaConfig();
//        result.setBufferSize(config.getBufferSize());
//        result.setMaxReadCount(config.getMaxReadCount());
//        result.setTimeout(config.getTimeout());
//        result.setTopic(config.getTopic());
//        result.setZookeeper(config.getZookeeper());
//        result.setPartitionId(config.getPartitionId());
//        result.setBrokers(config.getBrokers());
//        return result;
//    }
//
//    public static KafkaConfig load(Properties properties) {
//        Preconditions.checkNotNull(properties);
//        KafkaConfig result = new KafkaConfig();
//        result.setBufferSize(Integer.parseInt(properties.getProperty("consumer.bufferSize")));
//        result.setMaxReadCount(Integer.parseInt(properties.getProperty("consumer.maxReadCount")));
//        result.setTimeout(Integer.parseInt(properties.getProperty("consumer.timeout")));
//        result.setTopic(properties.getProperty("topic"));
//        result.setZookeeper(properties.getProperty("zookeeper"));
//        result.setPartitionId(Integer.parseInt(properties.getProperty("partitionId")));
//
//        int id = 0;
//        List<Broker> brokers = Lists.newArrayList();
//        for (String str: properties.getProperty("brokers").split(",")) {
//            final String[] split = str.split(":");
//            final Broker broker = new Broker(id++, split[0], Integer.parseInt(split[1]));
//            brokers.add(broker);
//        }
//        result.setBrokers(brokers);
//        return result;
//    }
}
