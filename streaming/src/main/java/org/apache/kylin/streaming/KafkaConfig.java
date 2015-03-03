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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import kafka.cluster.Broker;

import java.util.List;
import java.util.Properties;

/**
 * Created by qianzhou on 3/2/15.
 */
public class KafkaConfig {

    private List<Broker> brokers;

    private String zookeeper;

    private String topic;

    private int timeout;

    private int maxReadCount;

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

    public List<Broker> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<Broker> brokers) {
        this.brokers = brokers;
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

    public static KafkaConfig load(KafkaConfig config) {
        KafkaConfig result = new KafkaConfig();
        result.setBufferSize(config.getBufferSize());
        result.setMaxReadCount(config.getMaxReadCount());
        result.setTimeout(config.getTimeout());
        result.setTopic(config.getTopic());
        result.setZookeeper(config.getZookeeper());
        result.setPartitionId(config.getPartitionId());
        result.setBrokers(config.getBrokers());
        return result;
    }

    public static KafkaConfig load(Properties properties) {
        Preconditions.checkNotNull(properties);
        KafkaConfig result = new KafkaConfig();
        result.setBufferSize(Integer.parseInt(properties.getProperty("consumer.bufferSize")));
        result.setMaxReadCount(Integer.parseInt(properties.getProperty("consumer.maxReadCount")));
        result.setTimeout(Integer.parseInt(properties.getProperty("consumer.timeout")));
        result.setTopic(properties.getProperty("topic"));
        result.setZookeeper(properties.getProperty("zookeeper"));
        result.setPartitionId(Integer.parseInt(properties.getProperty("partitionId")));

        int id = 0;
        List<Broker> brokers = Lists.newArrayList();
        for (String str: properties.getProperty("brokers").split(",")) {
            final String[] split = str.split(":");
            final Broker broker = new Broker(id++, split[0], Integer.parseInt(split[1]));
            brokers.add(broker);
        }
        result.setBrokers(brokers);
        return result;
    }
}
