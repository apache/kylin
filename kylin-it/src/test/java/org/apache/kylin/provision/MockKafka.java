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
package org.apache.kylin.provision;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;

public class MockKafka {
    private static Properties createProperties(ZkConnection zkServerConnection, String logDir, String port, String brokerId) {
        Properties properties = new Properties();
        properties.put("port", port);
        properties.put("broker.id", brokerId);
        properties.put("log.dirs", logDir);
        properties.put("host.name", "localhost");
        properties.put("offsets.topic.replication.factor", "1");
        properties.put("delete.topic.enable", "true");
        properties.put("zookeeper.connect", zkServerConnection.getServers());
        String ip = NetworkUtils.getLocalIp();
        properties.put("listeners", "PLAINTEXT://" + ip + ":" + port);
        properties.put("advertised.listeners", "PLAINTEXT://" + ip + ":" + port);
        return properties;
    }

    private KafkaServerStartable kafkaServer;
    private static final Logger logger = LoggerFactory.getLogger(MockKafka.class);

    private ZkConnection zkConnection;

    public MockKafka(ZkConnection zkServerConnection) {
        this(zkServerConnection, System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString(), "9092", "1");
        start();
    }

    private MockKafka(Properties properties) {
        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        kafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    public MockKafka(ZkConnection zkServerConnection, int port, int brokerId) {
        this(zkServerConnection, System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString(), String.valueOf(port), String.valueOf(brokerId));
        //start();
    }

    private MockKafka(ZkConnection zkServerConnection, String logDir, String port, String brokerId) {
        this(createProperties(zkServerConnection, logDir, port, brokerId));
        this.zkConnection = zkServerConnection;
        System.out.println(String.format("Kafka %s:%s dir:%s", kafkaServer.serverConfig().brokerId(), kafkaServer.serverConfig().port(), kafkaServer.serverConfig().logDirs()));
    }

    public void createTopic(String topic, int partition, int replication) {
        ZkClient zkClient = new ZkClient(zkConnection);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        zkClient.setZkSerializer(new ZKStringSerializer());
        AdminUtils.createTopic(zkUtils, topic, partition, replication, new Properties(), null);
        zkClient.close();
    }

    public void createTopic(String topic) {
        this.createTopic(topic, 1, 1);
    }

    public MetadataResponse.TopicMetadata fetchTopicMeta(String topic) {
        ZkClient zkClient = new ZkClient(zkConnection);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        zkClient.setZkSerializer(new ZKStringSerializer());
        MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
        zkClient.close();
        return topicMetadata;
    }

    /**
     * Delete may not work
     *
     * @param topic
     */
    public void deleteTopic(String topic) {
        ZkClient zkClient = new ZkClient(zkConnection);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        zkClient.setZkSerializer(new ZKStringSerializer());
        AdminUtils.deleteTopic(zkUtils, topic);
        zkClient.close();
    }

    public void start() {
        kafkaServer.startup();
        System.out.println("--embedded kafka is up");
    }

    public void stop() {
        kafkaServer.shutdown();
        System.out.println("embedded kafka down");
    }

    public MetadataResponse.TopicMetadata waitTopicUntilReady(String topic) {
        boolean isReady = false;
        MetadataResponse.TopicMetadata topicMeta = null;
        while (!isReady) {
            Random random = new Random();
            topicMeta = this.fetchTopicMeta(topic);
            List<MetadataResponse.PartitionMetadata> partitionsMetadata = topicMeta.partitionMetadata();
            Iterator<MetadataResponse.PartitionMetadata> iterator = partitionsMetadata.iterator();
            boolean hasGotLeader = true;
            boolean hasGotReplica = true;
            while (iterator.hasNext()) {
                MetadataResponse.PartitionMetadata partitionMeta = iterator.next();
                hasGotLeader &= (!partitionMeta.leader().isEmpty());
                if (partitionMeta.leader().isEmpty()) {
                    System.out.println("Partition leader is not ready, wait 1s.");
                    break;
                }
                hasGotReplica &= (!partitionMeta.replicas().isEmpty());
                if (partitionMeta.replicas().isEmpty()) {
                    System.out.println("Partition replica is not ready, wait 1s.");
                    break;
                }
            }
            isReady = hasGotLeader & hasGotReplica;
            if (!isReady) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
        return topicMeta;
    }

    public String getZookeeperConnection() {
        return this.zkConnection.getServers();
    }
}

class ZKStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        byte[] bytes = null;
        try {
            bytes = data.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
        return bytes;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        if (bytes == null)
            return null;
        else
            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
    }

}