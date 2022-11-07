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
package org.apache.kylin.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.kafka.util.KafkaClient;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class KafkaTableUtilTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    String brokerServer = "localhost:19093";
    KafkaConfig kafkaConfig = new KafkaConfig();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        init();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    public void init() {
        ReflectionUtils.setField(kafkaConfig, "database", "SSB");
        ReflectionUtils.setField(kafkaConfig, "name", "P_LINEORDER");
        ReflectionUtils.setField(kafkaConfig, "project", "streaming_test");
        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", brokerServer);
        ReflectionUtils.setField(kafkaConfig, "subscribe", "ssb-topic1");
        ReflectionUtils.setField(kafkaConfig, "startingOffsets", "latest");
        ReflectionUtils.setField(kafkaConfig, "batchTable", "");
        ReflectionUtils.setField(kafkaConfig, "parserName", "org.apache.kylin.parser.TimedJsonStreamParser");
    }

    @Test
    public void testConstructMethod() {
        val constructors = KafkaTableUtil.class.getDeclaredConstructors();
        Assert.assertTrue(constructors.length == 1);

        try {
            constructors[0].setAccessible(true);
            constructors[0].newInstance();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetMessages() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);

        val messages = KafkaTableUtil.getMessages(kafkaConfig, 1);
        Assert.assertEquals(7, messages.size());

        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", "");
        try {
            KafkaTableUtil.getMessages(kafkaConfig, 1);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_BROKER_DEFINITION.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);
    }

    @Test
    public void testGetMessageTypeAndDecodedMessages() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);

        try {
            KafkaTableUtil.getMessageTypeAndDecodedMessages(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }
        try {
            KafkaTableUtil.getMessageTypeAndDecodedMessages(new ArrayList(0));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }

        val messages = KafkaTableUtil.getMessages(kafkaConfig, 1);
        val decoded = KafkaTableUtil.getMessageTypeAndDecodedMessages(messages);

        val decodedMessages = (List) decoded.get("message");
        Assert.assertEquals(7, decodedMessages.size());

        val msg = "{\"timestamp\": \"2000-01-01 05:06:12\"}";
        val base64Msg = new String(Base64.encodeBase64(msg.getBytes()));
        val buffer1 = ByteBuffer.allocate(msg.length());
        buffer1.put(msg.getBytes());
        buffer1.flip();
        val buffer2 = ByteBuffer.allocate(msg.length());
        buffer2.put(msg.getBytes());
        buffer2.flip();
        val msgList = Arrays.asList(buffer1, buffer2);
        val map = KafkaTableUtil.getMessageTypeAndDecodedMessages(msgList);
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(CollectKafkaStats.JSON_MESSAGE, map.get("message_type"));
        Assert.assertEquals(1, ((List) map.get("message")).size());
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);
    }

    @Test
    public void testValidateKafkaConfig() {
        Assert.assertTrue(KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers()));

        ReflectionUtils.setField(kafkaConfig, "kafkaBootstrapServers", "");
        Assert.assertFalse(KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers()));
        Assert.assertFalse(KafkaTableUtil.validateKafkaConfig(null));
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);
    }

    @Test
    public void testGetTopics() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);
        val topics = KafkaTableUtil.getTopics(kafkaConfig, topic);
        Assert.assertEquals(1, topics.get("kafka-cluster-1").size());
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);

        kafkaConfig.setKafkaBootstrapServers("");
        try {
            KafkaTableUtil.getTopics(kafkaConfig, topic);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_BROKER_DEFINITION.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void getBrokenBrokers() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);

        String brokenBrokerServer = "1.1.1.1:9092,2.2.2.2:9092,3.3.3.3:9092";
        kafkaConfig.setKafkaBootstrapServers(brokenBrokerServer);
        val brokenBrokers1 = KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        Assert.assertEquals(3, brokenBrokers1.size());

        val brokenBrokers = CollectKafkaStats.getBrokenBrokers(kafkaConfig);
        Assert.assertEquals(3, brokenBrokers.size());

        kafkaConfig.setKafkaBootstrapServers(brokerServer);
        val brokenBrokers2 = KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        Assert.assertEquals(1, brokenBrokers2.size());

        kafkaConfig.setKafkaBootstrapServers("");
        try {
            KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_BROKER_DEFINITION.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testConvertMessageToFlatMap() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);
        try {
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE, null);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE, "");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE, "test");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(null, CollectKafkaStats.JSON_MESSAGE, "test");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        val result = KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE,
                "{\"a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}");

        Assert.assertEquals(3, result.size());

        try {
            kafkaConfig.setParserName("test");
            KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE,
                    "{\"timestamp\": \"2000-01-01 05:06:12\"}");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KafkaTableUtil.convertMessageToFlatMap(null, CollectKafkaStats.JSON_MESSAGE,
                    "{\"timestamp\": \"2000-01-01 05:06:12\"}");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);
    }

    @Test
    public void testValidateStreamMessageType() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);
        KafkaTableUtil.validateStreamMessageType(CollectKafkaStats.JSON_MESSAGE);
        KafkaTableUtil.validateStreamMessageType(CollectKafkaStats.BINARY_MESSAGE);
        try {
            KafkaTableUtil.validateStreamMessageType("");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            KafkaTableUtil.validateStreamMessageType("InvalidMessageType");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_STREAMING_MESSAGE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);
    }

    @Test
    public void testDeserializeSampleMessage() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, 7);
        try {
            KafkaTableUtil.deserializeSampleMessage(CollectKafkaStats.JSON_MESSAGE, "a");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        val buff = KafkaTableUtil.deserializeSampleMessage(CollectKafkaStats.JSON_MESSAGE, "{}");
        Assert.assertEquals("{}", new String(buff.array()));

        val msg = "{\"timestamp\": \"2000-01-01 05:06:12\"}";
        val base64Msg = new String(Base64.encodeBase64(msg.getBytes()));
        val base64Buff = KafkaTableUtil.deserializeSampleMessage(CollectKafkaStats.BINARY_MESSAGE, base64Msg);

        Assert.assertEquals(msg, new String(base64Buff.array()));

        try {
            KafkaTableUtil.deserializeSampleMessage("error-type", "{}");
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_PARSER_ERROR.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
        ReflectionUtils.setField(KafkaClient.class, "mockup", null);
    }

    private MockConsumer setupMockConsumer(String topic, int partition, int msgCnt) {
        ReflectionUtils.setField(KafkaClient.class, "mockup", new MockConsumer<>(OffsetResetStrategy.LATEST));
        val mockup = (MockConsumer) ReflectionUtils.getField(KafkaClient.class, "mockup");
        mockup.assign(Arrays.asList(new TopicPartition(topic, partition)));
        mockup.updatePartitions(topic,
                Arrays.asList(new PartitionInfo(topic, partition, null, new Node[0], new Node[0])));
        val beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(topic, partition), 0L);
        mockup.updateBeginningOffsets(beginningOffsets);
        for (int i = 0; i < msgCnt; i++) {
            val value = ByteBuffer.allocate(10);
            value.put(("msg-" + i).getBytes());
            value.flip();
            val rec = new ConsumerRecord<String, ByteBuffer>(topic, partition, i, null, value);
            mockup.addRecord(rec);
        }
        val endOffsets = new HashMap<>();
        endOffsets.put(new TopicPartition(topic, partition), 7L);
        mockup.updateEndOffsets(endOffsets);
        return mockup;
    }

}
