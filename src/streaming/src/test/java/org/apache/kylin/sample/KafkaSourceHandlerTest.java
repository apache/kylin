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

package org.apache.kylin.sample;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.STREAMING_PARSE_MESSAGE_ERROR;
import static org.apache.kylin.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.kafka.util.KafkaUtils;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import lombok.val;

public class KafkaSourceHandlerTest extends NLocalFileMetadataTestCase {

    private static final StreamingSourceHandler sourceHandler = Mockito.spy(new KafkaSourceHandler());
    private static final String BROKER_SERVER = "localhost:19093";
    private static final KafkaConfig kafkaConfig = new KafkaConfig();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        init();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    public void init() {
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setName("P_LINEORDER");
        kafkaConfig.setProject("streaming_test");
        kafkaConfig.setKafkaBootstrapServers(BROKER_SERVER);
        kafkaConfig.setSubscribe("ssb-topic1");
        kafkaConfig.setStartingOffsets("latest");
        kafkaConfig.setBatchTable("");
        kafkaConfig.setParserName(DEFAULT_PARSER_NAME);
    }

    @Test
    public void testParseMessage() {

        {
            String msg = "{\"timestamp\": \"2000-01-01 05:06:12\"}";
            Map<String, Object> map = sourceHandler.parserMessage(kafkaConfig, msg);
            Assert.assertEquals(1, map.size());
            Assert.assertTrue(map.containsKey("timestamp"));
        }

        {
            String msg = "{\"times[tamp]\": \"2000-01-01 05:06:12\"}";
            Assert.assertThrows(CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED.getCodeMsg(), KylinException.class,
                    () -> sourceHandler.parserMessage(kafkaConfig, msg));
        }

        {
            String msg = "{timestamp: \"2000-01-01 05:06:12\"}";
            Assert.assertThrows(
                    STREAMING_PARSE_MESSAGE_ERROR.getMsg(kafkaConfig.getParserName(), kafkaConfig.getSubscribe()),
                    KylinException.class, () -> sourceHandler.parserMessage(kafkaConfig, msg));
        }
    }

    @Test
    public void testGetMessage() {
        val topic = "ssb-topic1";
        val partition = 0;

        {
            setupMockConsumer(topic, partition, true, 7);
            val messages = sourceHandler.getMessages(kafkaConfig);
            Assert.assertEquals(7, messages.size());
        }

        {
            setupMockConsumer(topic, partition, true, 11);
            val messages = sourceHandler.getMessages(kafkaConfig);
            Assert.assertEquals(10, messages.size());
        }

        {
            setupMockConsumer(topic, partition, false, 0);
            val messages = sourceHandler.getMessages(kafkaConfig);
            Assert.assertEquals(0, messages.size());
        }
    }

    @Test
    public void testBrokenBrokers() {
        val topic = "ssb-topic1";
        val partition = 0;
        setupMockConsumer(topic, partition, false, 0);

        {
            kafkaConfig.setKafkaBootstrapServers("1.1.1.1:9092,2.2.2.2:9092,3.3.3.3:9092");
            val brokenBrokers = sourceHandler.getBrokenBrokers(kafkaConfig);
            Assert.assertEquals(3, brokenBrokers.size());
        }

        {
            kafkaConfig.setKafkaBootstrapServers(BROKER_SERVER);
            val brokenBrokers = sourceHandler.getBrokenBrokers(kafkaConfig);
            Assert.assertEquals(1, brokenBrokers.size());
        }
    }

    @Test
    public void testGetTopics() {
        val topic = "ssb-topic1";
        val partition = 0;
        {
            setupMockConsumer(topic, partition, false, 0);
            val topics = sourceHandler.getTopics(kafkaConfig, topic);
            Assert.assertEquals(1, topics.get("kafka-cluster-1").size());
        }

        {
            setupMockConsumer(topic, partition, false, 0);
            val topics = sourceHandler.getTopics(kafkaConfig, "");
            Assert.assertEquals(1, topics.get("kafka-cluster-1").size());
        }
    }

    private void setupMockConsumer(String topic, int partition, boolean addMsg, int msgCnt) {
        ReflectionUtils.setField(KafkaUtils.class, "mockup", new MockConsumer<>(OffsetResetStrategy.LATEST));
        val mockup = (MockConsumer<String, ByteBuffer>) ReflectionUtils.getField(KafkaUtils.class, "mockup");
        mockup.assign(Collections.singletonList(new TopicPartition(topic, partition)));
        mockup.updatePartitions(topic,
                Collections.singletonList(new PartitionInfo(topic, partition, null, new Node[0], new Node[0])));
        val beginningOffsets = new HashMap<TopicPartition, Long>();
        beginningOffsets.put(new TopicPartition(topic, partition), 0L);
        mockup.updateBeginningOffsets(beginningOffsets);
        if (!addMsg) {
            val endOffsets = new HashMap<TopicPartition, Long>();
            endOffsets.put(new TopicPartition(topic, partition), 0L);
            mockup.updateEndOffsets(endOffsets);
            return;
        }
        for (int i = 0; i < msgCnt; i++) {
            val value = ByteBuffer.allocate(10);
            value.put(("msg-" + i).getBytes());
            value.flip();
            val rec = new ConsumerRecord<String, ByteBuffer>(topic, partition, i, null, value);
            mockup.addRecord(rec);
        }
        val endOffsets = new HashMap<TopicPartition, Long>();
        endOffsets.put(new TopicPartition(topic, partition), (long) msgCnt);
        mockup.updateEndOffsets(endOffsets);
    }
}
