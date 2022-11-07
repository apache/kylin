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

package org.apache.kylin.rest.service;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.kafka.CollectKafkaStats;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class KafkaServiceTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private final KafkaService kafkaService = Mockito.spy(KafkaService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private static final String brokerServer = "localhost:19093";
    private static final String PROJECT = "streaming_test";

    KafkaConfig kafkaConfig = new KafkaConfig();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(kafkaService, "aclEvaluate", aclEvaluate);
        init();
    }

    public void init() {
        ReflectionTestUtils.setField(kafkaConfig, "database", "SSB");
        ReflectionTestUtils.setField(kafkaConfig, "name", "P_LINEORDER");
        ReflectionTestUtils.setField(kafkaConfig, "project", "streaming_test");
        ReflectionTestUtils.setField(kafkaConfig, "kafkaBootstrapServers", brokerServer);
        ReflectionTestUtils.setField(kafkaConfig, "subscribe", "ssb-topic1");
        ReflectionTestUtils.setField(kafkaConfig, "startingOffsets", "latest");
        ReflectionTestUtils.setField(kafkaConfig, "batchTable", "");
        ReflectionTestUtils.setField(kafkaConfig, "parserName", "org.apache.kylin.parser.TimedJsonStreamParser");
    }

    @Test
    public void testCheckBrokerStatus() {
        try {
            kafkaService.checkBrokerStatus(kafkaConfig);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            KylinException kylinException = (KylinException) e;
            ErrorResponse errorResponse = new ErrorResponse("http://localhost:7070/kylin/api/kafka/topics",
                    kylinException);
            Assert.assertEquals("http://localhost:7070/kylin/api/kafka/topics", errorResponse.url);
            val dataMap = (Map<String, Object>) errorResponse.getData();
            Assert.assertEquals(1, dataMap.size());
            Assert.assertEquals(Collections.singletonList(brokerServer), dataMap.get("failed_servers"));
        }
    }

    @Test
    public void testGetTopics() {
        expectedException.expect(KylinException.class);
        expectedException.expectMessage(Message.getInstance().getBrokerTimeoutMessage());
        kafkaService.getTopics(kafkaConfig, PROJECT, "test");
    }

    @Test
    public void testGetMessage() {
        expectedException.expect(KylinException.class);
        expectedException.expectMessage("Can’t get sample data. Please check and try again.");
        kafkaService.getMessages(kafkaConfig, PROJECT, 1);
    }

    @Test
    public void testGetMessageTypeAndDecodedMessages() {
        val value = ByteBuffer.allocate(10);
        value.put("msg-1".getBytes());
        value.flip();
        val messages = Arrays.asList(value);
        val decoded = kafkaService.getMessageTypeAndDecodedMessages(messages);
        val decodedMessages = (List) decoded.get("message");
        Assert.assertEquals(1, decodedMessages.size());
    }

    @Test
    public void testConvertSampleMessageToFlatMap() {
        val result = kafkaService.convertSampleMessageToFlatMap(kafkaConfig, CollectKafkaStats.JSON_MESSAGE,
                "{\"a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}");
        Assert.assertEquals(3, result.size());
    }
}
