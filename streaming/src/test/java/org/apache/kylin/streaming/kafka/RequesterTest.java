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

package org.apache.kylin.streaming.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by qianzhou on 2/16/15.
 */
public class RequesterTest extends KafkaBaseTest {

    private static TopicConfig topicConfig;
    private static ConsumerConfig consumerConfig;

    private static final String UNEXISTED_TOPIC = "unexist_topic";

    @BeforeClass
    public static void beforeClass() {
        topicConfig = new TopicConfig();
        topicConfig.setTopic(TestConstants.TOPIC);
        topicConfig.setBrokers(Collections.singletonList(TestConstants.BROKER));
        consumerConfig = new ConsumerConfig();
        consumerConfig.setBufferSize(64 * 1024);
        consumerConfig.setMaxReadCount(1000);
        consumerConfig.setTimeout(60 * 1000);
    }

    @AfterClass
    public static void afterClass() {
    }

    @Test
    public void testTopicMeta() throws Exception {
        TopicMeta kafkaTopicMeta = Requester.getKafkaTopicMeta(topicConfig, consumerConfig);
        assertNotNull(kafkaTopicMeta);
        assertEquals(2, kafkaTopicMeta.getPartitionIds().size());
        assertEquals(topicConfig.getTopic(), kafkaTopicMeta.getName());

        TopicConfig anotherTopicConfig = new TopicConfig();
        anotherTopicConfig.setBrokers(Collections.singletonList(TestConstants.BROKER));
        anotherTopicConfig.setTopic(UNEXISTED_TOPIC);

        kafkaTopicMeta = Requester.getKafkaTopicMeta(anotherTopicConfig, consumerConfig);
        assertTrue(kafkaTopicMeta == null);
    }
}
