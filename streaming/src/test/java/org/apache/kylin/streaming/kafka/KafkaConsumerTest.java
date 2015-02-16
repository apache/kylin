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

import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 * Created by qianzhou on 2/16/15.
 */
public class KafkaConsumerTest extends KafkaBaseTest {

    private TestProducer producer;

    private static final int TOTAL_SEND_COUNT = 100;

    @Before
    public void before() {
        producer = new TestProducer(TOTAL_SEND_COUNT);
        producer.start();
    }

    @After
    public void after() {
        producer.stop();
    }

    private void waitForProducerToStop(TestProducer producer) {
        while (!producer.isStopped()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test() throws InterruptedException {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopic(TestConstants.TOPIC);
        topicConfig.setBrokers(Collections.singletonList(TestConstants.BROKER));
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setBufferSize(64 * 1024);
        consumerConfig.setMaxReadCount(1000);
        consumerConfig.setTimeout(60 * 1000);
        final TopicMeta kafkaTopicMeta = Requester.getKafkaTopicMeta(topicConfig, consumerConfig);
        final ExecutorService executorService = Executors.newFixedThreadPool(kafkaTopicMeta.getPartitionIds().size());
        List<BlockingQueue<Stream>> queues = Lists.newArrayList();
        for (Integer partitionId : kafkaTopicMeta.getPartitionIds()) {
            Consumer consumer = new Consumer(kafkaTopicMeta.getName(), partitionId, Lists.asList(TestConstants.BROKER, new Broker[0]), consumerConfig);
            queues.add(consumer.getStreamQueue());
            executorService.execute(consumer);
        }
        waitForProducerToStop(producer);
        int count = 0;
        for (BlockingQueue<Stream> queue : queues) {
            count += queue.size();
        }
        //since there will be historical data
        assertTrue(count >= TOTAL_SEND_COUNT);
    }
}
