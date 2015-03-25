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

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 * Created by qianzhou on 2/16/15.
 */
public class KafkaConsumerTest extends KafkaBaseTest {

    private OneOffStreamProducer producer;

    private static final int TOTAL_SEND_COUNT = 100;

    private KafkaConfig kafkaConfig;

    @Before
    public void before() throws IOException {
        producer = new OneOffStreamProducer(TOTAL_SEND_COUNT);
        producer.start();
        kafkaConfig = StreamManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig("kafka_test");
    }

    @After
    public void after() {
        producer.stop();
    }

    private void waitForProducerToStop(OneOffStreamProducer producer) {
        while (!producer.isStopped()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @Ignore("since ci does not have the topic")
    public void test() throws InterruptedException {
        final TopicMeta kafkaTopicMeta = KafkaRequester.getKafkaTopicMeta(kafkaConfig);
        final ExecutorService executorService = Executors.newFixedThreadPool(kafkaTopicMeta.getPartitionIds().size());
        List<BlockingQueue<Stream>> queues = Lists.newArrayList();
        for (Integer partitionId : kafkaTopicMeta.getPartitionIds()) {
            KafkaConsumer consumer = new KafkaConsumer(kafkaTopicMeta.getName(), partitionId, kafkaConfig.getBrokers(), kafkaConfig) {
                @Override
                protected void consume(long offset, ByteBuffer payload) throws Exception {
                    //TODO use ByteBuffer maybe
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);
                    logger.info("get message offset:" + offset);
                    getStreamQueue().put(new Stream(offset, bytes));
                }
            };
            queues.add(consumer.getStreamQueue());
            executorService.execute(consumer);
        }
        waitForProducerToStop(producer);

        //wait some time to ensure consumer has fetched all data
        Thread.sleep(5000);
        int count = 0;
        for (BlockingQueue<Stream> queue : queues) {
            count += queue.size();
        }

        logger.info("count of messages are " + count);
        //since there will be historical data
        assertTrue(count >= TOTAL_SEND_COUNT && (count % TOTAL_SEND_COUNT == 0));
    }
}
