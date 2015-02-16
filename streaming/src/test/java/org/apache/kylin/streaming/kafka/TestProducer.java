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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by qianzhou on 2/16/15.
 */
public class TestProducer {

    private volatile boolean stopped = false;

    private static final Logger logger = LoggerFactory.getLogger(TestConstants.class);

    private final int sendCount;

    public TestProducer(int sendCount) {
        this.sendCount = sendCount;
    }

    public void start() {
        Properties props = new Properties();
        props.put("metadata.broker.list", TestConstants.BROKER.getConnectionString());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        final Producer<String, String> producer = new Producer<String, String>(config);

        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                int count = 0;
                while (!stopped && count < sendCount) {
                    final KeyedMessage<String, String> message = new KeyedMessage<>(TestConstants.TOPIC, "current time is:" + System.currentTimeMillis());
                    producer.send(message);
                    count++;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                logger.debug("totally " + count +" messages have been sent");
                stopped = true;

            }
        });
        thread.setDaemon(false);
        thread.start();
    }

    public boolean isStopped() {
        return stopped;
    }

    public void stop() {
        stopped = true;
    }
}
