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

import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by qianzhou on 2/15/15.
 */
public class Consumer implements Runnable {

    private String topic;
    private int partitionId;

    private ConsumerConfig consumerConfig;
    private List<Broker> replicaBrokers;
    private AtomicLong offset = new AtomicLong();
    private BlockingQueue<Stream> streamQueue;

    private Logger logger;

    public Consumer(String topic, int partitionId, List<Broker> initialBrokers, ConsumerConfig consumerConfig) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.consumerConfig = consumerConfig;
        this.replicaBrokers = initialBrokers;
        logger = LoggerFactory.getLogger("KafkaConsumer_" + topic + "_" + partitionId);
        streamQueue = new ArrayBlockingQueue<Stream>(consumerConfig.getMaxReadCount());
    }

    public BlockingQueue<Stream> getStreamQueue() {
        return streamQueue;
    }

    private Broker getLeadBroker() {
        final PartitionMetadata partitionMetadata = Requester.getPartitionMetadata(topic, partitionId, replicaBrokers, consumerConfig);
        if (partitionMetadata != null && partitionMetadata.errorCode() == 0) {
            replicaBrokers = partitionMetadata.replicas();
            return partitionMetadata.leader();
        } else {
            return null;
        }
    }

    @Override
    public void run() {
        while (true) {
            final Broker leadBroker = getLeadBroker();
            if (leadBroker == null) {
                logger.warn("cannot find lead broker");
                continue;
            }
            final FetchResponse fetchResponse = Requester.fetchResponse(topic, partitionId, offset.get(), leadBroker, consumerConfig);
            if (fetchResponse.errorCode(topic, partitionId) != 0) {
                logger.warn("fetch response offset:" + offset.get() + " errorCode:" + fetchResponse.errorCode(topic, partitionId));
                continue;
            }
            for (MessageAndOffset messageAndOffset: fetchResponse.messageSet(topic, partitionId)) {
                final ByteBuffer payload = messageAndOffset.message().payload();
                //TODO use ByteBuffer maybe
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                logger.debug("get message offset:" + messageAndOffset.offset());
                try {
                    streamQueue.put(new Stream(System.currentTimeMillis(), bytes));
                } catch (InterruptedException e) {
                    logger.error("error put streamQueue", e);
                }
                offset.incrementAndGet();
            }
        }
    }

}
