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

import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by qianzhou on 2/15/15.
 */
public abstract class KafkaConsumer implements Runnable {

    private String topic;
    private int partitionId;

    private KafkaConfig kafkaConfig;
    private List<Broker> replicaBrokers;
    private long offset;
    private LinkedBlockingQueue<Stream> streamQueue;

    private Logger logger;

    private volatile boolean isRunning = true;

    public KafkaConsumer(String topic, int partitionId, long startOffset, List<Broker> initialBrokers, KafkaConfig kafkaConfig) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.kafkaConfig = kafkaConfig;
        offset = startOffset;
        this.replicaBrokers = initialBrokers;
        logger = LoggerFactory.getLogger("KafkaConsumer_" + topic + "_" + partitionId);
        streamQueue = new LinkedBlockingQueue<Stream>(kafkaConfig.getMaxReadCount());
    }

    public BlockingQueue<Stream> getStreamQueue() {
        return streamQueue;
    }

    private Broker getLeadBroker() {
        final PartitionMetadata partitionMetadata = KafkaRequester.getPartitionMetadata(topic, partitionId, replicaBrokers, kafkaConfig);
        if (partitionMetadata != null && partitionMetadata.errorCode() == 0) {
            replicaBrokers = partitionMetadata.replicas();
            return partitionMetadata.leader();
        } else {
            return null;
        }
    }

    @Override
    public void run() {
        try {
            Broker leadBroker = getLeadBroker();
            int consumeMsgCount = 0;
            int fetchRound = 0;
            while (isRunning) {
                int consumeMsgCountAtBeginning = consumeMsgCount;
                fetchRound++;
                logger.info("start " + fetchRound + "th round of fetching");

                if (leadBroker == null) {
                    leadBroker = getLeadBroker();
                }

                if (leadBroker == null) {
                    logger.warn("cannot find lead broker");
                    continue;
                }

                logger.info("fetching topic {} partition id {} offset {} leader {}", new String[] { topic, String.valueOf(partitionId), String.valueOf(offset), leadBroker.toString() });

                final FetchResponse fetchResponse = KafkaRequester.fetchResponse(topic, partitionId, offset, leadBroker, kafkaConfig);
                if (fetchResponse.errorCode(topic, partitionId) != 0) {
                    logger.warn("fetch response offset:" + offset + " errorCode:" + fetchResponse.errorCode(topic, partitionId));
                    continue;
                }

                for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)) {
                    try {
                        consume(messageAndOffset.offset(), messageAndOffset.message().payload());
                    } catch (Exception e) {
                        logger.error("error put streamQueue", e);
                        break;
                    }
                    offset++;
                    consumeMsgCount++;
                }
                logger.info("Number of messages consumed: " + consumeMsgCount + " offset is " + offset);

                if (consumeMsgCount == consumeMsgCountAtBeginning)//nothing this round
                {
                    Thread.sleep(5000);
                }
            }
            getStreamQueue().put(Stream.EOF);
        } catch (Exception e) {
            logger.error("consumer has encountered an error", e);
        }
    }

    protected abstract void consume(long offset, ByteBuffer payload) throws Exception;

    public void stop() {
        this.isRunning = false;
    }

}
