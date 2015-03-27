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

import kafka.api.OffsetRequest;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

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

    private volatile boolean stop = false;

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
            while (!stop) {
                if (leadBroker == null) {
                    leadBroker = getLeadBroker();
                }
                if (leadBroker == null) {
                    logger.warn("cannot find lead broker");
                    continue;
                }

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
                }
            }
        } catch (Exception e) {
            logger.error("consumer has encountered an error", e);
        }
    }

    protected abstract void consume(long offset, ByteBuffer payload) throws Exception;

    public void stop() {
        this.stop = true;
    }

}
