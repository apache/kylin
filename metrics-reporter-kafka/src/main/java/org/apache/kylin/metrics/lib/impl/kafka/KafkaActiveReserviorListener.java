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

package org.apache.kylin.metrics.lib.impl.kafka;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaActiveReserviorListener implements ActiveReservoirListener {

    public static final long TOPIC_AVAILABLE_TAG = 0L;
    protected static final Logger logger = LoggerFactory.getLogger(KafkaActiveReserviorListener.class);
    protected Long maxBlockMs = 1800000L;
    protected int maxRecordForLogNum = KylinConfig.getInstanceFromEnv().printSampleEventRatio();
    protected int maxRecordSkipForLogNum = 10000;
    protected ConcurrentHashMap<String, Long> topicsIfAvailable = new ConcurrentHashMap<>();
    private long nRecord = 0;
    private long nRecordSkip = 0;
    private int threshold = Integer.min((int)(maxRecordForLogNum * 0.002), 25);

    private Callback produceCallback = (RecordMetadata metadata, Exception exception) -> {
        if(exception != null){
            logger.warn("Unexpected exception.",  exception);
        } else {
            logger.debug("Topic:{} ; partition:{} ; offset:{} .", metadata.topic(), metadata.partition(), metadata.offset());
        }
    };

    protected abstract String decorateTopic(String topic);

    protected abstract void tryFetchMetadataFor(String topic);

    protected abstract void send(String topic, Record record, Callback callback);

    protected void sendWrapper(String topic, Record record, Callback callback) {
        try {
            send(topic, record, callback);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            setUnAvailable(topic);
            throw e;
        }
    }

    public boolean onRecordUpdate(final List<Record> records) {
        try {
            for (Record record : records) {
                String topic = decorateTopic(record.getSubject());
                if (nRecord <= threshold) {
                    logger.debug("Send record {} to topic : {}", record, topic);
                }
                if (!checkAvailable(topic)) {
                    if (nRecordSkip % maxRecordSkipForLogNum == 0) {
                        nRecordSkip = 0;
                        logger.warn("Skip to send record to topic {}", topic);
                    }
                    nRecordSkip++;
                    continue;
                }
                if (nRecord % maxRecordForLogNum == 0) {
                    sendWrapper(topic, record, produceCallback);
                } else {
                    sendWrapper(topic, record, null);
                }
                nRecord++;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    protected boolean checkAvailable(String topic) {
        Long timeBlock = topicsIfAvailable.get(topic);
        if (timeBlock != null && timeBlock == TOPIC_AVAILABLE_TAG) {
            return true;
        } else if (timeBlock == null || System.currentTimeMillis() - timeBlock > maxBlockMs) {
            try {
                tryFetchMetadataFor(topic);
                topicsIfAvailable.put(topic, TOPIC_AVAILABLE_TAG);
                return true;
            } catch (org.apache.kafka.common.errors.TimeoutException e) {
                logger.warn("Fail to fetch metadata for topic " + topic, e);
                setUnAvailable(topic);
                return false;
            }
        }
        return false;
    }

    protected void setUnAvailable(String topic) {
        logger.debug("Cannot find topic {}", topic);
        topicsIfAvailable.put(topic, System.currentTimeMillis());
    }
}
