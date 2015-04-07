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

package org.apache.kylin.job.streaming;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.streaming.*;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by qianzhou on 3/26/15.
 */
public class StreamingBootstrap {

    private static Logger logger = LoggerFactory.getLogger(StreamingBootstrap.class);

    private KylinConfig kylinConfig;
    private StreamingManager streamingManager;
    private IIManager iiManager;

    private Map<String, KafkaConsumer> kafkaConsumers = Maps.newConcurrentMap();

    public static StreamingBootstrap getInstance(KylinConfig kylinConfig) {
        final StreamingBootstrap bootstrap = new StreamingBootstrap(kylinConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                bootstrap.stop();
            }
        }));
        return bootstrap;
    }

    private StreamingBootstrap(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.streamingManager = StreamingManager.getInstance(kylinConfig);
        this.iiManager = IIManager.getInstance(kylinConfig);
    }

    private static Broker getLeadBroker(KafkaConfig kafkaConfig, int partitionId) {
        final PartitionMetadata partitionMetadata = KafkaRequester.getPartitionMetadata(kafkaConfig.getTopic(), partitionId, kafkaConfig.getBrokers(), kafkaConfig);
        if (partitionMetadata != null && partitionMetadata.errorCode() == 0) {
            return partitionMetadata.leader();
        } else {
            return null;
        }
    }

    public void stop() {
        for (KafkaConsumer consumer : kafkaConsumers.values()) {
            consumer.stop();
        }
    }

    public void start(String streaming, int partitionId) throws Exception {
        final KafkaConfig kafkaConfig = streamingManager.getKafkaConfig(streaming);
        Preconditions.checkArgument(kafkaConfig != null, "cannot find kafka config:" + streaming);
        final IIInstance ii = iiManager.getII(kafkaConfig.getIiName());
        Preconditions.checkNotNull(ii, "cannot find ii name:" + kafkaConfig.getIiName());
        Preconditions.checkArgument(partitionId >= 0 && partitionId < ii.getDescriptor().getSharding(), "invalid partition id:" + partitionId);
        Preconditions.checkArgument(ii.getSegments().size() > 0);
        final IISegment iiSegment = ii.getSegments().get(0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        KafkaConfig.SERIALIZER.serialize(kafkaConfig, new DataOutputStream(out));
        logger.debug("kafka config:" + new String(out.toByteArray()));
        final Broker leadBroker = getLeadBroker(kafkaConfig, partitionId);
        Preconditions.checkState(leadBroker != null, "cannot find lead broker");
        final long earliestOffset = KafkaRequester.getLastOffset(kafkaConfig.getTopic(), partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaConfig);
        long streamOffset = streamingManager.getOffset(streaming, partitionId);
        logger.info("offset from ii desc is " + streamOffset);
        logger.info("offset from KafkaRequester is " + earliestOffset);
        if (streamOffset < earliestOffset) {
            streamOffset = earliestOffset;
        }
        logger.info("offset is " + streamOffset);

        if (!HBaseConnection.tableExists(kylinConfig.getStorageUrl(), iiSegment.getStorageLocationIdentifier())) {
            logger.error("no htable:" + iiSegment.getStorageLocationIdentifier() + " found");
            throw new IllegalStateException("please create htable:" + iiSegment.getStorageLocationIdentifier() + " first");
        }

        KafkaConsumer consumer = new KafkaConsumer(kafkaConfig.getTopic(), partitionId, streamOffset, kafkaConfig.getBrokers(), kafkaConfig) {
            @Override
            protected void consume(long offset, ByteBuffer payload) {
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                Stream newStream = new Stream(offset, bytes);
                while (true) {
                    try {
                        if (getStreamQueue().offer(newStream, 60, TimeUnit.SECONDS)) {
                            break;
                        } else {
                            logger.info("the queue is full, wait for builder to catch up");
                        }
                    } catch (InterruptedException e) {
                        logger.info("InterruptedException", e);
                        continue;
                    }
                }
            }
        };
        kafkaConsumers.put(getKey(streaming, partitionId), consumer);

        final IIStreamBuilder task = new IIStreamBuilder(consumer.getStreamQueue(), streaming, iiSegment.getStorageLocationIdentifier(), iiSegment.getIIDesc(), partitionId);

        StreamParser parser;
        if (!StringUtils.isEmpty(kafkaConfig.getParserName())) {
            Class clazz = Class.forName(kafkaConfig.getParserName());
            Constructor constructor = clazz.getConstructor(List.class);
            parser = (StreamParser) constructor.newInstance(ii.getDescriptor().listAllColumns());
        } else {
            parser = new JsonStreamParser(ii.getDescriptor().listAllColumns());
        }
        task.setStreamParser(parser);

        Executors.newSingleThreadExecutor().submit(consumer);
        Executors.newSingleThreadExecutor().submit(task).get();
    }

    private String getKey(String streaming, int partitionId) {
        return streaming + "_" + partitionId;
    }
}
