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

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private long getEarliestStreamingOffset(String streaming, int startShard, int endShard) {
        long result = Long.MAX_VALUE;
        for (int i = startShard; i < endShard; ++i) {
            final long offset = streamingManager.getOffset(streaming, i);
            if (offset < result) {
                result = offset;
            }
        }
        return result;
    }

    public void start(String streaming, int partitionId) throws Exception {
        final KafkaConfig kafkaConfig = streamingManager.getKafkaConfig(streaming);
        Preconditions.checkArgument(kafkaConfig != null, "cannot find kafka config:" + streaming);
        final IIInstance ii = iiManager.getII(kafkaConfig.getIiName());
        Preconditions.checkNotNull(ii, "cannot find ii name:" + kafkaConfig.getIiName());
        Preconditions.checkArgument(partitionId >= 0 && partitionId < kafkaConfig.getPartition(), "invalid partition id:" + partitionId);
        Preconditions.checkArgument(ii.getSegments().size() > 0);
        final IISegment iiSegment = ii.getSegments().get(0);

        final Broker leadBroker = getLeadBroker(kafkaConfig, partitionId);
        Preconditions.checkState(leadBroker != null, "cannot find lead broker");
        final int partition = kafkaConfig.getPartition();
        final int shard = ii.getDescriptor().getSharding();
        Preconditions.checkArgument(shard % partition == 0);
        final int parallelism = shard / partition;
        final int startShard = partitionId * parallelism;
        final int endShard = startShard + parallelism;
        long streamingOffset = getEarliestStreamingOffset(streaming, startShard, endShard);
        streamingOffset = streamingOffset - (streamingOffset % parallelism);
        logger.info("offset from ii desc is " + streamingOffset);
        final long earliestOffset = KafkaRequester.getLastOffset(kafkaConfig.getTopic(), partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaConfig);
        logger.info("offset from KafkaRequester is " + earliestOffset);
        streamingOffset = Math.max(streamingOffset, earliestOffset);
        logger.info("starting offset is " + streamingOffset);

        if (!HBaseConnection.tableExists(kylinConfig.getStorageUrl(), iiSegment.getStorageLocationIdentifier())) {
            logger.error("no htable:" + iiSegment.getStorageLocationIdentifier() + " found");
            throw new IllegalStateException("please create htable:" + iiSegment.getStorageLocationIdentifier() + " first");
        }


        KafkaConsumer consumer = new KafkaConsumer(kafkaConfig.getTopic(), partitionId, streamingOffset, kafkaConfig.getBrokers(), kafkaConfig, parallelism);
        kafkaConsumers.put(getKey(streaming, partitionId), consumer);

        StreamParser parser;
        if (!StringUtils.isEmpty(kafkaConfig.getParserName())) {
            Class clazz = Class.forName(kafkaConfig.getParserName());
            Constructor constructor = clazz.getConstructor(List.class);
            parser = (StreamParser) constructor.newInstance(ii.getDescriptor().listAllColumns());
        } else {
            parser = new JsonStreamParser(ii.getDescriptor().listAllColumns());
        }
        Executors.newSingleThreadExecutor().submit(consumer);
        final ExecutorService streamingBuilderPool = Executors.newFixedThreadPool(parallelism);
        for (int i = startShard; i < endShard; ++i) {
            final IIStreamBuilder task = new IIStreamBuilder(consumer.getStreamQueue(i % parallelism), streaming, iiSegment.getStorageLocationIdentifier(), iiSegment.getIIDesc(), i);
            task.setStreamParser(parser);
            if (i == endShard - 1) {
                streamingBuilderPool.submit(task).get();
            } else {
                streamingBuilderPool.submit(task);
            }
        }


    }

    private String getKey(String streaming, int partitionId) {
        return streaming + "_" + partitionId;
    }
}
