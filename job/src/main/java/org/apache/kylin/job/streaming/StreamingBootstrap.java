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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.*;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 */
public class StreamingBootstrap {

    private static Logger logger = LoggerFactory.getLogger(StreamingBootstrap.class);

    private KylinConfig kylinConfig;
    private StreamingManager streamingManager;

    private Map<String, KafkaConsumer> kafkaConsumers = Maps.newConcurrentMap();

    private StreamingBootstrap(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.streamingManager = StreamingManager.getInstance(kylinConfig);
    }

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

    private static Broker getLeadBroker(KafkaClusterConfig kafkaClusterConfig, int partitionId) {
        final PartitionMetadata partitionMetadata = KafkaRequester.getPartitionMetadata(kafkaClusterConfig.getTopic(), partitionId, kafkaClusterConfig.getBrokers(), kafkaClusterConfig);
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
        final StreamingConfig streamingConfig = streamingManager.getStreamingConfig(streaming);
        Preconditions.checkArgument(streamingConfig != null, "cannot find kafka config:" + streaming);

        if (!StringUtils.isEmpty(streamingConfig.getIiName())) {
            startIIStreaming(streamingConfig, partitionId);
        } else if (!StringUtils.isEmpty(streamingConfig.getCubeName())) {
            startCubeStreaming(streamingConfig, partitionId);
        } else {
            throw new IllegalArgumentException("no cube or ii in kafka config");
        }
    }

    private List<BlockingQueue<StreamMessage>> consume(KafkaClusterConfig kafkaClusterConfig, final int partitionCount) {
        List<BlockingQueue<StreamMessage>> result = Lists.newArrayList();
        for (int partitionId = 0; partitionId < partitionCount; ++partitionId) {
            final Broker leadBroker = getLeadBroker(kafkaClusterConfig, partitionId);

            final long latestOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, OffsetRequest.LatestTime(), leadBroker, kafkaClusterConfig);
            long streamingOffset = latestOffset;
            logger.info("submitting offset:" + streamingOffset);

            KafkaConsumer consumer = new KafkaConsumer(kafkaClusterConfig.getTopic(), partitionId, streamingOffset, kafkaClusterConfig.getBrokers(), kafkaClusterConfig, 1);
            Executors.newSingleThreadExecutor().submit(consumer);
            result.add(consumer.getStreamQueue(0));
        }
        return result;
    }

    private void startCubeStreaming(StreamingConfig streamingConfig, final int partitionId) throws Exception {
        List<KafkaClusterConfig> kafkaClusterConfigs = streamingConfig.getKafkaClusterConfigs();

        final List<List<BlockingQueue<StreamMessage>>> allClustersData = Lists.newArrayList();

        for (KafkaClusterConfig kafkaClusterConfig : kafkaClusterConfigs) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            Preconditions.checkArgument(partitionId >= 0 && partitionId < partitionCount, "invalid partition id:" + partitionId);

            final List<BlockingQueue<StreamMessage>> oneClusterData = consume(kafkaClusterConfig, partitionCount);
            logger.info("Cluster {} with {} partitions", allClustersData.size(), oneClusterData.size());
            allClustersData.add(oneClusterData);
        }

        final LinkedBlockingDeque<StreamMessage> alldata = new LinkedBlockingDeque<>();
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                int totalMessage = 0;
                while (true) {
                    for (List<BlockingQueue<StreamMessage>> oneCluster : allClustersData) {
                        for (BlockingQueue<StreamMessage> onePartition : oneCluster) {
                            try {
                                alldata.put(onePartition.take());
                                if (totalMessage++ % 10000 == 0) {
                                    logger.info("Total stream message count: " + totalMessage);
                                }
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            }
        });

        final String cubeName = streamingConfig.getCubeName();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

        CubeStreamBuilder cubeStreamBuilder = new CubeStreamBuilder(alldata, cubeName);
        cubeStreamBuilder.setStreamParser(getStreamParser(streamingConfig, cubeInstance.getAllColumns()));
        cubeStreamBuilder.setStreamFilter(getStreamFilter(streamingConfig));
        final Future<?> future = Executors.newSingleThreadExecutor().submit(cubeStreamBuilder);
        future.get();
    }

    private StreamParser getStreamParser(StreamingConfig streamingConfig, List<TblColRef> columns) throws Exception {
        if (!StringUtils.isEmpty(streamingConfig.getParserName())) {
            Class clazz = Class.forName(streamingConfig.getParserName());
            Constructor constructor = clazz.getConstructor(List.class);
            return (StreamParser) constructor.newInstance(columns);
        } else {
            return new JsonStreamParser(columns);
        }
    }

    private StreamFilter getStreamFilter(StreamingConfig streamingConfig) throws Exception {
        if (!StringUtils.isEmpty(streamingConfig.getFilterName())) {
            Class clazz = Class.forName(streamingConfig.getFilterName());
            return (StreamFilter) clazz.newInstance();
        } else {
            return DefaultStreamFilter.instance;
        }
    }

    private void startIIStreaming(StreamingConfig streamingConfig, final int partitionId) throws Exception {

        List<KafkaClusterConfig> allClustersConfigs = streamingConfig.getKafkaClusterConfigs();
        if (allClustersConfigs.size() != 1) {
            throw new RuntimeException("II streaming only support one kafka cluster");
        }
        KafkaClusterConfig kafkaClusterConfig = allClustersConfigs.get(0);

        final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
        Preconditions.checkArgument(partitionId >= 0 && partitionId < partitionCount, "invalid partition id:" + partitionId);

        final IIInstance ii = IIManager.getInstance(this.kylinConfig).getII(streamingConfig.getIiName());
        Preconditions.checkNotNull(ii, "cannot find ii name:" + streamingConfig.getIiName());
        Preconditions.checkArgument(ii.getSegments().size() > 0);
        final IISegment iiSegment = ii.getSegments().get(0);

        final Broker leadBroker = getLeadBroker(kafkaClusterConfig, partitionId);
        Preconditions.checkState(leadBroker != null, "cannot find lead broker");
        final int shard = ii.getDescriptor().getSharding();
        Preconditions.checkArgument(shard % partitionCount == 0);
        final int parallelism = shard / partitionCount;
        final int startShard = partitionId * parallelism;
        final int endShard = startShard + parallelism;

        long streamingOffset = getEarliestStreamingOffset(streamingConfig.getName(), startShard, endShard);
        streamingOffset = streamingOffset - (streamingOffset % parallelism);
        logger.info("offset from ii desc is " + streamingOffset);
        final long earliestOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaClusterConfig);
        logger.info("offset from KafkaRequester is " + earliestOffset);
        streamingOffset = Math.max(streamingOffset, earliestOffset);
        logger.info("starting offset is " + streamingOffset);

        if (!HBaseConnection.tableExists(kylinConfig.getStorageUrl(), iiSegment.getStorageLocationIdentifier())) {
            logger.error("no htable:" + iiSegment.getStorageLocationIdentifier() + " found");
            throw new IllegalStateException("please create htable:" + iiSegment.getStorageLocationIdentifier() + " first");
        }

        KafkaConsumer consumer = new KafkaConsumer(kafkaClusterConfig.getTopic(), partitionId, streamingOffset, kafkaClusterConfig.getBrokers(), kafkaClusterConfig, parallelism);
        kafkaConsumers.put(getKey(streamingConfig.getName(), partitionId), consumer);

        Executors.newSingleThreadExecutor().submit(consumer);
        final ExecutorService streamingBuilderPool = Executors.newFixedThreadPool(parallelism);
        for (int i = startShard; i < endShard; ++i) {
            final IIStreamBuilder task = new IIStreamBuilder(consumer.getStreamQueue(i % parallelism), streamingConfig.getName(), iiSegment.getStorageLocationIdentifier(), iiSegment.getIIDesc(), i);
            task.setStreamParser(getStreamParser(streamingConfig, ii.getDescriptor().listAllColumns()));
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
