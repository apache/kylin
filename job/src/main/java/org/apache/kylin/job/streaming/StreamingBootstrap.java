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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.IntermediateColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.*;
import org.apache.kylin.streaming.invertedindex.IIStreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    private List<BlockingQueue<StreamMessage>> consume(int clusterID, KafkaClusterConfig kafkaClusterConfig, final int partitionCount, final Map<Integer, Long> partitionIdOffsetMap, final int partitionOffset) {
        List<BlockingQueue<StreamMessage>> result = Lists.newArrayList();
        for (int partitionId = 0; partitionId < partitionCount; ++partitionId) {
            final Broker leadBroker = getLeadBroker(kafkaClusterConfig, partitionId);
            final int transferredPartitionId = partitionId + partitionOffset;
            final long latestOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, OffsetRequest.LatestTime(), leadBroker, kafkaClusterConfig);
            long streamingOffset = latestOffset;
            if (partitionIdOffsetMap.containsKey(transferredPartitionId)) {
                final long earliestOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaClusterConfig);
                long committedOffset = partitionIdOffsetMap.get(transferredPartitionId);
                Preconditions.checkArgument(committedOffset >= earliestOffset && committedOffset <= latestOffset, String.format("invalid offset:%d, earliestOffset:%d, latestOffset:%d", committedOffset, earliestOffset, latestOffset));
                streamingOffset = committedOffset;
            }
            logger.info("starting offset:" + streamingOffset + " cluster id:" + clusterID + " partitionId:" + partitionId + " transferredPartitionId:" + transferredPartitionId);
            KafkaConsumer consumer = new KafkaConsumer(clusterID, kafkaClusterConfig.getTopic(), partitionId, streamingOffset, kafkaClusterConfig.getBrokers(), kafkaClusterConfig);
            Executors.newSingleThreadExecutor().submit(consumer);
            result.add(consumer.getStreamQueue(0));
        }
        return result;
    }

    private void startCubeStreaming(StreamingConfig streamingConfig, final int partitionId) throws Exception {
        List<KafkaClusterConfig> kafkaClusterConfigs = streamingConfig.getKafkaClusterConfigs();

        final List<BlockingQueue<StreamMessage>> allClustersData = Lists.newArrayList();

        ArrayList<Integer> allPartitions = Lists.newArrayList();
        int partitionOffset = 0;
        for (KafkaClusterConfig kafkaClusterConfig : kafkaClusterConfigs) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            for (int i = 0; i < partitionCount; i++) {
                allPartitions.add(i + partitionOffset);
            }
            partitionOffset += partitionCount;
        }
        final Map<Integer, Long> partitionIdOffsetMap = streamingManager.getOffset(streamingConfig.getName(), allPartitions);

        int clusterID = 0;
        partitionOffset = 0;
        for (KafkaClusterConfig kafkaClusterConfig : kafkaClusterConfigs) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            Preconditions.checkArgument(partitionId >= 0 && partitionId < partitionCount, "invalid partition id:" + partitionId);

            final List<BlockingQueue<StreamMessage>> oneClusterData = consume(clusterID, kafkaClusterConfig, partitionCount, partitionIdOffsetMap, partitionOffset);
            logger.info("Cluster {} with {} partitions", allClustersData.size(), oneClusterData.size());
            allClustersData.addAll(oneClusterData);
            clusterID++;
            partitionOffset += partitionCount;
        }

        final String cubeName = streamingConfig.getCubeName();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

        int batchInterval = 5 * 60 * 1000;
        MicroBatchCondition condition = new MicroBatchCondition(Integer.MAX_VALUE, batchInterval);
        long startTimestamp = cubeInstance.getDateRangeEnd() == 0 ? TimeUtil.getNextPeriodStart(System.currentTimeMillis(), (long) batchInterval) : cubeInstance.getDateRangeEnd();
        logger.info("batch time interval is {} to {}", DateFormat.formatToTimeStr(startTimestamp), DateFormat.formatToTimeStr(startTimestamp + batchInterval));
        StreamBuilder cubeStreamBuilder = new StreamBuilder(streamingConfig.getName(), allClustersData, condition, new CubeStreamConsumer(cubeName), startTimestamp);
        cubeStreamBuilder.setStreamParser(getStreamParser(streamingConfig, Lists.transform(new CubeJoinedFlatTableDesc(cubeInstance.getDescriptor(), null).getColumnList(), new Function<IntermediateColumnDesc, TblColRef>() {
            @Nullable
            @Override
            public TblColRef apply(IntermediateColumnDesc input) {
                return input.getColRef();
            }
        })));
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

    private void startIIStreaming(StreamingConfig streamingConfig, final int partitionId) throws Exception {

        List<KafkaClusterConfig> allClustersConfigs = streamingConfig.getKafkaClusterConfigs();
        int clusterID = 0;
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

        KafkaConsumer consumer = new KafkaConsumer(clusterID, kafkaClusterConfig.getTopic(), partitionId, streamingOffset, kafkaClusterConfig.getBrokers(), kafkaClusterConfig, parallelism);
        kafkaConsumers.put(getKey(streamingConfig.getName(), partitionId), consumer);

        final IIDesc iiDesc = iiSegment.getIIDesc();

        Executors.newSingleThreadExecutor().submit(consumer);
        final ExecutorService streamingBuilderPool = Executors.newFixedThreadPool(parallelism);
        for (int i = startShard; i < endShard; ++i) {
            final StreamBuilder task = new StreamBuilder(streamingConfig.getName(),
                    consumer.getStreamQueue(i % parallelism),
                    new MicroBatchCondition(iiDesc.getSliceSize(), Integer.MAX_VALUE),
                    new IIStreamConsumer(streamingConfig.getName(), iiSegment.getStorageLocationIdentifier(), iiDesc, i), 0L);
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
