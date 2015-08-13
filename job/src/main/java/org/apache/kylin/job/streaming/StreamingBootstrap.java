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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import kafka.api.OffsetRequest;
import kafka.cluster.Broker;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.job.monitor.StreamingMonitor;
import org.apache.kylin.metadata.model.IntermediateColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.streaming.JsonStreamParser;
import org.apache.kylin.streaming.KafkaClusterConfig;
import org.apache.kylin.streaming.KafkaConsumer;
import org.apache.kylin.streaming.KafkaRequester;
import org.apache.kylin.streaming.OneOffStreamBuilder;
import org.apache.kylin.streaming.StreamBuilder;
import org.apache.kylin.streaming.StreamMessage;
import org.apache.kylin.streaming.StreamParser;
import org.apache.kylin.streaming.StreamingConfig;
import org.apache.kylin.streaming.StreamingManager;
import org.apache.kylin.streaming.StreamingUtil;
import org.apache.kylin.streaming.invertedindex.IIStreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

    public void start(BootstrapConfig bootstrapConfig) throws Exception {
        final String streaming = bootstrapConfig.getStreaming();
        Preconditions.checkNotNull(streaming, "streaming name cannot be empty");
        final StreamingConfig streamingConfig = streamingManager.getStreamingConfig(streaming);
        Preconditions.checkArgument(streamingConfig != null, "cannot find kafka config:" + streaming);

        if (!StringUtils.isEmpty(streamingConfig.getIiName())) {
            int partitionId = bootstrapConfig.getPartitionId();
            Preconditions.checkArgument(partitionId >= 0, "partitionId cannot be empty for inverted index streaming");
            startIIStreaming(streamingConfig, partitionId);
        } else if (!StringUtils.isEmpty(streamingConfig.getCubeName())) {
            if (bootstrapConfig.isFillGap()) {
                final List<Pair<Long, Long>> gaps = StreamingMonitor.findGaps(streamingConfig.getCubeName());
                logger.info("all gaps:" + StringUtils.join(gaps, ","));
                for (Pair<Long, Long> gap : gaps) {
                    startOneOffCubeStreaming(streamingConfig, gap.getFirst(), gap.getSecond(), streamingConfig.getMargin());
                }
            } else {
                if (bootstrapConfig.isOneOff()) {
                    Preconditions.checkArgument(bootstrapConfig.getStart() != 0);
                    Preconditions.checkArgument(bootstrapConfig.getEnd() != 0);
                    startOneOffCubeStreaming(streamingConfig, bootstrapConfig.getStart(), bootstrapConfig.getEnd(), streamingConfig.getMargin());
                } else {
                    startCubeStreaming(streamingConfig);
                }
            }
        } else {
            throw new IllegalArgumentException("no cube or ii in kafka config");
        }
    }

    public void start(String streaming, int partitionId) throws Exception {
        final BootstrapConfig bootstrapConfig = new BootstrapConfig();
        bootstrapConfig.setPartitionId(partitionId);
        bootstrapConfig.setStreaming(streaming);
        start(bootstrapConfig);
    }

    private List<BlockingQueue<StreamMessage>> consume(final int clusterID, final KafkaClusterConfig kafkaClusterConfig, final int partitionCount, final Map<Integer, Long> partitionIdOffsetMap, final int partitionIdOffset) {
        List<BlockingQueue<StreamMessage>> result = Lists.newArrayList();
        for (int partitionId = 0; partitionId < partitionCount; ++partitionId) {
            final Broker leadBroker = StreamingUtil.getLeadBroker(kafkaClusterConfig, partitionId);
            final int transferredPartitionId = partitionId + partitionIdOffset;
            final long latestOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, OffsetRequest.LatestTime(), leadBroker, kafkaClusterConfig);
            long streamingOffset = latestOffset;
            if (partitionIdOffsetMap.containsKey(transferredPartitionId)) {
                final long earliestOffset = KafkaRequester.getLastOffset(kafkaClusterConfig.getTopic(), partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaClusterConfig);
                long committedOffset = partitionIdOffsetMap.get(transferredPartitionId);
                Preconditions.checkArgument(committedOffset <= latestOffset, String.format("invalid offset:%d, earliestOffset:%d, latestOffset:%d", committedOffset, earliestOffset, latestOffset));

                if (committedOffset < earliestOffset) {
                    logger.warn(String.format("invalid offset:%d, earliestOffset:%d; Will use earliestOffset as committedOffset.", committedOffset, earliestOffset));
                    committedOffset = earliestOffset;
                }
                streamingOffset = committedOffset;
            }
            logger.info("starting offset:" + streamingOffset + " cluster id:" + clusterID + " partitionId:" + partitionId + " transferredPartitionId:" + transferredPartitionId);
            KafkaConsumer consumer = new KafkaConsumer(clusterID, kafkaClusterConfig.getTopic(), partitionId, streamingOffset, kafkaClusterConfig.getBrokers(), kafkaClusterConfig);
            Executors.newSingleThreadExecutor(new DaemonThreadFactory()).submit(consumer);
            result.add(consumer.getStreamQueue(0));
        }
        return result;
    }

    private void startCubeStreaming(StreamingConfig streamingConfig) throws Exception {
        List<KafkaClusterConfig> kafkaClusterConfigs = streamingConfig.getKafkaClusterConfigs();

        final List<BlockingQueue<StreamMessage>> allClustersData = Lists.newArrayList();

        ArrayList<Integer> allPartitions = Lists.newArrayList();
        int partitionIdOffset = 0;
        for (KafkaClusterConfig kafkaClusterConfig : kafkaClusterConfigs) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            for (int i = 0; i < partitionCount; i++) {
                allPartitions.add(i + partitionIdOffset);
            }
            partitionIdOffset += partitionCount;
        }
        final Map<Integer, Long> partitionIdOffsetMap = streamingManager.getOffset(streamingConfig.getName(), allPartitions);

        int clusterID = 0;
        partitionIdOffset = 0;
        for (KafkaClusterConfig kafkaClusterConfig : kafkaClusterConfigs) {
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();

            final List<BlockingQueue<StreamMessage>> oneClusterData = consume(clusterID, kafkaClusterConfig, partitionCount, partitionIdOffsetMap, partitionIdOffset);
            logger.info("Cluster {} with {} partitions", allClustersData.size(), oneClusterData.size());
            allClustersData.addAll(oneClusterData);
            clusterID++;
            partitionIdOffset += partitionCount;
        }

        final String cubeName = streamingConfig.getCubeName();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

        int batchInterval = 5 * 60 * 1000;
        long startTimestamp = cubeInstance.getDateRangeEnd() == 0 ? TimeUtil.getNextPeriodStart(System.currentTimeMillis() - 30 * 60 * 1000, (long) batchInterval) : cubeInstance.getDateRangeEnd();
        logger.info("batch time interval is {} to {}", DateFormat.formatToTimeStr(startTimestamp), DateFormat.formatToTimeStr(startTimestamp + batchInterval));
        StreamBuilder cubeStreamBuilder = StreamBuilder.newPeriodicalStreamBuilder(streamingConfig.getName(), allClustersData, new CubeStreamConsumer(cubeName), startTimestamp, batchInterval);
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

    private void startOneOffCubeStreaming(StreamingConfig streamingConfig, long startTimestamp, long endTimestamp, long margin) throws Exception {
        final String cubeName = streamingConfig.getCubeName();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final StreamParser streamParser = getStreamParser(streamingConfig, Lists.transform(new CubeJoinedFlatTableDesc(cubeInstance.getDescriptor(), null).getColumnList(), new Function<IntermediateColumnDesc, TblColRef>() {
            @Nullable
            @Override
            public TblColRef apply(IntermediateColumnDesc input) {
                return input.getColRef();
            }
        }));
        final List<BlockingQueue<StreamMessage>> queues = Lists.newLinkedList();

        int clusterId = 0;
        final ExecutorService executorService = Executors.newFixedThreadPool(10, new DaemonThreadFactory());
        final long targetTimestamp = startTimestamp - margin;
        for (final KafkaClusterConfig kafkaClusterConfig : streamingConfig.getKafkaClusterConfigs()) {
            final ConcurrentMap<Integer, Long> partitionIdOffsetMap = Maps.newConcurrentMap();
            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
            final CountDownLatch countDownLatch = new CountDownLatch(partitionCount);
            for (int i = 0; i < partitionCount; ++i) {
                final int idx = i;
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            partitionIdOffsetMap.put(idx, StreamingUtil.findClosestOffsetWithDataTimestamp(kafkaClusterConfig, idx, targetTimestamp, streamParser));
                        } catch (Exception e) {
                            logger.error(String.format("fail to get start offset partitionId: %d, target timestamp: %d", idx, targetTimestamp), e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
            }
            countDownLatch.await();
            logger.info("partitionId to start offset map:" + partitionIdOffsetMap);
            Preconditions.checkArgument(partitionIdOffsetMap.size() == partitionCount, "fail to get all start offset");
            final List<BlockingQueue<StreamMessage>> oneClusterQueue = consume(clusterId, kafkaClusterConfig, partitionCount, partitionIdOffsetMap, 0);
            queues.addAll(oneClusterQueue);
            logger.info("Cluster {} with {} partitions", clusterId, oneClusterQueue.size());
            clusterId++;
        }
        executorService.shutdown();

        logger.info(String.format("starting one off streaming build with timestamp{%d, %d}", startTimestamp, endTimestamp));
        OneOffStreamBuilder oneOffStreamBuilder = new OneOffStreamBuilder(streamingConfig.getName(), queues, streamParser, new CubeStreamConsumer(cubeName), startTimestamp, endTimestamp, margin);
        Executors.newSingleThreadExecutor().submit(oneOffStreamBuilder).get();
        logger.info("one off build finished");
    }

    //    private void startCalculatingMargin(final StreamingConfig streamingConfig) throws Exception {
    //        final String cubeName = streamingConfig.getCubeName();
    //        final StreamParser streamParser = getStreamParser(streamingConfig, Lists.<TblColRef>newArrayList());
    //        final List<BlockingQueue<StreamMessage>> queues = Lists.newLinkedList();
    //
    //        int clusterId = 0;
    //        final List<Pair<Long,Long>> firstAndLastOffsets = Lists.newArrayList();
    //
    //        for (final KafkaClusterConfig kafkaClusterConfig : streamingConfig.getKafkaClusterConfigs()) {
    //            final ConcurrentMap<Integer, Long> partitionIdOffsetMap = Maps.newConcurrentMap();
    //            final int partitionCount = KafkaRequester.getKafkaTopicMeta(kafkaClusterConfig).getPartitionIds().size();
    //            for (int i = 0; i < partitionCount; ++i) {
    //                Pair<Long,Long> firstlast = StreamingUtil.getFirstAndLastOffset(kafkaClusterConfig,i);
    //                firstAndLastOffsets.add(firstlast);
    //                partitionIdOffsetMap.putIfAbsent(i,firstlast.getFirst());
    //            }
    //
    //            logger.info("partitionId to start offset map:" + partitionIdOffsetMap);
    //            Preconditions.checkArgument(partitionIdOffsetMap.size() == partitionCount, "fail to get all start offset");
    //            final List<BlockingQueue<StreamMessage>> oneClusterQueue = consume(clusterId, kafkaClusterConfig, partitionCount, partitionIdOffsetMap, 0);
    //            queues.addAll(oneClusterQueue);
    //            logger.info("Cluster {} with {} partitions", clusterId, oneClusterQueue.size());
    //            clusterId++;
    //        }
    //
    //        OneOffStreamBuilder oneOffStreamBuilder = new OneOffStreamBuilder(streamingConfig.getName(), queues, streamParser, new CubeStreamConsumer(cubeName), startTimestamp, endTimestamp, margin);
    //        Executors.newSingleThreadExecutor().submit(oneOffStreamBuilder).get();
    //        logger.info("one off build finished");
    //    }

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

        final Broker leadBroker = StreamingUtil.getLeadBroker(kafkaClusterConfig, partitionId);
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

        Executors.newSingleThreadExecutor(new DaemonThreadFactory()).submit(consumer);
        final ExecutorService streamingBuilderPool = Executors.newFixedThreadPool(parallelism);
        for (int i = startShard; i < endShard; ++i) {
            final StreamBuilder task = StreamBuilder.newLimitedSizeStreamBuilder(streamingConfig.getName(), consumer.getStreamQueue(i % parallelism), new IIStreamConsumer(streamingConfig.getName(), iiSegment.getStorageLocationIdentifier(), iiDesc, i), 0L, iiDesc.getSliceSize());
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
