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

package org.apache.kylin.stream.server;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.coordinator.StreamMetadataStoreFactory;
import org.apache.kylin.stream.coordinator.StreamingUtils;
import org.apache.kylin.stream.coordinator.client.CoordinatorClient;
import org.apache.kylin.stream.coordinator.client.HttpCoordinatorClient;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicIdempotent;
import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.consumer.EndPositionStopCondition;
import org.apache.kylin.stream.core.consumer.IConsumerProvider;
import org.apache.kylin.stream.core.consumer.IStopConsumptionCondition;
import org.apache.kylin.stream.core.consumer.IStreamingConnector;
import org.apache.kylin.stream.core.consumer.StreamingConsumerChannel;
import org.apache.kylin.stream.core.metrics.StreamingMetrics;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.StreamingCubeConsumeState;
import org.apache.kylin.stream.core.model.stats.ReceiverCubeStats;
import org.apache.kylin.stream.core.model.stats.ReceiverStats;
import org.apache.kylin.stream.core.model.stats.SegmentStats;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePositionHandler;
import org.apache.kylin.stream.core.source.IStreamingSource;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceConfigManager;
import org.apache.kylin.stream.core.source.StreamingSourceFactory;
import org.apache.kylin.stream.core.storage.StreamingCubeSegment;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.core.storage.columnar.ColumnarStoreCache;
import org.apache.kylin.stream.core.util.HDFSUtil;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.apache.kylin.stream.core.util.NodeUtil;
import org.apache.kylin.stream.server.retention.RetentionPolicyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class StreamingServer implements ReplicaSetLeaderSelector.LeaderChangeListener, IConsumerProvider {
    private static final Logger logger = LoggerFactory.getLogger(StreamingServer.class);
    public static final int DEFAULT_PORT = 9090;
    private static final int CONSUMER_STOP_WAIT_TIMEOUT = 10000;
    private static volatile StreamingServer instance = null;

    private Map<String, StreamingConsumerChannel> cubeConsumerMap = Maps.newHashMap();
    private Map<String, List<Partition>> assignments = Maps.newHashMap();
    private Map<String, StreamingSegmentManager> streamingSegmentManagerMap = new ConcurrentHashMap<>();

    private CuratorFramework streamZKClient;
    private ReplicaSetLeaderSelector leaderSelector;
    private CoordinatorClient coordinatorClient;
    private StreamMetadataStore streamMetadataStore;
    private Node currentNode;
    private int replicaSetID = -1;
    /**
     * indicate whether current receiver is the leader of whole replica set
     */
    private volatile boolean isLeader = false;

    private ScheduledExecutorService segmentStateCheckerExecutor;
    private ExecutorService segmentFlushExecutor;

    private final String baseStorePath;

    private StreamingServer() {
        streamZKClient = StreamingUtils.getZookeeperClient();
        streamMetadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
        coordinatorClient = new HttpCoordinatorClient(streamMetadataStore);
        currentNode = NodeUtil.getCurrentNode(DEFAULT_PORT);
        baseStorePath = calLocalSegmentCacheDir();
        segmentStateCheckerExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(
                "segment_state_check"));
        segmentFlushExecutor = Executors.newFixedThreadPool(5, new NamedThreadFactory("segment_flush"));
    }

    @VisibleForTesting
    public void setCoordinatorClient(CoordinatorClient coordinatorClient) {
        this.coordinatorClient = coordinatorClient;
    }

    public static synchronized StreamingServer getInstance() {
        if (instance == null) {
            instance = new StreamingServer();
        }
        return instance;
    }

    public void start() throws Exception {
        registerReceiver();
        ReplicaSet rs = findBelongReplicaSet();
        if (rs != null) {
            addToReplicaSet(rs.getReplicaSetID());
        }
        startMetrics();
        startSegmentStateChecker();
        addShutdownHook();
    }

    private void startSegmentStateChecker() {
        segmentStateCheckerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Collection<StreamingSegmentManager> segmentManagers = getAllCubeSegmentManagers();
                long curr = System.currentTimeMillis();
                for (StreamingSegmentManager segmentManager : segmentManagers) {
                    CubeInstance cubeInstance = segmentManager.getCubeInstance();
                    String cubeName = cubeInstance.getName();
                    try {
                        Collection<StreamingCubeSegment> activeSegments = segmentManager.getActiveSegments();
                        for (StreamingCubeSegment segment : activeSegments) {
                            long delta = curr - segment.getLastUpdateTime();
                            if (curr > segment.getDateRangeEnd() && delta > segmentManager.cubeDuration) {
                                logger.debug("Make {} immutable because it lastUpdate[{}] exceed wait duration.", segment.getSegmentName(), segment.getLastUpdateTime());
                                segmentManager.makeSegmentImmutable(segment.getSegmentName());
                            }
                        }
                        RetentionPolicyInfo retentionPolicyInfo = new RetentionPolicyInfo();
                        String policyName = cubeInstance.getConfig().getStreamingSegmentRetentionPolicy();
                        Map<String, String> policyProps = cubeInstance.getConfig()
                                .getStreamingSegmentRetentionPolicyProperties(policyName);
                        retentionPolicyInfo.setName(policyName);
                        retentionPolicyInfo.setProperties(policyProps);
                        //The returned segments that require remote persisted are already sorted in ascending order by the segment start time
                        Collection<StreamingCubeSegment> segments = segmentManager.getRequireRemotePersistSegments();
                        if (!segments.isEmpty()) {
                            logger.info("found cube {} segments:{} are immutable, retention policy is: {}", cubeName,
                                    segments, retentionPolicyInfo.getName());
                        } else {
                            continue;
                        }
                        handleImmutableCubeSegments(cubeName, segmentManager, segments, retentionPolicyInfo);
                    } catch (Exception e) {
                        logger.error("error when handle cube:" + cubeName, e);
                    }
                }
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * <pre>
     * When segment status was changed to immutable, the leader of replica will
     * try to upload local segment cache to remote.
     * </pre>
     */
    private void handleImmutableCubeSegments(String cubeName, StreamingSegmentManager segmentManager,
            Collection<StreamingCubeSegment> segments, RetentionPolicyInfo retentionPolicyInfo) throws Exception {
        if (RetentionPolicyInfo.FULL_BUILD_POLICY.equalsIgnoreCase(retentionPolicyInfo.getName())) {
            if (isLeader) {
                sendSegmentsToFullBuild(cubeName, segmentManager, segments);
            }
        } else {
            purgeSegments(cubeName, segments, retentionPolicyInfo.getProperties());
        }
    }

    @NotAtomicIdempotent
    private void sendSegmentsToFullBuild(String cubeName, StreamingSegmentManager segmentManager,
            Collection<StreamingCubeSegment> segments) throws Exception {
        List<Future<?>> futureList = Lists.newArrayList();
        for (StreamingCubeSegment segment : segments) {
            String segmentHDFSPath = HDFSUtil.getStreamingSegmentFilePath(cubeName, segment.getSegmentName()) + "/"
                    + replicaSetID;
            SegmentHDFSFlusher flusher = new SegmentHDFSFlusher(segment, segmentHDFSPath);
            futureList.add(segmentFlushExecutor.submit(flusher));
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cubeInstance);
        int i = 0;
        for (StreamingCubeSegment segment : segments) {
            futureList.get(i).get();
            logger.info("Save remote store state to metadata store.");
            streamMetadataStore.addCompleteReplicaSetForSegmentBuild(segment.getCubeName(), segment.getSegmentName(),
                    replicaSetID);

            logger.info("save remote checkpoint to metadata store");
            ISourcePosition smallestSourcePosition = segmentManager.getSmallestSourcePosition(segment);
            String smallestSourcePosStr = streamingSource.getSourcePositionHandler().serializePosition(smallestSourcePosition);
            streamMetadataStore.saveSourceCheckpoint(segment.getCubeName(), segment.getSegmentName(), replicaSetID,
                    smallestSourcePosStr);

            logger.info("Send notification to coordinator for cube {} segment {}.", cubeName, segment.getSegmentName());
            coordinatorClient.segmentRemoteStoreComplete(currentNode, segment.getCubeName(),
                    new Pair<>(segment.getDateRangeStart(), segment.getDateRangeEnd()));
            logger.info("Send notification success.");
            segment.saveState(StreamingCubeSegment.State.REMOTE_PERSISTED);
            logger.info("Commit cube {} segment {}  status converted to {}.", segment.getCubeName(), segment.getSegmentName(),
                    StreamingCubeSegment.State.REMOTE_PERSISTED.name());
            i++;
        }
    }

    private void purgeSegments(String cubeName, Collection<StreamingCubeSegment> segments,
            Map<String, String> properties) {
        long retentionTimeInSec = Long.valueOf(properties.get("retentionTimeInSec"));
        boolean hasPurgedSegment = false;
        for (StreamingCubeSegment segment : segments) {
            long liveTime = System.currentTimeMillis() - segment.getCreateTime();
            if (retentionTimeInSec * 1000 < liveTime) {
                logger.info("purge segment:{}", segment);
                getStreamingSegmentManager(cubeName).purgeSegment(segment.getSegmentName());
                hasPurgedSegment = true;
            }
        }
        if (hasPurgedSegment) {
            resumeConsumerIfPaused(cubeName);
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("start to shut down streaming receiver");
                for (Map.Entry<String, StreamingConsumerChannel> consumerEntry : cubeConsumerMap.entrySet()) {
                    logger.info("start to stop consumer for cube:{}", consumerEntry.getKey());
                    StreamingConsumerChannel consumer = consumerEntry.getValue();
                    consumer.stop(CONSUMER_STOP_WAIT_TIMEOUT);
                    logger.info("finish to stop consumer for cube:{}", consumerEntry.getKey());
                }
                logger.info("streaming receiver shut down successfully");
            }
        });
    }

    private void startMetrics() {
        StreamingMetrics.getInstance().start();
    }

    private ReplicaSet findBelongReplicaSet() {
        List<ReplicaSet> replicaSets = streamMetadataStore.getReplicaSets();
        for (ReplicaSet rs : replicaSets) {
            if (rs.containPhysicalNode(currentNode)) {
                return rs;
            }
        }
        return null;
    }

    private void registerReceiver() throws Exception {
        logger.info("register receiver: {}", currentNode);
        streamMetadataStore.addReceiver(currentNode);
    }

    private void joinReplicaSetLeaderElection(int replicaSetID) {
        leaderSelector = new ReplicaSetLeaderSelector(streamZKClient, currentNode, replicaSetID);
        leaderSelector.addLeaderChangeListener(this);
        leaderSelector.start();
    }

    public synchronized void assign(Map<String, List<Partition>> cubeAssignment) {
        this.assignments.putAll(cubeAssignment);
    }

    public synchronized void assign(String cubeName, List<Partition> partitions) {
        this.assignments.put(cubeName, partitions);
    }

    public synchronized void unAssign(String cubeName) {
        stopConsumer(cubeName);
        this.assignments.remove(cubeName);
        removeCubeData(cubeName);
    }

    public synchronized void startConsumers(List<String> cubes) {
        for (String cube : cubes) {
            startConsumer(cube, null);
        }
    }

    public synchronized void startConsumer(String cubeName, ConsumerStartProtocol startProtocol) {
        List<Partition> partitions = assignments.get(cubeName);
        StreamingConsumerChannel consumer = cubeConsumerMap.get(cubeName);
        if (consumer != null) {
            List<Partition> consumingPartitions = consumer.getConsumePartitions();
            Collections.sort(partitions);
            Collections.sort(consumingPartitions);
            if (partitions.equals(consumingPartitions)) {
                logger.info("The consumer for cube:{} is already running, skip starting", cubeName);
            } else {
                String msg = String
                        .format(Locale.ROOT, "The running consumer for cube:%s partition:%s is conflict with assign partitions:%s, should stop the consumer first.",
                                cubeName, consumingPartitions, partitions);
                throw new IllegalStateException(msg);
            }
        } else {
            if (partitions == null || partitions.isEmpty()) {
                logger.info("partitions is empty for cube:{}", cubeName);
                return;
            }
            logger.info("create and start new consumer for cube:{}", cubeName);
            try {
                reloadCubeMetadata(cubeName);
                StreamingConsumerChannel newConsumer = createNewConsumer(cubeName, partitions, startProtocol);
                newConsumer.start();
            } catch (Exception e) {
                logger.error("consumer start fail for cube:" + cubeName, e);
            }
        }
    }

    public synchronized ConsumerStatsResponse stopConsumer(String cube) {
        logger.info("stop consumers for cube: {}", cube);
        ConsumerStatsResponse response = new ConsumerStatsResponse();
        StreamingConsumerChannel consumer = cubeConsumerMap.get(cube);
        if (consumer != null) {
            consumer.stop(CONSUMER_STOP_WAIT_TIMEOUT);
            cubeConsumerMap.remove(cube);
            response.setCubeName(cube);
            response.setConsumePosition(consumer.getSourceConsumeInfo());
        }
        return response;
    }

    public synchronized void stopAllConsumers() {
        List<String> cubes = Lists.newArrayList(cubeConsumerMap.keySet());
        for (String cube : cubes) {
            stopConsumer(cube);
        }
    }

    public synchronized ConsumerStatsResponse pauseConsumer(String cubeName) {
        logger.info("pause consumers for cube: {}", cubeName);
        ConsumerStatsResponse response = new ConsumerStatsResponse();
        response.setCubeName(cubeName);
        StreamingConsumerChannel consumer = cubeConsumerMap.get(cubeName);
        if (consumer != null) {
            consumer.pause(true);
            response.setConsumePosition(consumer.getSourceConsumeInfo());
        } else {
            logger.warn("the consumer for cube:{} does not exist ", cubeName);
        }
        return response;
    }

    public synchronized ConsumerStatsResponse resumeConsumer(String cubeName, String resumeToPosition) {
        logger.info("resume consumers for cube: {}", cubeName);
        ConsumerStatsResponse response = new ConsumerStatsResponse();
        response.setCubeName(cubeName);
        StreamingConsumerChannel consumer = cubeConsumerMap.get(cubeName);
        if (consumer == null) {
            logger.warn("the consumer for cube:{} does not exist", cubeName);
            return response;
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cube);
        if (resumeToPosition != null && !resumeToPosition.isEmpty()) {
            IStopConsumptionCondition stopCondition = new EndPositionStopCondition(streamingSource.getSourcePositionHandler().parsePosition(resumeToPosition));
            consumer.resumeToStopCondition(stopCondition);
            cubeConsumerMap.remove(cubeName);
        } else {
            consumer.resume();
        }
        response.setConsumePosition(consumer.getSourceConsumeInfo());

        return response;
    }

    public void addToReplicaSet(int replicaSetID) {
        logger.info("add the node to the replicaSet:{}, join the group leader election.", replicaSetID);
        if (this.replicaSetID == replicaSetID) {
            logger.info("the receiver already in the replica set:{}, return", replicaSetID);
            return;
        }
        if (this.replicaSetID != -1) {
            throw new IllegalStateException("the receiver is in replica set:" + this.replicaSetID
                    + ", please remove first");
        }
        this.replicaSetID = replicaSetID;
        joinReplicaSetLeaderElection(replicaSetID);
        Map<String, List<Partition>> nodeAssignments = streamMetadataStore.getAssignmentsByReplicaSet(replicaSetID);
        if (nodeAssignments != null) {
            assign(nodeAssignments);
            List<String> assignedCubes = Lists.newArrayList(nodeAssignments.keySet());
            initLocalSegmentManager(assignedCubes);
            startConsumers(assignedCubes);
        } else {
            initLocalSegmentManager(Lists.<String> newArrayList());
        }
    }

    public void removeFromReplicaSet() {
        if (leaderSelector != null) {
            try {
                leaderSelector.close();
            } catch (Exception e) {
                logger.error("error happens when close leader selector", e);
            }
        }
        this.replicaSetID = -1;
        this.isLeader = false;
        assignments.clear();
        stopAllConsumers();
        List<String> cubes = Lists.newArrayList(streamingSegmentManagerMap.keySet());
        for (String cube : cubes) {
            removeCubeData(cube);
        }
    }

    public ReceiverStats getReceiverStats() {
        ReceiverStats stats = new ReceiverStats();
        stats.setAssignments(assignments);
        stats.setLead(isLeader);

        Set<String> allCubes = Sets.newHashSet();
        allCubes.addAll(assignments.keySet());
        allCubes.addAll(cubeConsumerMap.keySet());
        allCubes.addAll(streamingSegmentManagerMap.keySet());
        for (String cube : allCubes) {
            stats.addCubeStats(cube, getCubeStats(cube));
        }
        stats.setCacheStats(ColumnarStoreCache.getInstance().getCacheStats());
        return stats;
    }

    public ReceiverCubeStats getCubeStats(String cubeName) {
        ReceiverCubeStats receiverCubeStats = new ReceiverCubeStats();
        StreamingConsumerChannel consumer = cubeConsumerMap.get(cubeName);
        if (consumer != null) {
            receiverCubeStats.setConsumerStats(consumer.getConsumerStats());
        }

        StreamingSegmentManager segmentManager = streamingSegmentManagerMap.get(cubeName);
        if (segmentManager != null) {
            Map<String, SegmentStats> segmentStatsMap = segmentManager.getSegmentStats();
            receiverCubeStats.setSegmentStatsMap(segmentStatsMap);
            receiverCubeStats.setTotalIngest(segmentManager.getIngestCount());
            receiverCubeStats.setLatestEventTime(
                    StreamingSegmentManager.resetTimestampByTimeZone(segmentManager.getLatestEventTime()));
            receiverCubeStats.setLatestEventIngestTime(
                    StreamingSegmentManager.resetTimestampByTimeZone(segmentManager.getLatestEventIngestTime()));
            receiverCubeStats.setLongLatencyInfo(segmentManager.getLongLatencyInfo());
        }
        return receiverCubeStats;
    }

    public void makeCubeImmutable(String cubeName) {
        if (cubeConsumerMap.containsKey(cubeName)) {
            logger.info("before make cube immutable, stop consumer for cube:{}", cubeName);
            StreamingConsumerChannel consumer = cubeConsumerMap.get(cubeName);
            consumer.stop(CONSUMER_STOP_WAIT_TIMEOUT);
            cubeConsumerMap.remove(cubeName);
        }

        StreamingSegmentManager segmentManager = streamingSegmentManagerMap.get(cubeName);
        if (segmentManager == null) {
            return;
        }
        segmentManager.makeAllSegmentsImmutable();
    }

    public void makeCubeSegmentImmutable(String cubeName, String segmentName) {
        StreamingSegmentManager cubeStore = streamingSegmentManagerMap.get(cubeName);
        if (cubeStore == null) {
            return;
        }
        cubeStore.makeSegmentImmutable(segmentName);
    }

    public void remoteSegmentBuildComplete(String cubeName, String segmentName) {
        StreamingSegmentManager segmentManager = getStreamingSegmentManager(cubeName);
        List<String> removedSegments = segmentManager.remoteSegmentBuildComplete(segmentName);
        if (!removedSegments.isEmpty()) {
            resumeConsumerIfPaused(cubeName);
        }
    }

    /**
     * resume cube consumer, if it is paused by too many segments
     * @param cubeName
     */
    private void resumeConsumerIfPaused(String cubeName) {
        StreamingConsumerChannel consumer = getConsumer(cubeName);
        if (consumer == null || !consumer.isPaused()) {
            return;
        }
        StreamingCubeConsumeState consumeState = streamMetadataStore.getStreamingCubeConsumeState(cubeName);
        if (consumeState == null || consumeState == StreamingCubeConsumeState.RUNNING) {
            logger.info("resume the cube consumer:{} after remove some local immutable segments", cubeName);
            consumer.resume();
        }
    }

    private StreamingConsumerChannel createNewConsumer(String cubeName, List<Partition> partitions, ConsumerStartProtocol startProtocol)
            throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        StreamingSegmentManager segmentManager = getStreamingSegmentManager(cubeName);

        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cube);
        IStreamingConnector streamingConnector = streamingSource.createStreamingConnector(cubeName, partitions,
                startProtocol, segmentManager);
        StreamingConsumerChannel consumer = new StreamingConsumerChannel(cubeName, streamingConnector, segmentManager,
                IStopConsumptionCondition.NEVER_STOP);
        long minAcceptEventTime = cube.getDescriptor().getPartitionDateStart();
        CubeSegment latestRemoteSegment = cube.getLatestReadySegment();
        if (latestRemoteSegment != null) {
            minAcceptEventTime = latestRemoteSegment.getTSRange().end.v;
        }
        if (minAcceptEventTime > 0) {
            consumer.setMinAcceptEventTime(minAcceptEventTime);
        }
        StreamingCubeConsumeState consumeState = streamMetadataStore.getStreamingCubeConsumeState(cubeName);
        if (consumeState != null && consumeState == StreamingCubeConsumeState.PAUSED) {
            consumer.pause(false);
        }
        cubeConsumerMap.put(cubeName, consumer);
        return consumer;
    }

    @Override
    public void becomeLeader() {
        if (replicaSetID != -1) {
            logger.info("become leader of the replicaSet:{}", replicaSetID);
            try {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(replicaSetID);
                rs.setLeader(currentNode);
                streamMetadataStore.updateReplicaSet(rs);
                coordinatorClient.replicaSetLeaderChange(replicaSetID, currentNode);
            } catch (Exception e) {
                logger.error("error when send lead change notification to coordinator", e);
            }
        }
        isLeader = true;
    }

    @Override
    public void becomeFollower() {
        isLeader = false;
        if (replicaSetID != -1) {
            logger.info("become follower of the replicaSet:{}", replicaSetID);
        }
    }

    @Override
    public StreamingConsumerChannel getConsumer(String cubeName) {
        return cubeConsumerMap.get(cubeName);
    }

    public StreamingSegmentManager getStreamingSegmentManager(String cubeName) {
        if (streamingSegmentManagerMap.get(cubeName) == null) {
            synchronized (streamingSegmentManagerMap) {
                if (streamingSegmentManagerMap.get(cubeName) == null) {
                    CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
                    ISourcePositionHandler sourcePositionHandler = StreamingSourceFactory.getStreamingSource(cubeInstance).getSourcePositionHandler();
                    StreamingSegmentManager segmentManager = new StreamingSegmentManager(baseStorePath, cubeInstance, sourcePositionHandler, this);
                    streamingSegmentManagerMap.put(cubeName, segmentManager);
                }
            }
        }
        return streamingSegmentManagerMap.get(cubeName);
    }

    public void removeCubeData(String cubeName) {
        logger.info("remove cube data: {}", cubeName);
        StreamingSegmentManager segmentManager = getStreamingSegmentManager(cubeName);
        if (segmentManager != null) {
            streamingSegmentManagerMap.remove(cubeName);
            segmentManager.close();
            segmentManager.purgeAllSegments();
        }
    }

    public void reSubmitCubeSegment(String cubeName, String segmentName) {
        StreamingSegmentManager segmentManager = getStreamingSegmentManager(cubeName);
        StreamingCubeSegment segment = segmentManager.getSegmentByName(segmentName);
        if (segment == null) {
            throw new IllegalStateException("cannot find segment:" + segmentName);
        }
        if (segment.isActive()) {
            throw new IllegalStateException("the segment must be immutable:" + segment);
        }
        String segmentHDFSPath = HDFSUtil.getStreamingSegmentFilePath(cubeName, segmentName) + "/" + replicaSetID;
        SegmentHDFSFlusher flusher = new SegmentHDFSFlusher(segment, segmentHDFSPath);
        try {
            flusher.flushToHDFS();
        } catch (IOException e) {
            throw new RuntimeException("fail to copy segment to hdfs:" + segment, e);
        }
    }

    public Collection<StreamingSegmentManager> getAllCubeSegmentManagers() {
        return streamingSegmentManagerMap.values();
    }

    private void initLocalSegmentManager(List<String> assignedCubes) {
        File baseFolder = new File(baseStorePath);
        if (!baseFolder.exists()) {
            baseFolder.mkdirs();
        }

        File[] subFolders = baseFolder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });

        for (File cubeFolder : subFolders) {
            String cubeName = cubeFolder.getName();
            if (!assignedCubes.contains(cubeName)) {
                logger.info("remove the cube:{} data, because it is not assigned to this node", cubeName);
                try {
                    FileUtils.deleteDirectory(cubeFolder);
                } catch (IOException e) {
                    logger.error("error happens when remove cube folder", e);
                }
                continue;
            }
            try {
                StreamingSegmentManager segmentManager = getStreamingSegmentManager(cubeName);
                segmentManager.restoreSegmentsFromLocal();
            } catch (Exception e) {
                logger.error("local cube store init fail", e);
            }
        }
    }

    private void reloadCubeMetadata(String cubeName) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ResourceStore resourceStore = ResourceStore.getStore(kylinConfig);
        CubeInstance rawCubeInstance = resourceStore.getResource(CubeInstance.concatResourcePath(cubeName),
                 CubeManager.CUBE_SERIALIZER);
        CubeDesc rawCubeDesc = resourceStore.getResource(CubeDesc.concatResourcePath(rawCubeInstance.getDescName()),
                CubeDescManager.CUBE_DESC_SERIALIZER);
        DataModelDesc rawModel = resourceStore.getResource(
                DataModelDesc.concatResourcePath(rawCubeDesc.getModelName()),
                new JsonSerializer<>(DataModelDesc.class));
        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        List<ProjectInstance> projects = projectManager.findProjectsByModel(rawModel.getName());
        if (projects.isEmpty()) {
            projectManager.reloadAll();
            projects = projectManager.findProjectsByModel(rawModel.getName());
        }
        if (projects.size() != 1) {
            throw new IllegalArgumentException("the cube:" + cubeName + " is not in any project");
        }

        TableMetadataManager.getInstance(kylinConfig).reloadSourceTableQuietly(rawModel.getRootFactTableName(),
                projects.get(0).getName());
        DataModelManager.getInstance(kylinConfig).reloadDataModel(rawModel.getName());
        CubeDescManager.getInstance(kylinConfig).reloadCubeDescLocal(cubeName);
        CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).reloadCubeQuietly(cubeName);
        StreamingSourceConfigManager.getInstance(kylinConfig).reloadStreamingConfigLocal(
                cubeInstance.getRootFactTable());
    }

    private String calLocalSegmentCacheDir() {
        String kylinHome = KylinConfig.getKylinHome();
        String indexPathStr = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        String localSegmentCachePath;
        File indexPath = new File(indexPathStr);

        if (indexPath.isAbsolute()) {
            localSegmentCachePath = indexPathStr;
        } else {
            if (kylinHome != null && !kylinHome.equals("")) {
                File localSegmentFile = new File(kylinHome, indexPathStr);
                localSegmentCachePath = localSegmentFile.getAbsolutePath();
            } else {
                localSegmentCachePath = indexPathStr;
            }
        }
        logger.info("Using {} to store local segment cache.", localSegmentCachePath);
        return localSegmentCachePath;
    }

    private static class SegmentHDFSFlusher implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(SegmentHDFSFlusher.class);
        private StreamingCubeSegment segment;
        private String hdfsPath;

        public SegmentHDFSFlusher(StreamingCubeSegment segment, String hdfsPath) {
            this.segment = segment;
            this.hdfsPath = hdfsPath;
        }

        public void flushToHDFS() throws IOException {
            logger.info("start to flush cube:{} segment:{} to hdfs:{}", segment.getCubeName(),
                    segment.getSegmentName(), hdfsPath);
            final FileSystem fs = HadoopUtil.getFileSystem(hdfsPath);
            final String localPath = segment.getDataSegmentFolder().getPath();
            final Path remotePath = new Path(hdfsPath);
            if (fs.exists(remotePath)) {
                logger.info("the remote path:{} is already exist, skip copy data to remote", remotePath);
                return;
            }
            final Path remoteTempPath = new Path(hdfsPath + ".tmp");
            if (fs.exists(remoteTempPath)) {
                FileStatus sdst = fs.getFileStatus(remoteTempPath);
                if (sdst.isDirectory()) {
                    logger.warn("target temp path: {} is an existed directory, try to delete it.", remoteTempPath);
                    fs.delete(remoteTempPath, true);
                    logger.warn("target temp path: {} is deleted.", remoteTempPath);
                }
            }
            fs.copyFromLocalFile(new Path(localPath), remoteTempPath);
            logger.info("data copy to remote temp path:{}", remoteTempPath);
            boolean renamed = fs.rename(remoteTempPath, remotePath);
            if (renamed) {
                logger.info("successfully rename the temp path to:{}", remotePath);
            }
        }

        @Override
        public void run() {
            try {
                flushToHDFS();
            } catch (Exception e) {
                logger.error("error when flush segment data to hdfs", e);
                throw new IllegalStateException(e);
            }
        }
    }

}
