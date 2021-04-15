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

package org.apache.kylin.stream.coordinator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ServerMode;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.StreamingCubingEngine;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.stream.coordinator.assign.Assigner;
import org.apache.kylin.stream.coordinator.assign.AssignmentUtil;
import org.apache.kylin.stream.coordinator.assign.AssignmentsCache;
import org.apache.kylin.stream.coordinator.assign.CubePartitionRoundRobinAssigner;
import org.apache.kylin.stream.coordinator.assign.DefaultAssigner;
import org.apache.kylin.stream.coordinator.client.CoordinatorClient;
import org.apache.kylin.stream.coordinator.exception.ClusterStateException;
import org.apache.kylin.stream.coordinator.exception.ClusterStateException.ClusterState;
import org.apache.kylin.stream.coordinator.exception.ClusterStateException.TransactionStep;
import org.apache.kylin.stream.coordinator.exception.CoordinateException;
import org.apache.kylin.stream.coordinator.exception.NotLeadCoordinatorException;
import org.apache.kylin.stream.coordinator.exception.StoreException;
import org.apache.kylin.stream.core.client.HttpReceiverAdminClient;
import org.apache.kylin.stream.core.client.ReceiverAdminClient;
import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.model.AssignRequest;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.ResumeConsumerRequest;
import org.apache.kylin.stream.core.model.SegmentBuildState;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.model.StreamingCubeConsumeState;
import org.apache.kylin.stream.core.model.UnAssignRequest;
import org.apache.kylin.stream.core.model.stats.ReceiverCubeStats;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePositionHandler;
import org.apache.kylin.stream.core.source.ISourcePositionHandler.MergeStrategy;
import org.apache.kylin.stream.core.source.IStreamingSource;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceFactory;
import org.apache.kylin.stream.core.source.StreamingTableSourceInfo;
import org.apache.kylin.stream.core.util.HDFSUtil;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.apache.kylin.stream.core.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Collections2;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.MapDifference;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * <pre>
 * Each Kylin streaming cluster has at least one coordinator processes/server, coordinator
 * server works as the master node of streaming cluster and handle generic assignment,
 * membership and streaming cube state management.
 *
 * When cluster have several coordinator processes, only the leader try to answer coordinator client's
 * request, others process will become standby/candidate, so single point of failure will be eliminated.
 * </pre>
 */
@Deprecated
public class Coordinator implements CoordinatorClient {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    private static final int DEFAULT_PORT = 7070;
    private static volatile Coordinator instance = null;

    private StreamMetadataStore streamMetadataStore;
    private Assigner assigner;
    private ReceiverAdminClient receiverAdminClient;
    private CuratorFramework zkClient;
    private CoordinatorLeaderSelector selector;
    private volatile boolean isLead = false;

    private ScheduledExecutorService streamingJobCheckExecutor;

    private StreamingBuildJobStatusChecker jobStatusChecker;

    private Coordinator() {
        this.streamMetadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
        this.receiverAdminClient = new HttpReceiverAdminClient();
        this.assigner = getAssigner();
        this.zkClient = StreamingUtils.getZookeeperClient();
        this.selector = new CoordinatorLeaderSelector();
        this.jobStatusChecker = new StreamingBuildJobStatusChecker();
        this.streamingJobCheckExecutor = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("streaming_job_status_checker"));
        if (ServerMode.SERVER_MODE.canServeStreamingCoordinator()) {
            start();
        }
    }

    @VisibleForTesting
    public Coordinator(StreamMetadataStore metadataStore, ReceiverAdminClient receiverClient) {
        this.streamMetadataStore = metadataStore;
        this.receiverAdminClient = receiverClient;
        this.assigner = getAssigner();
        this.zkClient = StreamingUtils.getZookeeperClient();
        this.selector = new CoordinatorLeaderSelector();
        this.jobStatusChecker = new StreamingBuildJobStatusChecker();
        this.streamingJobCheckExecutor = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("streaming_job_status_checker"));
        if (ServerMode.SERVER_MODE.canServeStreamingCoordinator()) {
            start();
        }
        this.isLead = true;
    }

    public static Coordinator getInstance() {
        if (instance == null) {
            synchronized (Coordinator.class) {
                if (instance == null) {
                    instance = new Coordinator();
                }
            }
        }
        return instance;
    }

    public void start() {
        selector.start();
        streamingJobCheckExecutor.scheduleAtFixedRate(jobStatusChecker, 0, 2, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    public void setReceiverAdminClient(ReceiverAdminClient receiverAdminClient) {
        this.receiverAdminClient = receiverAdminClient;
    }

    @VisibleForTesting
    public void setToLeader() {
        streamMetadataStore.setCoordinatorNode(NodeUtil.getCurrentNode(DEFAULT_PORT));
        this.isLead = true;
    }

    private void restoreJobStatusChecker() {
        logger.info("restore job status checker");
        List<String> cubes = streamMetadataStore.getCubes();
        for (String cube : cubes) {
            List<SegmentBuildState> segmentBuildStates = streamMetadataStore.getSegmentBuildStates(cube);
            Collections.sort(segmentBuildStates);
            for (SegmentBuildState segmentBuildState : segmentBuildStates) {
                if (segmentBuildState.isInBuilding()) {
                    SegmentJobBuildInfo jobBuildInfo = new SegmentJobBuildInfo(cube, segmentBuildState.getSegmentName(),
                            segmentBuildState.getState().getJobId());
                    jobStatusChecker.addSegmentBuildJob(jobBuildInfo);
                }
            }
        }
    }

    /**
     * Assign the streaming cube to replicaSets 
     * 
     * @param cubeName
     * @return
     */
    @Override
    public synchronized void assignCube(String cubeName) {
        checkLead();
        streamMetadataStore.addStreamingCube(cubeName);
        StreamingCubeInfo cube = getStreamCubeInfo(cubeName);
        CubeAssignment existAssignment = streamMetadataStore.getAssignmentsByCube(cube.getCubeName());
        if (existAssignment != null) {
            logger.warn("cube " + cube.getCubeName() + " is already assigned.");
            return;
        }
        List<ReplicaSet> replicaSets = streamMetadataStore.getReplicaSets();
        if (replicaSets == null || replicaSets.isEmpty()) {
            throw new IllegalStateException("no replicaSet is configured in system");
        }
        CubeAssignment assignment = assigner.assign(cube, replicaSets, streamMetadataStore.getAllCubeAssignments());
        doAssignCube(cubeName, assignment);
    }

    @Override
    public void unAssignCube(String cubeName) {
        checkLead();
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        if (assignment == null) {
            return;
        }
        List<Node> unAssignedFailReceivers = Lists.newArrayList();
        try {
            logger.info("send unAssign cube:{} request to receivers", cubeName);
            for (Integer replicaSetID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(replicaSetID);
                UnAssignRequest request = new UnAssignRequest();
                request.setCube(cubeName);
                for (Node receiver : rs.getNodes()) {
                    try {
                        unAssignToReceiver(receiver, request);
                    } catch (Exception e) {
                        logger.error("exception throws when unAssign receiver", e);
                        //                        unAssignedFailReceivers.add(receiver);
                    }
                }
            }
            logger.info("remove temp hdfs files");
            removeCubeHDFSFiles(cubeName);
            logger.info("clear cube info from job check list");
            jobStatusChecker.clearCheckCube(cubeName);
            logger.info("remove cube info in metadata store");
            streamMetadataStore.removeStreamingCube(cubeName);
            AssignmentsCache.getInstance().clearCubeCache(cubeName);
        } catch (Exception e) {
            throw new CoordinateException(e);
        }
        if (unAssignedFailReceivers.size() > 0) {
            String msg = "unAssign fail for receivers:" + unAssignedFailReceivers;
            throw new CoordinateException(msg);
        }
    }

    @Override
    public synchronized void reAssignCube(String cubeName, CubeAssignment assignments) {
        checkLead();
        CubeAssignment preAssignments = streamMetadataStore.getAssignmentsByCube(cubeName);
        if (preAssignments == null) {
            logger.info("no previous cube assign exists, use the new assignment:{}", assignments);
            doAssignCube(cubeName, assignments);
        } else {
            reassignCubeImpl(cubeName, preAssignments, assignments);
        }
    }

    @Override
    public void segmentRemoteStoreComplete(Node receiver, String cubeName, Pair<Long, Long> segment) {
        checkLead();
        logger.info(
                "segment remote store complete signal received for cube:{}, segment:{}, try to find proper segment to build",
                cubeName, segment);
        tryFindAndBuildSegment(cubeName);
    }

    @Override
    public Map<Integer, Map<String, List<Partition>>> reBalanceRecommend() {
        checkLead();
        return reBalancePlan(getEnableStreamingCubes(), streamMetadataStore.getReplicaSets());
    }

    /**
     * reBalance the cube and partitions
     * @param newAssignmentsPlan Map<ReplicaSetID, Map<CubeName, List<Partition>>
     * @return new assignments
     */
    @Override
    public synchronized void reBalance(Map<Integer, Map<String, List<Partition>>> newAssignmentsPlan) {
        checkLead();
        List<CubeAssignment> currCubeAssignments = streamMetadataStore.getAllCubeAssignments();
        List<CubeAssignment> newCubeAssignments = AssignmentUtil.convertReplicaSetAssign2CubeAssign(newAssignmentsPlan);
        doReBalance(currCubeAssignments, newCubeAssignments);
    }

    @Override
    public void pauseConsumers(String cubeName) {
        checkLead();
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        PauseConsumersRequest request = new PauseConsumersRequest();
        request.setCube(cubeName);
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                pauseConsumersInReplicaSet(rs, request);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        streamMetadataStore.saveStreamingCubeConsumeState(cubeName, StreamingCubeConsumeState.PAUSED);
    }

    @Override
    public void resumeConsumers(String cubeName) {
        checkLead();
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        ResumeConsumerRequest request = new ResumeConsumerRequest();
        request.setCube(cubeName);
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                resumeConsumersInReplicaSet(rs, request);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        streamMetadataStore.saveStreamingCubeConsumeState(cubeName, StreamingCubeConsumeState.RUNNING);
    }

    @Override
    public void replicaSetLeaderChange(int replicaSetID, Node newLeader) {
        checkLead();
        Map<String, List<Partition>> assignment = streamMetadataStore.getAssignmentsByReplicaSet(replicaSetID);
        if (assignment == null || assignment.isEmpty()) {
            return;
        }
        // clear assign cache for this group
        for (String cubeName : assignment.keySet()) {
            AssignmentsCache.getInstance().clearCubeCache(cubeName);
        }
    }

    private void checkLead() {
        if (!isLead) {
            Node coordinatorLeader;
            try {
                coordinatorLeader = streamMetadataStore.getCoordinatorNode();
            } catch (StoreException store) {
                throw new NotLeadCoordinatorException("Lead coordinator can not found.", store);
            }
            throw new NotLeadCoordinatorException(
                    "Current coordinator is not lead, please check host " + coordinatorLeader);
        }
    }

    public StreamingCubeInfo getStreamCubeInfo(String cubeName) {
        CubeInstance cube = CubeManager.getInstance(getConfig()).getCube(cubeName);
        if (cube == null) {
            return null;
        }
        int numOfConsumerTasks = cube.getConfig().getStreamingCubeConsumerTasksNum();
        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cube);
        StreamingTableSourceInfo tableSourceInfo = streamingSource.load(cubeName);
        return new StreamingCubeInfo(cubeName, tableSourceInfo, numOfConsumerTasks);
    }

    private KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    private void doAssignCube(String cubeName, CubeAssignment assignment) {
        Set<ReplicaSet> successRS = Sets.newHashSet();
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                assignCubeToReplicaSet(rs, cubeName, assignment.getPartitionsByReplicaSetID(rsID), true, false);
                successRS.add(rs);
            }
            streamMetadataStore.saveNewCubeAssignment(assignment);
        } catch (Exception e) {
            // roll back the success group assignment
            for (ReplicaSet rs : successRS) {
                try {
                    UnAssignRequest request = new UnAssignRequest();
                    request.setCube(cubeName);
                    unAssignFromReplicaSet(rs, request);
                } catch (IOException e1) {
                    logger.error("error when roll back assignment", e);
                }
            }
            throw new RuntimeException(e);
        }
    }

    private CubeAssignment reassignCubeImpl(String cubeName, CubeAssignment preAssignments,
            CubeAssignment newAssignments) {
        logger.info("start cube reBalance, cube:{}, previous assignments:{}, new assignments:{}", cubeName,
                preAssignments, newAssignments);
        if (newAssignments.equals(preAssignments)) {
            logger.info("the new assignment is the same as the previous assignment, do nothing for this reassignment");
            return newAssignments;
        }
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        doReassign(cubeInstance, preAssignments, newAssignments);
        MapDifference<Integer, List<Partition>> assignDiff = Maps.difference(preAssignments.getAssignments(),
                newAssignments.getAssignments());

        // add empty partitions to the removed replica sets, means that there's still data in the replica set, but no new data will be consumed.
        Map<Integer, List<Partition>> removedAssign = assignDiff.entriesOnlyOnLeft();
        for (Integer removedReplicaSet : removedAssign.keySet()) {
            newAssignments.addAssignment(removedReplicaSet, Lists.<Partition> newArrayList());
        }
        streamMetadataStore.saveNewCubeAssignment(newAssignments);
        AssignmentsCache.getInstance().clearCubeCache(cubeName);
        return newAssignments;
    }

    // todo move to source specific implementation?
    void doReassign(CubeInstance cubeInstance, CubeAssignment preAssignments, CubeAssignment newAssignments) {
        String cubeName = preAssignments.getCubeName();
        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cubeInstance);
        MapDifference<Integer, List<Partition>> assignDiff = Maps.difference(preAssignments.getAssignments(),
                newAssignments.getAssignments());
        Map<Integer, List<Partition>> sameAssign = assignDiff.entriesInCommon();
        Map<Integer, List<Partition>> newAssign = assignDiff.entriesOnlyOnRight();
        Map<Integer, List<Partition>> removedAssign = assignDiff.entriesOnlyOnLeft();

        List<ISourcePosition> allPositions = Lists.newArrayList();
        List<ReplicaSet> successSyncReplicaSet = Lists.newArrayList();
        ISourcePosition consumePosition;
        try {
            for (Map.Entry<Integer, List<Partition>> assignmentEntry : preAssignments.getAssignments().entrySet()) {
                Integer replicaSetID = assignmentEntry.getKey();
                if (sameAssign.containsKey(replicaSetID)) {
                    logger.info("the assignment is not changed for cube:{}, replicaSet:{}", cubeName, replicaSetID);
                    continue;
                }
                ReplicaSet rs = getStreamMetadataStore().getReplicaSet(replicaSetID);
                ISourcePosition position = syncAndStopConsumersInRs(streamingSource, cubeName, rs);
                allPositions.add(position);
                successSyncReplicaSet.add(rs);
            }
            consumePosition = streamingSource.getSourcePositionHandler().mergePositions(allPositions,
                    MergeStrategy.KEEP_LARGE);
            logger.info("the consumer position for cube:{} is:{}", cubeName, consumePosition);
        } catch (Exception e) {
            logger.error("fail to sync assign replicaSet for cube:" + cubeName, e);
            // roll back the success group
            Set<Integer> needRollback = successSyncReplicaSet.stream().map(ReplicaSet::getReplicaSetID)
                    .collect(Collectors.toSet());
            for (ReplicaSet rs : successSyncReplicaSet) {
                StartConsumersRequest request = new StartConsumersRequest();
                request.setCube(cubeName);
                try {
                    startConsumersInReplicaSet(rs, request);
                    needRollback.remove(rs.getReplicaSetID());
                } catch (IOException e1) {
                    logger.error("fail to start consumers for cube:" + cubeName + " replicaSet:" + rs.getReplicaSetID(),
                            e1);
                }
            }
            if (needRollback.isEmpty()) {
                throw new ClusterStateException(cubeName, ClusterState.ROLLBACK_SUCCESS, TransactionStep.STOP_AND_SNYC,
                        "", e);
            } else {
                StringBuilder str = new StringBuilder();
                try {
                    str.append("Fail restart:").append(JsonUtil.writeValueAsString(needRollback));
                } catch (JsonProcessingException jpe) {
                    logger.error("", jpe);
                }
                throw new ClusterStateException(cubeName, ClusterState.ROLLBACK_FAILED, TransactionStep.STOP_AND_SNYC,
                        str.toString(), e);
            }
        }

        List<ReplicaSet> successAssigned = Lists.newArrayList();
        List<ReplicaSet> successStarted = Lists.newArrayList();
        Set<Node> failedConvertToImmutableNodes = new HashSet<>();
        // the new assignment is break into two phrase to ensure transactional, first phrase is assign, and second phrase is start consumer.
        try {
            for (Map.Entry<Integer, List<Partition>> cubeAssignmentEntry : newAssignments.getAssignments().entrySet()) {
                Integer replicaSetID = cubeAssignmentEntry.getKey();
                if (sameAssign.containsKey(replicaSetID)) {
                    continue;
                }

                ReplicaSet rs = getStreamMetadataStore().getReplicaSet(replicaSetID);
                logger.info("assign cube:{} to replicaSet:{}", cubeName, replicaSetID);
                assignCubeToReplicaSet(rs, cubeName, cubeAssignmentEntry.getValue(), false, true);
                successAssigned.add(rs);
            }

            for (Map.Entry<Integer, List<Partition>> cubeAssignmentEntry : newAssignments.getAssignments().entrySet()) {
                Integer replicaSetID = cubeAssignmentEntry.getKey();
                if (sameAssign.containsKey(replicaSetID)) {
                    continue;
                }

                ConsumerStartProtocol consumerStartProtocol = new ConsumerStartProtocol(
                        streamingSource.getSourcePositionHandler().serializePosition(consumePosition.advance()));

                ReplicaSet rs = getStreamMetadataStore().getReplicaSet(replicaSetID);
                StartConsumersRequest startRequest = new StartConsumersRequest();
                startRequest.setCube(cubeName);
                startRequest.setStartProtocol(consumerStartProtocol);
                logger.info("start consumers for cube:{}, replicaSet:{}, startRequest:{}", cubeName, replicaSetID,
                        startRequest);
                startConsumersInReplicaSet(rs, startRequest);
                successStarted.add(rs);
            }

            for (Map.Entry<Integer, List<Partition>> removeAssignmentEntry : removedAssign.entrySet()) {
                Integer replicaSetID = removeAssignmentEntry.getKey();
                logger.info("make cube immutable for cube:{}, replicaSet{}", cubeName, replicaSetID);
                ReplicaSet rs = getStreamMetadataStore().getReplicaSet(replicaSetID);
                List<Node> failedNodes = makeCubeImmutableInReplicaSet(rs, cubeName);
                failedConvertToImmutableNodes.addAll(failedNodes);
            }
            if (!failedConvertToImmutableNodes.isEmpty()) {
                throw new IOException();
            }

            logger.info("finish cube reBalance, cube:{}", cubeName);
        } catch (IOException e) {
            logger.error("fail to start consumers for cube:" + cubeName, e);
            // roll back success started
            Set<Integer> rollbackStarted = successStarted.stream().map(ReplicaSet::getReplicaSetID)
                    .collect(Collectors.toSet());
            for (ReplicaSet rs : successStarted) {
                try {
                    StopConsumersRequest stopRequest = new StopConsumersRequest();
                    stopRequest.setCube(cubeName);
                    // for new group assignment, need to stop the consumers and remove the cube data
                    if (newAssign.containsKey(rs.getReplicaSetID())) {
                        stopRequest.setRemoveData(true);
                    }
                    stopConsumersInReplicaSet(rs, stopRequest);
                    rollbackStarted.remove(rs.getReplicaSetID());
                } catch (IOException e1) {
                    logger.error("fail to stop consumers for cube:" + cubeName + " replicaSet:" + rs.getReplicaSetID(),
                            e1);
                }
            }

            // roll back success assignment
            Set<Integer> rollbackAssigned = successAssigned.stream().map(ReplicaSet::getReplicaSetID)
                    .collect(Collectors.toSet());
            for (ReplicaSet rs : successAssigned) {
                try {
                    List<Partition> partitions = preAssignments.getPartitionsByReplicaSetID(rs.getReplicaSetID());
                    assignCubeToReplicaSet(rs, cubeName, partitions, true, true);
                    rollbackAssigned.remove(rs.getReplicaSetID());
                } catch (IOException e1) {
                    logger.error("fail to start consumers for cube:" + cubeName + " replicaSet:" + rs.getReplicaSetID(),
                            e1);
                }
            }

            Set<Node> failedReceiver = new HashSet<>(failedConvertToImmutableNodes);
            for (Node node : failedConvertToImmutableNodes) {
                try {
                    makeCubeImmutableForReceiver(node, cubeName);
                    failedReceiver.remove(node);
                } catch (IOException ioe) {
                    logger.error("fail to make cube immutable for cube:" + cubeName + " to " + node, ioe);
                }
            }

            StringBuilder str = new StringBuilder();
            try {
                str.append("FailStarted:").append(JsonUtil.writeValueAsString(rollbackStarted)).append(";");
                str.append("FailAssigned:").append(JsonUtil.writeValueAsString(rollbackAssigned)).append(";");
                str.append("FailRemotedPresisted:").append(JsonUtil.writeValueAsString(failedReceiver));
            } catch (JsonProcessingException jpe) {
                logger.error("", jpe);
            }

            String failedInfo = str.toString();

            if (!rollbackStarted.isEmpty()) {
                throw new ClusterStateException(cubeName, ClusterState.ROLLBACK_FAILED, TransactionStep.START_NEW,
                        failedInfo, e);
            } else if (!rollbackAssigned.isEmpty()) {
                throw new ClusterStateException(cubeName, ClusterState.ROLLBACK_FAILED, TransactionStep.ASSIGN_NEW,
                        failedInfo, e);
            } else if (!failedReceiver.isEmpty()) {
                throw new ClusterStateException(cubeName, ClusterState.ROLLBACK_FAILED, TransactionStep.MAKE_IMMUTABLE,
                        failedInfo, e);
            } else {
                throw new ClusterStateException(cubeName, ClusterState.ROLLBACK_SUCCESS, TransactionStep.ASSIGN_NEW,
                        failedInfo, e);
            }
        }

    }

    /**
     * sync the consumers in the replicaSet, ensure that all consumers in the group consume to the same position
     *
     * @param streamingSource
     * @param cubeName
     * @param replicaSet
     * @return the consume position info.
     */
    private ISourcePosition syncAndStopConsumersInRs(IStreamingSource streamingSource, String cubeName,
            ReplicaSet replicaSet) throws IOException {
        if (replicaSet.getNodes().size() > 1) { // when group nodes more than 1, force to sync the group
            logger.info("sync consume for cube:{}, replicaSet:{}", cubeName, replicaSet.getReplicaSetID());

            PauseConsumersRequest suspendRequest = new PauseConsumersRequest();
            suspendRequest.setCube(cubeName);
            List<ConsumerStatsResponse> allReceiverConsumeState = pauseConsumersInReplicaSet(replicaSet,
                    suspendRequest);

            List<ISourcePosition> consumePositionList = Lists.transform(allReceiverConsumeState,
                    new Function<ConsumerStatsResponse, ISourcePosition>() {
                        @Nullable
                        @Override
                        public ISourcePosition apply(@Nullable ConsumerStatsResponse input) {
                            return streamingSource.getSourcePositionHandler().parsePosition(input.getConsumePosition());
                        }
                    });
            ISourcePosition consumePosition = streamingSource.getSourcePositionHandler()
                    .mergePositions(consumePositionList, MergeStrategy.KEEP_LARGE);
            ResumeConsumerRequest resumeRequest = new ResumeConsumerRequest();
            resumeRequest.setCube(cubeName);
            resumeRequest
                    .setResumeToPosition(streamingSource.getSourcePositionHandler().serializePosition(consumePosition));
            // assume that the resume will always succeed when the replica set can be paused successfully
            resumeConsumersInReplicaSet(replicaSet, resumeRequest);
            return consumePosition;
        } else if (replicaSet.getNodes().size() == 1) {
            Node receiver = replicaSet.getNodes().iterator().next();
            StopConsumersRequest request = new StopConsumersRequest();
            request.setCube(cubeName);
            logger.info("stop consumers for cube:{}, receiver:{}", cubeName, receiver);
            List<ConsumerStatsResponse> stopResponse = stopConsumersInReplicaSet(replicaSet, request);
            return streamingSource.getSourcePositionHandler().parsePosition(stopResponse.get(0).getConsumePosition());
        } else {
            return null;
        }
    }

    public Map<Integer, Map<String, List<Partition>>> reBalancePlan(List<StreamingCubeInfo> allCubes,
            List<ReplicaSet> allReplicaSets) {
        List<CubeAssignment> currCubeAssignments = streamMetadataStore.getAllCubeAssignments();
        return assigner.reBalancePlan(allReplicaSets, allCubes, currCubeAssignments);
    }

    private List<StreamingCubeInfo> getEnableStreamingCubes() {
        List<StreamingCubeInfo> allCubes = getStreamingCubes();
        List<StreamingCubeInfo> result = Lists.newArrayList();
        for (StreamingCubeInfo cube : allCubes) {
            CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getCube(cube.getCubeName());
            if (cubeInstance.getStatus() == RealizationStatusEnum.READY) {
                result.add(cube);
            }
        }
        return result;
    }

    private List<StreamingCubeInfo> getStreamingCubes() {
        List<String> cubes = streamMetadataStore.getCubes();
        List<StreamingCubeInfo> result = Lists.newArrayList();
        for (String cubeName : cubes) {
            StreamingCubeInfo cubeInfo = getStreamCubeInfo(cubeName);
            if (cubeInfo != null) {
                result.add(cubeInfo);
            }
        }
        return result;
    }

    public void removeCubeHDFSFiles(String cubeName) {
        String segmentHDFSPath = HDFSUtil.getStreamingCubeFilePath(cubeName);
        try {
            FileSystem fs = HadoopUtil.getFileSystem(segmentHDFSPath);
            fs.delete(new Path(segmentHDFSPath), true);
        } catch (Exception e) {
            logger.error("error when remove hdfs file, hdfs path:{}", segmentHDFSPath);
        }
    }

    public synchronized void createReplicaSet(ReplicaSet rs) {
        int replicaSetID = streamMetadataStore.createReplicaSet(rs);
        try {
            for (Node receiver : rs.getNodes()) {
                addReceiverToReplicaSet(receiver, replicaSetID);
            }
        } catch (IOException e) {
            logger.warn("create replica set fail", e);
        }
    }

    public synchronized void removeReplicaSet(int rsID) {
        ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
        if (rs == null) {
            return;
        }
        if (rs.getNodes() != null && rs.getNodes().size() > 0) {
            throw new CoordinateException("cannot remove rs, because there are nodes in it");
        }
        Map<String, List<Partition>> assignment = streamMetadataStore.getAssignmentsByReplicaSet(rsID);
        if (assignment != null && !assignment.isEmpty()) {
            throw new CoordinateException("cannot remove rs, because there are assignments");
        }
        streamMetadataStore.removeReplicaSet(rsID);
    }

    public synchronized void addNodeToReplicaSet(Integer replicaSetID, String nodeID) {
        ReplicaSet rs = streamMetadataStore.getReplicaSet(replicaSetID);
        Node receiver = Node.fromNormalizeString(nodeID);
        List<ReplicaSet> allReplicaSet = streamMetadataStore.getReplicaSets();
        for (ReplicaSet other : allReplicaSet) {
            if (other.getReplicaSetID() != replicaSetID) {
                if (other.getNodes().contains(receiver)) {
                    logger.error("error add Node {} to replicaSet {}, already exist in replicaSet {} ", nodeID,
                            replicaSetID, other.getReplicaSetID());
                    throw new IllegalStateException("Node exists in ReplicaSet!");
                }
            }
        }
        rs.addNode(receiver);
        streamMetadataStore.updateReplicaSet(rs);
        try {
            Map<String, List<Partition>> assignment = streamMetadataStore.getAssignmentsByReplicaSet(replicaSetID);
            if (assignment == null || assignment.isEmpty()) {
                return;
            }
            addReceiverToReplicaSet(receiver, replicaSetID);
            // clear assign cache for this group
            for (String cubeName : assignment.keySet()) {
                AssignmentsCache.getInstance().clearCubeCache(cubeName);
            }
        } catch (IOException e) {
            logger.warn("fail to add receiver to replicaSet ", e);
        }
    }

    public synchronized void removeNodeFromReplicaSet(Integer replicaSetID, String nodeID) {
        ReplicaSet rs = streamMetadataStore.getReplicaSet(replicaSetID);
        Node receiver = Node.fromNormalizeString(nodeID);
        rs.removeNode(receiver);
        streamMetadataStore.updateReplicaSet(rs);
        try {
            Map<String, List<Partition>> assignment = streamMetadataStore.getAssignmentsByReplicaSet(replicaSetID);
            removeReceiverFromReplicaSet(receiver);
            // clear assign cache for this group
            if (assignment != null) {
                for (String cubeName : assignment.keySet()) {
                    AssignmentsCache.getInstance().clearCubeCache(cubeName);
                }
            }
        } catch (IOException e) {
            logger.warn("remove node from replicaSet fail", e);
        }
    }

    public void stopConsumers(String cubeName) {
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        StopConsumersRequest request = new StopConsumersRequest();
        request.setCube(cubeName);
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                stopConsumersInReplicaSet(rs, request);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void doReBalance(List<CubeAssignment> previousAssignments, List<CubeAssignment> newAssignments) {
        Map<String, CubeAssignment> previousCubeAssignMap = Maps.newHashMap();
        Map<String, CubeAssignment> newCubeAssignMap = Maps.newHashMap();
        for (CubeAssignment cubeAssignment : previousAssignments) {
            previousCubeAssignMap.put(cubeAssignment.getCubeName(), cubeAssignment);
        }
        for (CubeAssignment cubeAssignment : newAssignments) {
            newCubeAssignMap.put(cubeAssignment.getCubeName(), cubeAssignment);
        }
        try {
            Set<String> preCubes = previousCubeAssignMap.keySet();
            Set<String> newCubes = newCubeAssignMap.keySet();
            if (!preCubes.equals(newCubes)) {
                logger.error("previous assignment cubes:" + preCubes + ", new assignment cubes:" + newCubes);
                throw new IllegalStateException("previous cube assignments");
            }

            MapDifference<String, CubeAssignment> diff = Maps.difference(previousCubeAssignMap, newCubeAssignMap);
            Map<String, MapDifference.ValueDifference<CubeAssignment>> changedAssignments = diff.entriesDiffering();

            for (Map.Entry<String, MapDifference.ValueDifference<CubeAssignment>> changedAssignmentEntry : changedAssignments
                    .entrySet()) {
                String cubeName = changedAssignmentEntry.getKey();
                MapDifference.ValueDifference<CubeAssignment> cubeAssignDiff = changedAssignmentEntry.getValue();
                reassignCubeImpl(cubeName, cubeAssignDiff.leftValue(), cubeAssignDiff.rightValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void assignCubeToReplicaSet(ReplicaSet rs, String cubeName, List<Partition> partitions,
            boolean startConsumer, boolean mustAllSucceed) throws IOException {
        boolean hasNodeAssigned = false;
        IOException exception = null;
        AssignRequest assignRequest = new AssignRequest();
        assignRequest.setCubeName(cubeName);
        assignRequest.setPartitions(partitions);
        assignRequest.setStartConsumers(startConsumer);
        for (final Node node : rs.getNodes()) {
            try {
                assignToReceiver(node, assignRequest);
                hasNodeAssigned = true;
            } catch (IOException e) {
                if (mustAllSucceed) {
                    throw e;
                }
                exception = e;
                logger.error("cube:" + cubeName + " consumers start fail for node:" + node.toString(), e);
            }
        }
        if (!hasNodeAssigned) {
            if (exception != null) {
                throw exception;
            }
        }
    }

    private void assignToReceiver(final Node receiver, final AssignRequest request) throws IOException {
        receiverAdminClient.assign(receiver, request);
    }

    private void unAssignFromReplicaSet(final ReplicaSet rs, final UnAssignRequest unAssignRequest) throws IOException {
        for (Node receiver : rs.getNodes()) {
            unAssignToReceiver(receiver, unAssignRequest);
        }
    }

    private void unAssignToReceiver(final Node receiver, final UnAssignRequest request) throws IOException {
        receiverAdminClient.unAssign(receiver, request);
    }

    private void addReceiverToReplicaSet(final Node receiver, final int replicaSetID) throws IOException {
        receiverAdminClient.addToReplicaSet(receiver, replicaSetID);
    }

    private void removeReceiverFromReplicaSet(final Node receiver) throws IOException {
        receiverAdminClient.removeFromReplicaSet(receiver);
    }

    private void startConsumersForReceiver(final Node receiver, final StartConsumersRequest request)
            throws IOException {
        receiverAdminClient.startConsumers(receiver, request);
    }

    private ConsumerStatsResponse stopConsumersForReceiver(final Node receiver, final StopConsumersRequest request)
            throws IOException {
        return receiverAdminClient.stopConsumers(receiver, request);
    }

    private ConsumerStatsResponse pauseConsumersForReceiver(final Node receiver, final PauseConsumersRequest request)
            throws IOException {
        return receiverAdminClient.pauseConsumers(receiver, request);
    }

    public ConsumerStatsResponse resumeConsumersForReceiver(final Node receiver, final ResumeConsumerRequest request)
            throws IOException {
        return receiverAdminClient.resumeConsumers(receiver, request);
    }

    private void makeCubeImmutableForReceiver(final Node receiver, final String cubeName) throws IOException {
        receiverAdminClient.makeCubeImmutable(receiver, cubeName);
    }

    public void startConsumersInReplicaSet(ReplicaSet rs, final StartConsumersRequest request) throws IOException {
        for (final Node node : rs.getNodes()) {
            startConsumersForReceiver(node, request);
        }
    }

    public List<Node> makeCubeImmutableInReplicaSet(ReplicaSet rs, String cubeName) throws IOException {
        List<Node> failedNodes = new ArrayList<>();
        for (final Node node : rs.getNodes()) {
            try {
                makeCubeImmutableForReceiver(node, cubeName);
            } catch (IOException ioe) {
                logger.error(String.format(Locale.ROOT, "Convert %s to immutable for node %s failed.", cubeName,
                        node.toNormalizeString()), ioe);
                failedNodes.add(node);
            }
        }
        return failedNodes;
    }

    public List<ConsumerStatsResponse> stopConsumersInReplicaSet(ReplicaSet rs, final StopConsumersRequest request)
            throws IOException {
        List<ConsumerStatsResponse> consumerStats = Lists.newArrayList();
        for (final Node node : rs.getNodes()) {
            consumerStats.add(stopConsumersForReceiver(node, request));
        }
        return consumerStats;
    }

    public List<ConsumerStatsResponse> pauseConsumersInReplicaSet(ReplicaSet rs, final PauseConsumersRequest request)
            throws IOException {
        List<ConsumerStatsResponse> consumerStats = Lists.newArrayList();
        List<Node> successReceivers = Lists.newArrayList();
        try {
            for (final Node node : rs.getNodes()) {
                consumerStats.add(pauseConsumersForReceiver(node, request));
                successReceivers.add(node);
            }
        } catch (IOException ioe) {
            //roll back
            logger.info("roll back pause consumers for receivers:" + successReceivers);
            ResumeConsumerRequest resumeRequest = new ResumeConsumerRequest();
            resumeRequest.setCube(request.getCube());
            for (Node receiver : successReceivers) {
                resumeConsumersForReceiver(receiver, resumeRequest);
            }
            throw ioe;
        }
        return consumerStats;
    }

    public List<ConsumerStatsResponse> resumeConsumersInReplicaSet(ReplicaSet rs, final ResumeConsumerRequest request)
            throws IOException {
        List<ConsumerStatsResponse> consumerStats = Lists.newArrayList();
        for (final Node node : rs.getNodes()) {
            consumerStats.add(resumeConsumersForReceiver(node, request));
        }
        return consumerStats;
    }

    public StreamMetadataStore getStreamMetadataStore() {
        return streamMetadataStore;
    }

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    private synchronized boolean tryFindAndBuildSegment(String cubeName) {
        List<SegmentBuildState> segmentStates = streamMetadataStore.getSegmentBuildStates(cubeName);
        if (segmentStates.isEmpty()) {
            logger.info("no segment build states for cube:{} found in the metadata store", cubeName);
            return true;
        }
        boolean triggered = true;
        List<String> segmentsToBuild = findSegmentsCanBuild(cubeName);
        if (segmentsToBuild != null && !segmentsToBuild.isEmpty()) {
            logger.info("try to trigger cube building for cube:{}, segments:{}", cubeName, segmentsToBuild);
            for (String segmentName : segmentsToBuild) {
                triggered = triggered && triggerSegmentBuild(cubeName, segmentName);
            }
        }

        if (!triggered) {
            jobStatusChecker.addPendingCube(cubeName);
        }
        return triggered;
    }

    private void segmentBuildComplete(CubingJob cubingJob, CubeInstance cubeInstance, CubeSegment cubeSegment,
            SegmentJobBuildInfo segmentBuildInfo) throws IOException {
        if (!checkPreviousSegmentReady(cubeSegment)) {
            logger.warn("the segment:{}'s previous segment is not ready, will not set the segment to ready",
                    cubeSegment);
            return;
        }
        if (!SegmentStatusEnum.READY.equals(cubeSegment.getStatus())) {
            promoteNewSegment(cubingJob, cubeInstance, cubeSegment);
        }
        String cubeName = segmentBuildInfo.cubeName;
        String segmentName = segmentBuildInfo.segmentName;
        CubeAssignment assignments = streamMetadataStore.getAssignmentsByCube(cubeName);
        for (int replicaSetID : assignments.getReplicaSetIDs()) {
            ReplicaSet rs = streamMetadataStore.getReplicaSet(replicaSetID);
            for (Node node : rs.getNodes()) {
                try {
                    receiverAdminClient.segmentBuildComplete(node, cubeName, segmentName);
                } catch (IOException e) {
                    logger.error("error when remove cube segment for receiver:" + node, e);
                }
            }
            // for the assignment that doesn't have partitions, check if there is local segments exist
            if (assignments.getPartitionsByReplicaSetID(replicaSetID).isEmpty()) {
                logger.info(
                        "no partition is assign to the replicaSet:{}, check whether there are local segments on the rs.",
                        replicaSetID);
                Node leader = rs.getLeader();
                try {
                    ReceiverCubeStats receiverCubeStats = receiverAdminClient.getReceiverCubeStats(leader, cubeName);
                    Set<String> segments = receiverCubeStats.getSegmentStatsMap().keySet();
                    if (segments.isEmpty()) {
                        logger.info("no local segments exist for replicaSet:{}, cube:{}, update assignments.",
                                replicaSetID, cubeName);
                        assignments.removeAssignment(replicaSetID);
                        streamMetadataStore.saveNewCubeAssignment(assignments);
                    }
                } catch (IOException e) {
                    logger.error("error when get receiver cube stats from:" + leader, e);
                }
            }
        }
        streamMetadataStore.removeSegmentBuildState(cubeName, segmentName);
        logger.info("try to remove the hdfs files for cube:{} segment:{}", cubeName, segmentName);
        removeHDFSFiles(cubeName, segmentName);
        logger.info("try to find segments for cube:{} build", cubeName);
        tryFindAndBuildSegment(segmentBuildInfo.cubeName); // try to build new segment immediately after build complete
    }

    private void promoteNewSegment(CubingJob cubingJob, CubeInstance cubeInstance, CubeSegment cubeSegment)
            throws IOException {
        long sourceCount = cubingJob.findSourceRecordCount();
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();
        Map<Integer, String> sourceCheckpoint = streamMetadataStore.getSourceCheckpoint(cubeInstance.getName(),
                cubeSegment.getName());

        ISourcePositionHandler positionOperator = StreamingSourceFactory.getStreamingSource(cubeInstance)
                .getSourcePositionHandler();
        Collection<ISourcePosition> sourcePositions = Collections2.transform(sourceCheckpoint.values(),
                new Function<String, ISourcePosition>() {
                    @Nullable
                    @Override
                    public ISourcePosition apply(@Nullable String input) {
                        return positionOperator.parsePosition(input);
                    }
                });
        ISourcePosition sourcePosition = positionOperator.mergePositions(sourcePositions, MergeStrategy.KEEP_SMALL);
        cubeSegment.setLastBuildJobID(cubingJob.getId());
        cubeSegment.setLastBuildTime(System.currentTimeMillis());
        cubeSegment.setSizeKB(cubeSizeBytes / 1024);
        cubeSegment.setInputRecords(sourceCount);
        cubeSegment.setInputRecordsSize(sourceSizeBytes);
        cubeSegment.setStreamSourceCheckpoint(positionOperator.serializePosition(sourcePosition));
        getCubeManager().promoteNewlyBuiltSegments(cubeInstance, cubeSegment);
    }

    private boolean checkPreviousSegmentReady(CubeSegment currSegment) {
        long currSegmentStart = currSegment.getTSRange().start.v;
        CubeInstance cubeInstance = currSegment.getCubeInstance();
        Segments<CubeSegment> segments = cubeInstance.getSegments();
        long previousSegmentEnd = -1;
        for (CubeSegment segment : segments) {
            long segmentEnd = segment.getTSRange().end.v;
            if (segmentEnd <= currSegmentStart && segmentEnd > previousSegmentEnd) {
                previousSegmentEnd = segmentEnd;
            }
        }

        if (previousSegmentEnd == -1) {//no previous segment exist
            return true;
        }

        for (CubeSegment segment : segments) {
            long segmentEnd = segment.getTSRange().end.v;
            if (segmentEnd == previousSegmentEnd && SegmentStatusEnum.READY.equals(segment.getStatus())) {
                return true; // any previous segment is ready return true
            }
        }
        return false;
    }

    private void removeHDFSFiles(String cubeName, String segmentName) {
        String segmentHDFSPath = HDFSUtil.getStreamingSegmentFilePath(cubeName, segmentName);
        try {
            FileSystem fs = HadoopUtil.getFileSystem(segmentHDFSPath);
            fs.delete(new Path(segmentHDFSPath), true);
        } catch (Exception e) {
            logger.error("error when remove hdfs file, hdfs path:{}", segmentHDFSPath);
        }
    }

    private boolean triggerSegmentBuild(String cubeName, String segmentName) {
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        try {
            Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentName);
            logger.info("submit streaming segment build, cube:{} segment:{}", cubeName, segmentName);
            CubeSegment newSeg = getCubeManager().appendSegment(cubeInstance,
                    new TSRange(segmentRange.getFirst(), segmentRange.getSecond()));
            DefaultChainedExecutable executable = new StreamingCubingEngine().createStreamingCubingJob(newSeg,
                    "SYSTEM");
            getExecutableManager().addJob(executable);
            CubingJob cubingJob = (CubingJob) executable;
            newSeg.setLastBuildJobID(cubingJob.getId());

            SegmentJobBuildInfo segmentJobBuildInfo = new SegmentJobBuildInfo(cubeName, segmentName, cubingJob.getId());
            jobStatusChecker.addSegmentBuildJob(segmentJobBuildInfo);
            SegmentBuildState.BuildState state = new SegmentBuildState.BuildState();
            state.setBuildStartTime(System.currentTimeMillis());
            state.setState(SegmentBuildState.BuildState.State.BUILDING);
            state.setJobId(cubingJob.getId());
            streamMetadataStore.updateSegmentBuildState(cubeName, segmentName, state);
            return true;
        } catch (Exception e) {
            logger.error("streaming job submit fail, cubeName:" + cubeName + " segment:" + segmentName, e);
            return false;
        }
    }

    private List<String> findSegmentsCanBuild(String cubeName) {
        List<String> result = Lists.newArrayList();
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        // in optimization
        if (isInOptimize(cubeInstance)) {
            return result;
        }
        int allowMaxBuildingSegments = cubeInstance.getConfig().getMaxBuildingSegments();
        CubeSegment latestHistoryReadySegment = cubeInstance.getLatestReadySegment();
        long minSegmentStart = -1;
        if (latestHistoryReadySegment != null) {
            minSegmentStart = latestHistoryReadySegment.getTSRange().end.v;
        } else {
            // there is no ready segment, to make cube planner work, only 1 segment can build
            logger.info("there is no ready segments for cube:{}, so only allow 1 segment build concurrently", cubeName);
            allowMaxBuildingSegments = 1;
        }

        CubeAssignment assignments = streamMetadataStore.getAssignmentsByCube(cubeName);
        Set<Integer> cubeAssignedReplicaSets = assignments.getReplicaSetIDs();
        List<SegmentBuildState> segmentStates = streamMetadataStore.getSegmentBuildStates(cubeName);
        Collections.sort(segmentStates);
        // TODO need to check whether it is in optimization
        int inBuildingSegments = cubeInstance.getBuildingSegments().size();
        int leftQuota = allowMaxBuildingSegments - inBuildingSegments;

        for (int i = 0; i < segmentStates.size(); i++) {
            SegmentBuildState segmentState = segmentStates.get(i);
            Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentState.getSegmentName());
            if (segmentRange.getFirst() < minSegmentStart) {
                logger.warn("the cube segment state is not clear correctly, cube:{} segment:{}, clear it", cubeName,
                        segmentState.getSegmentName());
                streamMetadataStore.removeSegmentBuildState(cubeName, segmentState.getSegmentName());
                continue;
            }

            if (segmentState.isInBuilding()) {
                inBuildingSegments++;
                String jobId = segmentState.getState().getJobId();
                logger.info("there is segment in building, cube:{} segment:{} jobId:{}", cubeName,
                        segmentState.getSegmentName(), jobId);
                long buildStartTime = segmentState.getState().getBuildStartTime();
                if (buildStartTime != 0 && jobId != null) {
                    long buildDuration = System.currentTimeMillis() - buildStartTime;
                    if (buildDuration < 40 * 60 * 1000) { // if build time larger than 40 minutes, check the job status
                        continue;
                    }
                    CubingJob cubingJob = (CubingJob) getExecutableManager().getJob(jobId);
                    ExecutableState jobState = cubingJob.getStatus();
                    if (ExecutableState.SUCCEED.equals(jobState)) { // job is already succeed, remove the build state
                        CubeSegment cubeSegment = cubeInstance.getSegment(segmentState.getSegmentName(), null);
                        if (cubeSegment != null && SegmentStatusEnum.READY == cubeSegment.getStatus()) {
                            logger.info(
                                    "job:{} is already succeed, and segment:{} is ready, remove segment build state",
                                    jobId, segmentState.getSegmentName());
                            streamMetadataStore.removeSegmentBuildState(cubeName, segmentState.getSegmentName());
                        }
                        continue;
                    } else if (ExecutableState.ERROR.equals(jobState)) {
                        logger.info("job:{} is error, resume the job", jobId);
                        getExecutableManager().resumeJob(jobId);
                        continue;
                    } else if (ExecutableState.DISCARDED.equals(jobState)) {
                        // if the job has been discard manually, just think that the segment is not in building
                        logger.info("job:{} is discard, reset the job state in metaStore", jobId);
                        SegmentBuildState.BuildState state = new SegmentBuildState.BuildState();
                        state.setBuildStartTime(0);
                        state.setState(SegmentBuildState.BuildState.State.WAIT);
                        state.setJobId(cubingJob.getId());
                        streamMetadataStore.updateSegmentBuildState(cubeName, segmentState.getSegmentName(), state);
                        segmentState.setState(state);
                        logger.info("segment:{} is discard", segmentState.getSegmentName());
                        continue;
                    } else {
                        logger.info("job:{} is in running, job state: {}", jobId, jobState);
                        continue;
                    }
                }
            }
            if (leftQuota <= 0) {
                logger.info("No left quota to build segments for cube:{}", cubeName);
                return result;
            }
            if (!checkSegmentIsReadyToBuild(segmentStates, i, cubeAssignedReplicaSets)) {
                break;
            }
            result.add(segmentState.getSegmentName());
            leftQuota--;
        }
        return result;
    }

    private boolean isInOptimize(CubeInstance cube) {
        Segments<CubeSegment> readyPendingSegments = cube.getSegments(SegmentStatusEnum.READY_PENDING);
        if (readyPendingSegments.size() > 0) {
            logger.info("The cube {} has READY_PENDING segments {}. It's not allowed for building", cube.getName(),
                    readyPendingSegments);
            return true;
        }
        Segments<CubeSegment> newSegments = cube.getSegments(SegmentStatusEnum.NEW);
        for (CubeSegment newSegment : newSegments) {
            String jobId = newSegment.getLastBuildJobID();
            if (jobId == null) {
                continue;
            }
            AbstractExecutable job = getExecutableManager().getJob(jobId);
            if (job != null && job instanceof CubingJob) {
                CubingJob cubingJob = (CubingJob) job;
                if (CubingJob.CubingJobTypeEnum.OPTIMIZE.toString().equals(cubingJob.getJobType())) {
                    logger.info(
                            "The cube {} is in optimization. It's not allowed to build new segments during optimization.",
                            cube.getName());
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * <pre>
     *     When all replica sets have uploaded their local segment cache to remote, we can mark
     *     this segment as ready to submit a MapReduce job to build into HBase.
     *
     *     Note the special situation, when some replica set didn't upload any data in some segment duration for lack
     *     of entered kafka event, we still try to check the newer segment duration, if found some newer segment have data
     *     uploaded for current miss replica set, we marked local segment cache has been uploaded for that replica for current segment.
     *     This workround will prevent job-submit queue from blocking by no data for some topic partition.
     * </pre>
     * @return true if all local segment cache has been uploaded successfully, else false
     */
    private boolean checkSegmentIsReadyToBuild(List<SegmentBuildState> allSegmentStates, int checkedSegmentIdx,
            Set<Integer> cubeAssignedReplicaSets) {
        SegmentBuildState checkedSegmentState = allSegmentStates.get(checkedSegmentIdx);
        Set<Integer> notCompleteReplicaSets = Sets
                .newHashSet(Sets.difference(cubeAssignedReplicaSets, checkedSegmentState.getCompleteReplicaSets()));
        if (notCompleteReplicaSets.isEmpty()) {
            return true;
        } else {
            for (int i = checkedSegmentIdx + 1; i < allSegmentStates.size(); i++) {
                SegmentBuildState segmentBuildState = allSegmentStates.get(i);
                Set<Integer> completeReplicaSetsForNext = segmentBuildState.getCompleteReplicaSets();
                Iterator<Integer> notCompleteRSItr = notCompleteReplicaSets.iterator();
                while (notCompleteRSItr.hasNext()) {
                    Integer rsID = notCompleteRSItr.next();
                    if (completeReplicaSetsForNext.contains(rsID)) {
                        logger.info(
                                "the replica set:{} doesn't have data for segment:{}, but have data for later segment:{}",
                                rsID, checkedSegmentState.getSegmentName(), segmentBuildState.getSegmentName());
                        notCompleteRSItr.remove();
                    }
                }
            }
            if (notCompleteReplicaSets.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private Assigner getAssigner() {
        String assignerName = getConfig().getStreamingAssigner();
        Assigner oneAssigner;
        logger.debug("Using assigner {}", assignerName);
        switch (assignerName) {
        case "DefaultAssigner":
            oneAssigner = new DefaultAssigner();
            break;
        case "CubePartitionRoundRobinAssigner":
            oneAssigner = new CubePartitionRoundRobinAssigner();
            break;
        default:
            oneAssigner = new DefaultAssigner();
        }
        return oneAssigner;
    }

    private class CoordinatorLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
        private LeaderSelector leaderSelector;

        public CoordinatorLeaderSelector() {
            String path = StreamingUtils.COORDINATOR_LEAD;
            leaderSelector = new LeaderSelector(zkClient, path, this);
            leaderSelector.autoRequeue();
        }

        @Override
        public void close() throws IOException {
            leaderSelector.close();
        }

        public void start() {
            leaderSelector.start();
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            logger.info("current node become the lead coordinator");
            streamMetadataStore.setCoordinatorNode(NodeUtil.getCurrentNode(DEFAULT_PORT));
            isLead = true;
            // check job status every minute
            restoreJobStatusChecker();
            while (true) {
                try {
                    Thread.sleep(5 * 60 * 1000L);
                } catch (InterruptedException exception) {
                    Thread.interrupted();
                    break;
                }
                if (!leaderSelector.hasLeadership()) {
                    break;
                }
            }
            logger.info("become the follower coordinator");
            isLead = false;
        }

    }

    private class StreamingBuildJobStatusChecker implements Runnable {
        private int maxJobTryCnt = 5;
        private ConcurrentMap<String, ConcurrentSkipListSet<SegmentJobBuildInfo>> segmentBuildJobMap = Maps
                .newConcurrentMap();
        private CopyOnWriteArrayList<String> pendingCubeName = Lists.newCopyOnWriteArrayList();

        public void addSegmentBuildJob(SegmentJobBuildInfo segmentBuildJob) {
            ConcurrentSkipListSet<SegmentJobBuildInfo> buildInfos = segmentBuildJobMap.get(segmentBuildJob.cubeName);
            if (buildInfos == null) {
                buildInfos = new ConcurrentSkipListSet<>();
                ConcurrentSkipListSet<SegmentJobBuildInfo> previousValue = segmentBuildJobMap
                        .putIfAbsent(segmentBuildJob.cubeName, buildInfos);
                if (previousValue != null) {
                    buildInfos = previousValue;
                }
            }
            buildInfos.add(segmentBuildJob);
        }

        public void addPendingCube(String cubeName) {
            if (!pendingCubeName.contains(cubeName)) {
                pendingCubeName.add(cubeName);
            }
        }

        public void clearCheckCube(String cubeName) {
            if (pendingCubeName.contains(cubeName)) {
                pendingCubeName.remove(cubeName);
            }
            segmentBuildJobMap.remove(cubeName);
        }

        @Override
        public void run() {
            try {
                if (isLead) {
                    doRun();
                }
            } catch (Exception e) {
                logger.error("error", e);
            }
        }

        private void doRun() {
            List<SegmentJobBuildInfo> successJobs = Lists.newArrayList();
            for (ConcurrentSkipListSet<SegmentJobBuildInfo> buildInfos : segmentBuildJobMap.values()) {
                if (buildInfos.isEmpty()) {
                    continue;
                }
                SegmentJobBuildInfo segmentBuildJob = buildInfos.first();
                logger.info("check the cube:{} segment:{} build status", segmentBuildJob.cubeName,
                        segmentBuildJob.segmentName);
                try {
                    CubingJob cubingJob = (CubingJob) getExecutableManager().getJob(segmentBuildJob.jobID);
                    ExecutableState jobState = cubingJob.getStatus();
                    if (ExecutableState.SUCCEED.equals(jobState)) {
                        logger.info("job:{} is complete", segmentBuildJob);
                        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
                        CubeInstance cubeInstance = cubeManager.getCube(segmentBuildJob.cubeName).latestCopyForWrite();
                        CubeSegment cubeSegment = cubeInstance.getSegment(segmentBuildJob.segmentName, null);
                        logger.info("the cube:{} segment:{} is ready", segmentBuildJob.cubeName,
                                segmentBuildJob.segmentName);
                        segmentBuildComplete(cubingJob, cubeInstance, cubeSegment, segmentBuildJob);
                        successJobs.add(segmentBuildJob);
                    } else if (ExecutableState.ERROR.equals(jobState)) {
                        if (segmentBuildJob.retryCnt < maxJobTryCnt) {
                            logger.info("job:{} is error, resume the job", segmentBuildJob);
                            getExecutableManager().resumeJob(segmentBuildJob.jobID);
                            segmentBuildJob.retryCnt++;
                        }
                    }
                } catch (Exception e) {
                    logger.error("error when check streaming segment job build state:" + segmentBuildJob, e);
                }
            }

            for (SegmentJobBuildInfo successJob : successJobs) {
                ConcurrentSkipListSet<SegmentJobBuildInfo> buildInfos = segmentBuildJobMap.get(successJob.cubeName);
                buildInfos.remove(successJob);
            }

            List<String> successCubes = Lists.newArrayList();
            for (String cubeName : pendingCubeName) {
                logger.info("check the pending cube:{} ", cubeName);
                try {
                    if (tryFindAndBuildSegment(cubeName)) {
                        successCubes.add(cubeName);
                    }
                } catch (Exception e) {
                    logger.error("error when try to find and build cube segment:{}" + cubeName, e);
                }
            }

            for (String successCube : successCubes) {
                pendingCubeName.remove(successCube);
            }
        }
    }

    private class SegmentJobBuildInfo implements Comparable<SegmentJobBuildInfo> {
        public String cubeName;
        public String segmentName;
        public String jobID;
        public int retryCnt = 0;

        public SegmentJobBuildInfo(String cubeName, String segmentName, String jobID) {
            this.cubeName = cubeName;
            this.segmentName = segmentName;
            this.jobID = jobID;
        }

        @Override
        public String toString() {
            return "SegmentJobBuildInfo{" + "cubeName='" + cubeName + '\'' + ", segmentName='" + segmentName + '\''
                    + ", jobID='" + jobID + '\'' + ", retryCnt=" + retryCnt + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SegmentJobBuildInfo that = (SegmentJobBuildInfo) o;

            if (cubeName != null ? !cubeName.equals(that.cubeName) : that.cubeName != null)
                return false;
            if (segmentName != null ? !segmentName.equals(that.segmentName) : that.segmentName != null)
                return false;
            return jobID != null ? jobID.equals(that.jobID) : that.jobID == null;

        }

        @Override
        public int hashCode() {
            int result = cubeName != null ? cubeName.hashCode() : 0;
            result = 31 * result + (segmentName != null ? segmentName.hashCode() : 0);
            result = 31 * result + (jobID != null ? jobID.hashCode() : 0);
            return result;
        }

        @Override
        public int compareTo(SegmentJobBuildInfo o) {
            if (!cubeName.equals(o.cubeName)) {
                return cubeName.compareTo(o.cubeName);
            }
            return segmentName.compareTo(o.segmentName);
        }
    }

}
