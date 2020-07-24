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
package org.apache.kylin.stream.coordinator.coordinate;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Collections2;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.MapDifference;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.stream.coordinator.assign.AssignmentsCache;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicIdempotent;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicAndNotIdempotent;
import org.apache.kylin.stream.coordinator.exception.ClusterStateException;
import org.apache.kylin.stream.coordinator.exception.StoreException;
import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.model.AssignRequest;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.ResumeConsumerRequest;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.model.stats.ReceiverCubeStats;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePositionHandler;
import org.apache.kylin.stream.core.source.IStreamingSource;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceFactory;
import org.apache.kylin.stream.core.util.HDFSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <pre>
 * This class manage operation related to multi streaming receivers. They are often not atomic and maybe idempotent.
 *
 * In a multi-step transaction, following steps should be thought twice:
 *  1. should fail fast or continue when exception thrown.
 *  2. should API(RPC) be synchronous or asynchronous
 *  3. when transaction failed, will roll back always succeed
 *  4. transaction should be idempotent so when it failed, it could be fixed by retry
 * </pre>
 */
public class ReceiverClusterManager {
    private static final Logger logger = LoggerFactory.getLogger(ReceiverClusterManager.class);

    ReceiverClusterManager(StreamingCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public StreamingCoordinator getCoordinator() {
        return coordinator;
    }

    StreamingCoordinator coordinator;

    // ================================================================================
    // ======================== Rebalance related operation ===========================

    @NotAtomicAndNotIdempotent
    void doReBalance(List<CubeAssignment> previousAssignments, List<CubeAssignment> newAssignments) {
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
                logger.error("previous assignment cubes:{}, new assignment cubes:{}", preCubes, newCubes);
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

    @NotAtomicAndNotIdempotent
    void reassignCubeImpl(String cubeName, CubeAssignment preAssignments, CubeAssignment newAssignments) {
        logger.info("start cube reBalance, cube:{}, previous assignments:{}, new assignments:{}", cubeName,
                preAssignments, newAssignments);
        if (newAssignments.equals(preAssignments)) {
            logger.info("the new assignment is the same as the previous assignment, do nothing for this reassignment");
            return;
        }
        CubeInstance cubeInstance = getCoordinator().getCubeManager().getCube(cubeName);
        doReassignWithoutCommit(cubeInstance, preAssignments, newAssignments);

        // add empty partitions to the removed replica sets, means that there's still data in the replica set, but no new data will be consumed.
        MapDifference<Integer, List<Partition>> assignDiff = Maps.difference(preAssignments.getAssignments(),
                newAssignments.getAssignments());
        Map<Integer, List<Partition>> removedAssign = assignDiff.entriesOnlyOnLeft();
        for (Integer removedReplicaSet : removedAssign.keySet()) {
            newAssignments.addAssignment(removedReplicaSet, Lists.<Partition> newArrayList());
        }

        logger.info("Commit reassign {} transaction.", cubeName);
        getCoordinator().getStreamMetadataStore().saveNewCubeAssignment(newAssignments);
        AssignmentsCache.getInstance().clearCubeCache(cubeName);
    }

    /**
     * <pre>
     * Reassign action is a process which move some consumption task from some replica set to some new replica sets.
     *
     * It is necessary in some case such as :
     *  - new topic partition was added, or receiver can not catch produce rate, so we need scale out
     *  - wordload not balance between different replica set
     *  - some nodes have to be offlined so the consumption task have be transfered
     * </pre>
     *
     * @param preAssignments current assignment
     * @param newAssignments the assignment we want to assign
     */
    @NotAtomicAndNotIdempotent
    void doReassignWithoutCommit(CubeInstance cubeInstance, CubeAssignment preAssignments,
            CubeAssignment newAssignments) {
        String cubeName = preAssignments.getCubeName();

        // Step 0. Prepare and check
        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cubeInstance);
        MapDifference<Integer, List<Partition>> assignDiff = Maps.difference(preAssignments.getAssignments(),
                newAssignments.getAssignments());
        Map<Integer, List<Partition>> sameAssign = assignDiff.entriesInCommon();
        Map<Integer, List<Partition>> newAssign = assignDiff.entriesOnlyOnRight();
        Map<Integer, List<Partition>> removedAssign = assignDiff.entriesOnlyOnLeft();

        List<ISourcePosition> allPositions = Lists.newArrayList();
        List<ReplicaSet> successSyncReplicaSet = Lists.newArrayList();
        ISourcePosition consumePosition;

        // TODO check health state of related receivers before real action will reduce chance of facing inconsistent state

        // Step 1. Stop and sync all replica set in preAssignment
        try {
            for (Map.Entry<Integer, List<Partition>> assignmentEntry : preAssignments.getAssignments().entrySet()) {
                Integer replicaSetID = assignmentEntry.getKey();
                if (sameAssign.containsKey(replicaSetID)) {
                    logger.info("the assignment is not changed for cube:{}, replicaSet:{}", cubeName, replicaSetID);
                    continue;
                }
                ReplicaSet rs = getCoordinator().getStreamMetadataStore().getReplicaSet(replicaSetID);
                ISourcePosition position = syncAndStopConsumersInRs(streamingSource, cubeName, rs);
                allPositions.add(position);
                successSyncReplicaSet.add(rs);
            }
            consumePosition = streamingSource.getSourcePositionHandler().mergePositions(allPositions,
                    ISourcePositionHandler.MergeStrategy.KEEP_LARGE);
            logger.info("the consumer position for cube:{} is:{}", cubeName, consumePosition);
        } catch (Exception e) {
            logger.error("fail to sync assign replicaSet for cube:" + cubeName, e);
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
                throw new ClusterStateException(cubeName, ClusterStateException.ClusterState.ROLLBACK_SUCCESS,
                        ClusterStateException.TransactionStep.STOP_AND_SNYC, "", e);
            } else {
                StringBuilder str = new StringBuilder();
                try {
                    str.append("Fail restart:").append(JsonUtil.writeValueAsString(needRollback));
                } catch (JsonProcessingException jpe) {
                    logger.error("", jpe);
                }
                throw new ClusterStateException(cubeName, ClusterStateException.ClusterState.ROLLBACK_FAILED,
                        ClusterStateException.TransactionStep.STOP_AND_SNYC, str.toString(), e);
            }
        }

        List<ReplicaSet> successAssigned = Lists.newArrayList();
        List<ReplicaSet> successStarted = Lists.newArrayList();
        Set<Node> failedConvertToImmutableNodes = new HashSet<>();

        try {
            // Step 2. Assign consumption task to newAssignment without start consumption
            for (Map.Entry<Integer, List<Partition>> cubeAssignmentEntry : newAssignments.getAssignments().entrySet()) {
                Integer replicaSetID = cubeAssignmentEntry.getKey();
                if (sameAssign.containsKey(replicaSetID)) {
                    continue;
                }

                ReplicaSet rs = getCoordinator().getStreamMetadataStore().getReplicaSet(replicaSetID);
                logger.info("assign cube:{} to replicaSet:{}", cubeName, replicaSetID);
                assignCubeToReplicaSet(rs, cubeName, cubeAssignmentEntry.getValue(), false, true);
                successAssigned.add(rs);
            }

            // Step 3. Start consumption task to newAssignment 
            for (Map.Entry<Integer, List<Partition>> cubeAssignmentEntry : newAssignments.getAssignments().entrySet()) {
                Integer replicaSetID = cubeAssignmentEntry.getKey();
                if (sameAssign.containsKey(replicaSetID)) {
                    continue;
                }

                ConsumerStartProtocol consumerStartProtocol = new ConsumerStartProtocol(
                        streamingSource.getSourcePositionHandler().serializePosition(consumePosition.advance()));

                ReplicaSet rs = getCoordinator().getStreamMetadataStore().getReplicaSet(replicaSetID);
                StartConsumersRequest startRequest = new StartConsumersRequest();
                startRequest.setCube(cubeName);
                startRequest.setStartProtocol(consumerStartProtocol);
                logger.info("start consumers for cube:{}, replicaSet:{}, startRequest:{}", cubeName, replicaSetID,
                        startRequest);
                startConsumersInReplicaSet(rs, startRequest);
                successStarted.add(rs);
            }

            // Step 4. Ask removed replica set to force transfer thier local segment into immutable 
            for (Map.Entry<Integer, List<Partition>> removeAssignmentEntry : removedAssign.entrySet()) {
                Integer replicaSetID = removeAssignmentEntry.getKey();
                logger.info("make cube immutable for cube:{}, replicaSet{}", cubeName, replicaSetID);
                ReplicaSet rs = getCoordinator().getStreamMetadataStore().getReplicaSet(replicaSetID);
                List<Node> failedNodes = makeCubeImmutableInReplicaSet(rs, cubeName);
                failedConvertToImmutableNodes.addAll(failedNodes);
            }
            if (!failedConvertToImmutableNodes.isEmpty()) {
                throw new IOException("Failed to convert to immutable state. ");
            }

            logger.info("Finish cube reassign for cube:{} .", cubeName);
        } catch (IOException e) {
            logger.error("Fail to start consumers for cube:" + cubeName, e);
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
                    getCoordinator().makeCubeImmutableForReceiver(node, cubeName);
                    failedReceiver.remove(node);
                } catch (IOException ioe) {
                    logger.error("fail to make cube immutable for cube:" + cubeName + " to " + node, ioe);
                }
            }

            // summary after reassign action
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
                throw new ClusterStateException(cubeName, ClusterStateException.ClusterState.ROLLBACK_FAILED,
                        ClusterStateException.TransactionStep.START_NEW, failedInfo, e);
            } else if (!rollbackAssigned.isEmpty()) {
                throw new ClusterStateException(cubeName, ClusterStateException.ClusterState.ROLLBACK_FAILED,
                        ClusterStateException.TransactionStep.ASSIGN_NEW, failedInfo, e);
            } else if (!failedReceiver.isEmpty()) {
                throw new ClusterStateException(cubeName, ClusterStateException.ClusterState.ROLLBACK_FAILED,
                        ClusterStateException.TransactionStep.MAKE_IMMUTABLE, failedInfo, e);
            } else {
                throw new ClusterStateException(cubeName, ClusterStateException.ClusterState.ROLLBACK_SUCCESS,
                        ClusterStateException.TransactionStep.ASSIGN_NEW, failedInfo, e);
            }
        }

    }

    // =========================================================================================
    // ======================= Operation related to single replica set   =======================

    /**
     * Sync/align consumers in the replica set, ensure that all consumers in the group consume to the same position.
     *
     * @return the consume position which all receivers have aligned.
     */
    @NotAtomicAndNotIdempotent
    ISourcePosition syncAndStopConsumersInRs(IStreamingSource streamingSource, String cubeName, ReplicaSet replicaSet)
            throws IOException {
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
                    .mergePositions(consumePositionList, ISourcePositionHandler.MergeStrategy.KEEP_LARGE);
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

    @NotAtomicAndNotIdempotent
    void startConsumersInReplicaSet(ReplicaSet rs, final StartConsumersRequest request) throws IOException {
        for (final Node node : rs.getNodes()) {
            getCoordinator().startConsumersForReceiver(node, request);
        }
    }

    @NotAtomicAndNotIdempotent
    List<Node> makeCubeImmutableInReplicaSet(ReplicaSet rs, String cubeName) {
        List<Node> failedNodes = new ArrayList<>();
        for (final Node node : rs.getNodes()) {
            try {
                getCoordinator().makeCubeImmutableForReceiver(node, cubeName);
            } catch (IOException ioe) {
                logger.error(String.format(Locale.ROOT, "Convert %s to immutable for node %s failed.", cubeName,
                        node.toNormalizeString()), ioe);
                failedNodes.add(node);
            }
        }
        return failedNodes;
    }

    @NotAtomicAndNotIdempotent
    List<ConsumerStatsResponse> resumeConsumersInReplicaSet(ReplicaSet rs, final ResumeConsumerRequest request)
            throws IOException {
        List<ConsumerStatsResponse> consumerStats = Lists.newArrayList();
        for (final Node node : rs.getNodes()) {
            consumerStats.add(getCoordinator().resumeConsumersForReceiver(node, request));
        }
        return consumerStats;
    }

    @NotAtomicIdempotent
    List<ConsumerStatsResponse> stopConsumersInReplicaSet(ReplicaSet rs, final StopConsumersRequest request)
            throws IOException {
        List<ConsumerStatsResponse> consumerStats = Lists.newArrayList();
        for (final Node node : rs.getNodes()) {
            consumerStats.add(getCoordinator().stopConsumersForReceiver(node, request));
        }
        return consumerStats;
    }

    @NotAtomicIdempotent
    List<ConsumerStatsResponse> pauseConsumersInReplicaSet(ReplicaSet rs, final PauseConsumersRequest request)
            throws IOException {
        List<ConsumerStatsResponse> consumerStats = Lists.newArrayList();
        List<Node> successReceivers = Lists.newArrayList();
        try {
            for (final Node node : rs.getNodes()) {
                consumerStats.add(getCoordinator().pauseConsumersForReceiver(node, request));
                successReceivers.add(node);
            }
        } catch (IOException ioe) {
            logger.info("Roll back pause consumers for receivers: {}", successReceivers);
            ResumeConsumerRequest resumeRequest = new ResumeConsumerRequest();
            resumeRequest.setCube(request.getCube());
            for (Node receiver : successReceivers) {
                getCoordinator().resumeConsumersForReceiver(receiver, resumeRequest);
            }
            throw ioe;
        }
        return consumerStats;
    }

    /**
     * Assign consumption task of specific topic partitions to current replica set.
     *
     * @param partitions specific topic partitions which replica set should consume
     * @param startConsumer should receiver start consumption at once
     * @param failFast if set to true, we should ensure all receivers has been correctly notified(fail-fast); false
     *                       for ensure at least one receivers has been correctly notified
     *
     * @throws IOException throwed when assign failed
     */
    @NotAtomicIdempotent
    void assignCubeToReplicaSet(ReplicaSet rs, String cubeName, List<Partition> partitions, boolean startConsumer,
            boolean failFast) throws IOException {
        boolean atLeastOneAssigned = false;
        IOException exception = null;
        AssignRequest assignRequest = new AssignRequest();
        assignRequest.setCubeName(cubeName);
        assignRequest.setPartitions(partitions);
        assignRequest.setStartConsumers(startConsumer);
        for (final Node node : rs.getNodes()) {
            try {
                getCoordinator().assignToReceiver(node, assignRequest);
                atLeastOneAssigned = true;
            } catch (IOException e) {
                if (failFast) {
                    throw e;
                }
                exception = e;
                logger.error("Cube:" + cubeName + " consumers start fail for node:" + node.toString(), e);
            }
        }
        if (!atLeastOneAssigned) {
            if (exception != null) {
                throw exception;
            }
        }
    }

    /**
     * When a segment build job succeed, we should do some following job to deliver it to historical part.
     *
     * @throws org.apache.kylin.stream.coordinator.exception.StoreException thrown when write metadata failed
     * @return true if promote succeed, else false
     */
    @NotAtomicIdempotent
    protected boolean segmentBuildComplete(CubingJob cubingJob, CubeInstance cubeInstance, CubeSegment cubeSegment,
            SegmentJobBuildInfo segmentBuildInfo) {
        String cubeName = segmentBuildInfo.cubeName;
        String segmentName = segmentBuildInfo.segmentName;

        // Step 1. check if the ready to promote into HBase Ready Segment and promote current segment
        if (!checkPreviousSegmentReady(cubeSegment)) {
            logger.warn("Segment:{}'s previous segment is not ready, will not set the segment to ready.", cubeSegment);
            return false;
        }

        if (!SegmentStatusEnum.READY.equals(cubeSegment.getStatus())) {
            try {
                promoteNewSegment(cubingJob, cubeInstance, cubeSegment);
            } catch (IOException storeException) {
                throw new StoreException("Promote failed because of metadata store.", storeException);
            }
            logger.debug("Promote {} succeed.", segmentName);
        } else {
            logger.debug("Segment status is: {}", cubeSegment.getStatus());
        }

        // Step 2. delete local segment files in receiver side because these are useless now
        CubeAssignment assignments = getCoordinator().getStreamMetadataStore().getAssignmentsByCube(cubeName);
        for (int replicaSetID : assignments.getReplicaSetIDs()) {

            // Step 2.1 normal case
            ReplicaSet rs = getCoordinator().getStreamMetadataStore().getReplicaSet(replicaSetID);
            for (Node node : rs.getNodes()) {
                try {
                    getCoordinator().notifyReceiverBuildSuccess(node, cubeName, segmentName); // Idempotent
                } catch (IOException e) {
                    // It is OK to just print log, unused segment cache in receiver will be deleted in next call
                    logger.error("error when remove cube segment for receiver:" + node, e);
                }
            }

            // Step 2.2 specical case
            // For the replica set that doesn't have partitions, that should be the "Removed Rs" in latest reassign action.
            // We check if any local segment belong to current cube exists, if nothing left, "Removed Rs" will be removed in assignment from StreamMetadata.
            if (assignments.getPartitionsByReplicaSetID(replicaSetID).isEmpty()) {
                logger.info(
                        "No partition is assign to the replicaSet:{}, check whether there are local segments on the rs.",
                        replicaSetID);
                Node leader = rs.getLeader();
                try {
                    ReceiverCubeStats receiverCubeStats = getCoordinator().getReceiverAdminClient()
                            .getReceiverCubeStats(leader, cubeName);
                    Set<String> segments = receiverCubeStats.getSegmentStatsMap().keySet();
                    if (segments.isEmpty()) {
                        logger.info("no local segments exist for replicaSet:{}, cube:{}, update assignments.",
                                replicaSetID, cubeName);
                        assignments.removeAssignment(replicaSetID);
                        getCoordinator().getStreamMetadataStore().saveNewCubeAssignment(assignments);
                    }
                } catch (IOException e) {
                    logger.error("error when get receiver cube stats from:" + leader, e);
                }
            }
        }

        // Step 3. remove entry in StreamMetadata
        getCoordinator().getStreamMetadataStore().removeSegmentBuildState(cubeName, segmentName);

        // Step 4. delete colmanear segment cache in HDFS becuase it is needless currently
        logger.info("Try to remove the hdfs files for cube:{} segment:{}", cubeName, segmentName);
        removeHDFSFiles(cubeName, segmentName);
        return true;
    }

    /**
     * Promote a segment from realtime part into historical part.
     */
    void promoteNewSegment(CubingJob cubingJob, CubeInstance cubeInstance, CubeSegment cubeSegment) throws IOException {
        logger.debug("Try transfer segment's {} state to ready.", cubeSegment.getName());
        long sourceCount = cubingJob.findSourceRecordCount();
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();
        Map<Integer, String> sourceCheckpoint = getCoordinator().getStreamMetadataStore()
                .getSourceCheckpoint(cubeInstance.getName(), cubeSegment.getName());

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
        ISourcePosition sourcePosition = positionOperator.mergePositions(sourcePositions,
                ISourcePositionHandler.MergeStrategy.KEEP_SMALL);
        cubeSegment.setLastBuildJobID(cubingJob.getId());
        cubeSegment.setLastBuildTime(System.currentTimeMillis());
        cubeSegment.setSizeKB(cubeSizeBytes / 1024);
        cubeSegment.setInputRecords(sourceCount);
        cubeSegment.setInputRecordsSize(sourceSizeBytes);
        cubeSegment.setStreamSourceCheckpoint(positionOperator.serializePosition(sourcePosition));
        getCoordinator().getCubeManager().promoteNewlyBuiltSegments(cubeInstance, cubeSegment);
    }

    /**
     * <pre>
     *  We will promote segment to HBase Ready Segment(historical part) in sequential way. So here we check
     *  if the lastest hbase segment of current cube meet two requirement:
     *     - In ready state
     *     - Connect to current segment exactly (no gap or overlap)
     *  If these two requirement met, we could promote current segment to HBase Ready Segment.
     *  </pre>
     */
    boolean checkPreviousSegmentReady(CubeSegment currSegment) {
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

        if (previousSegmentEnd == -1) {
            return true;
        }

        for (CubeSegment segment : segments) {
            long segmentEnd = segment.getTSRange().end.v;
            if (segmentEnd == previousSegmentEnd && SegmentStatusEnum.READY.equals(segment.getStatus())) {
                return true;
            }
        }
        return false;
    }

    private void removeHDFSFiles(String cubeName, String segmentName) {
        String segmentHDFSPath = HDFSUtil.getStreamingSegmentFilePath(cubeName, segmentName);
        try {
            FileSystem fs = HadoopUtil.getFileSystem(segmentHDFSPath);
            logger.info("Deleting segment data in HDFS {}", segmentHDFSPath);
            fs.delete(new Path(segmentHDFSPath), true);
        } catch (Exception e) {
            logger.error("error when remove hdfs file, hdfs path:{}", segmentHDFSPath);
        }
    }
}
