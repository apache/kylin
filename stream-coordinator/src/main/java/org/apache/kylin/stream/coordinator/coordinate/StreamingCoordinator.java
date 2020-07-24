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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ServerMode;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.coordinator.StreamMetadataStoreFactory;
import org.apache.kylin.stream.coordinator.StreamingCubeInfo;
import org.apache.kylin.stream.coordinator.StreamingUtils;
import org.apache.kylin.stream.coordinator.assign.Assigner;
import org.apache.kylin.stream.coordinator.assign.AssignmentUtil;
import org.apache.kylin.stream.coordinator.assign.AssignmentsCache;
import org.apache.kylin.stream.coordinator.assign.CubePartitionRoundRobinAssigner;
import org.apache.kylin.stream.coordinator.assign.DefaultAssigner;
import org.apache.kylin.stream.coordinator.client.CoordinatorClient;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicAndNotIdempotent;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicIdempotent;
import org.apache.kylin.stream.coordinator.doctor.ClusterDoctor;
import org.apache.kylin.stream.coordinator.exception.CoordinateException;
import org.apache.kylin.stream.coordinator.exception.NotLeadCoordinatorException;
import org.apache.kylin.stream.coordinator.exception.StoreException;
import org.apache.kylin.stream.core.client.HttpReceiverAdminClient;
import org.apache.kylin.stream.core.client.ReceiverAdminClient;
import org.apache.kylin.stream.core.model.AssignRequest;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.ResumeConsumerRequest;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.model.StreamingCubeConsumeState;
import org.apache.kylin.stream.core.model.UnAssignRequest;
import org.apache.kylin.stream.core.source.IStreamingSource;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceFactory;
import org.apache.kylin.stream.core.source.StreamingTableSourceInfo;
import org.apache.kylin.stream.core.util.HDFSUtil;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.apache.kylin.stream.core.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * <pre>
 * Each Kylin streaming cluster has at least one coordinator processes, coordinator
 * server works as the master node of streaming cluster and handle consume task assignment,
 * membership and streaming cube state management.
 *
 * When cluster have several coordinator processes, only the leader try to answer coordinator client's
 * request, other follower will become standby, so single point of failure will be eliminated.
 * </pre>
 */
public class StreamingCoordinator implements CoordinatorClient {
    private static final Logger logger = LoggerFactory.getLogger(StreamingCoordinator.class);
    private static final int DEFAULT_PORT = 7070;
    private static volatile StreamingCoordinator instance = null;

    private StreamMetadataStore streamMetadataStore;
    private Assigner assigner;
    private ReceiverAdminClient receiverAdminClient;
    private CuratorFramework zkClient;
    private CoordinatorLeaderSelector selector;
    private ReceiverClusterManager clusterManager;
    private volatile boolean isLead = false;

    private ScheduledExecutorService streamingJobSubmitExecutor;
    private ScheduledExecutorService clusterStateCheckExecutor;
    private ClusterDoctor clusterDoctor;
    private BuildJobSubmitter buildJobSubmitter;

    private StreamingCoordinator() {
        this.streamMetadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
        this.clusterManager = new ReceiverClusterManager(this);
        this.receiverAdminClient = new HttpReceiverAdminClient();
        this.assigner = getAssigner();
        this.zkClient = StreamingUtils.getZookeeperClient();
        this.selector = new CoordinatorLeaderSelector();
        this.buildJobSubmitter = new BuildJobSubmitter(this);
        this.clusterDoctor = new ClusterDoctor();
        if (ServerMode.SERVER_MODE.canServeStreamingCoordinator()) {
            this.streamingJobSubmitExecutor = Executors.newScheduledThreadPool(1,
                    new NamedThreadFactory("streaming_job_submitter"));
            this.clusterStateCheckExecutor = Executors.newScheduledThreadPool(1,
                    new NamedThreadFactory("cluster_state_checker"));
            start();
        }
    }

    public static StreamingCoordinator getInstance() {
        if (instance == null) {
            synchronized (StreamingCoordinator.class) {
                if (instance == null) {
                    instance = new StreamingCoordinator();
                }
            }
        }
        return instance;
    }

    private void start() {
        selector.start();
        streamingJobSubmitExecutor.scheduleAtFixedRate(buildJobSubmitter, 0, 2, TimeUnit.MINUTES);
        clusterStateCheckExecutor.scheduleAtFixedRate(clusterDoctor, 5, 10, TimeUnit.MINUTES);
    }

    /**
     * Assign the streaming cube to replica sets. Replica sets is calculated by Assigner.
     *
     * @throws CoordinateException when assign action failed
     */
    @Override
    @NotAtomicIdempotent
    public synchronized void assignCube(String cubeName) {
        checkLead();
        streamMetadataStore.addStreamingCube(cubeName);
        StreamingCubeInfo cube = getStreamCubeInfo(cubeName);
        CubeAssignment existAssignment = streamMetadataStore.getAssignmentsByCube(cube.getCubeName());
        if (existAssignment != null) {
            logger.warn("Cube {} is already assigned.", cube.getCubeName());
            return;
        }
        List<ReplicaSet> replicaSets = streamMetadataStore.getReplicaSets();
        if (replicaSets == null || replicaSets.isEmpty()) {
            throw new IllegalStateException("No replicaSet is configured in system");
        }
        CubeAssignment assignment = assigner.assign(cube, replicaSets, streamMetadataStore.getAllCubeAssignments());
        doAssignCube(cubeName, assignment);
    }

    /**
     * Unassign action will remove data which not belong to historical part
     * and stop consumption for all assigned receivers.
     *
     * @throws CoordinateException when unAssign action failed
     */
    @Override
    @NotAtomicIdempotent
    public void unAssignCube(String cubeName) {
        checkLead();
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        if (assignment == null) {
            return;
        }
        List<Node> unAssignedFailReceivers = Lists.newArrayList();
        try {
            logger.info("Send unAssign cube:{} request to receivers", cubeName);
            for (Integer replicaSetID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(replicaSetID);
                UnAssignRequest request = new UnAssignRequest();
                request.setCube(cubeName);
                for (Node receiver : rs.getNodes()) {
                    try {
                        unAssignToReceiver(receiver, request);
                    } catch (IOException e) {
                        logger.error("Exception throws when unAssign receiver", e);
                        unAssignedFailReceivers.add(receiver);
                    }
                }
            }
            logger.debug("Remove temp hdfs files for {}", cubeName);
            removeCubeHDFSFiles(cubeName);
            logger.debug("Clear cube info from job check list");
            buildJobSubmitter.clearCheckList(cubeName);
            logger.debug("Commit unassign {} transaction.", cubeName);
            streamMetadataStore.removeStreamingCube(cubeName);
            AssignmentsCache.getInstance().clearCubeCache(cubeName);
        } catch (Exception e) {
            throw new CoordinateException(e);
        }
        if (!unAssignedFailReceivers.isEmpty()) {
            String msg = "unAssign fail for receivers:" + String.join(",",
                    unAssignedFailReceivers.stream().map(Node::toString).collect(Collectors.toList()));
            throw new CoordinateException(msg);
        }
    }

    /**
     * change assignment of cubeName to assignments
     */
    @Override
    @NotAtomicAndNotIdempotent
    public synchronized void reAssignCube(String cubeName, CubeAssignment assignments) {
        checkLead();
        CubeAssignment preAssignments = streamMetadataStore.getAssignmentsByCube(cubeName);
        if (preAssignments == null) {
            logger.info("no previous cube assign exists, use the new assignment:{}", assignments);
            doAssignCube(cubeName, assignments);
        } else {
            clusterManager.reassignCubeImpl(cubeName, preAssignments, assignments);
        }
    }

    @Override
    public void segmentRemoteStoreComplete(Node receiver, String cubeName, Pair<Long, Long> segment) {
        checkLead();
        logger.info("Segment remote store complete signal received for cube:{}, segment:{}, by {}.", cubeName, segment,
                receiver);
        buildJobSubmitter.addToCheckList(cubeName);
    }

    @Override
    @NotAtomicIdempotent
    public void pauseConsumers(String cubeName) {
        checkLead();
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        PauseConsumersRequest request = new PauseConsumersRequest();
        request.setCube(cubeName);
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                clusterManager.pauseConsumersInReplicaSet(rs, request);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        logger.debug("Committing pauseConsumers {} transaction.", cubeName);
        streamMetadataStore.saveStreamingCubeConsumeState(cubeName, StreamingCubeConsumeState.PAUSED);
        logger.debug("Committed pauseConsumers {} transaction.", cubeName);

    }

    @Override
    @NotAtomicIdempotent
    public void resumeConsumers(String cubeName) {
        checkLead();
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        ResumeConsumerRequest request = new ResumeConsumerRequest();
        request.setCube(cubeName);
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                clusterManager.resumeConsumersInReplicaSet(rs, request);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debug("Committing resumeConsumers {} transaction.", cubeName);
        streamMetadataStore.saveStreamingCubeConsumeState(cubeName, StreamingCubeConsumeState.RUNNING);
        logger.debug("Committed resumeConsumers {} transaction.", cubeName);
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

    @NotAtomicIdempotent
    private void doAssignCube(String cubeName, CubeAssignment assignment) {
        Set<ReplicaSet> successRS = Sets.newHashSet();
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                clusterManager.assignCubeToReplicaSet(rs, cubeName, assignment.getPartitionsByReplicaSetID(rsID), true,
                        false);
                successRS.add(rs);
            }
            logger.debug("Committing assignment {} transaction.", cubeName);
            streamMetadataStore.saveNewCubeAssignment(assignment);
            logger.debug("Committed assignment {} transaction.", cubeName);
        } catch (Exception e) {
            logger.debug("Starting roll back success receivers.");
            for (ReplicaSet rs : successRS) {

                UnAssignRequest request = new UnAssignRequest();
                request.setCube(cubeName);
                unAssignFromReplicaSet(rs, request);
            }
            throw new CoordinateException(e);
        }
    }

    @Override
    public Map<Integer, Map<String, List<Partition>>> reBalanceRecommend() {
        checkLead();
        return reBalancePlan(getEnableStreamingCubes(), streamMetadataStore.getReplicaSets());
    }

    /**
     * reBalance the cube and partitions
     * @param newAssignmentsPlan Map<ReplicaSetID, Map<CubeName, List<Partition>>
     */
    @Override
    @NotAtomicAndNotIdempotent
    public synchronized void reBalance(Map<Integer, Map<String, List<Partition>>> newAssignmentsPlan) {
        checkLead();
        List<CubeAssignment> currCubeAssignments = streamMetadataStore.getAllCubeAssignments();
        List<CubeAssignment> newCubeAssignments = AssignmentUtil.convertReplicaSetAssign2CubeAssign(newAssignmentsPlan);
        clusterManager.doReBalance(currCubeAssignments, newCubeAssignments);
    }

    Map<Integer, Map<String, List<Partition>>> reBalancePlan(List<StreamingCubeInfo> allCubes,
            List<ReplicaSet> allReplicaSets) {
        List<CubeAssignment> currCubeAssignments = streamMetadataStore.getAllCubeAssignments();
        return assigner.reBalancePlan(allReplicaSets, allCubes, currCubeAssignments);
    }

    public synchronized void createReplicaSet(ReplicaSet rs) {
        int replicaSetID = streamMetadataStore.createReplicaSet(rs);
        try {
            for (Node receiver : rs.getNodes()) {
                logger.trace("Notify {} that it has been added to {} .", receiver, replicaSetID);
                addReceiverToReplicaSet(receiver, replicaSetID);
            }
        } catch (IOException e) {
            logger.warn("Create replica set failed.", e);
        }
    }

    public synchronized void removeReplicaSet(int rsID) {
        ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
        if (rs == null) {
            return;
        }
        if (rs.getNodes() != null && !rs.getNodes().isEmpty()) {
            throw new CoordinateException("Cannot remove rs, because there are nodes in it.");
        }
        Map<String, List<Partition>> assignment = streamMetadataStore.getAssignmentsByReplicaSet(rsID);
        if (assignment != null && !assignment.isEmpty()) {
            throw new CoordinateException("Cannot remove rs, because there are assignments.");
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
                    logger.error("Error add Node {} to replicaSet {}, already exist in replicaSet {} ", nodeID,
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
            logger.warn("Remove node from replicaSet failed.", e);
        }
    }

    @SuppressWarnings("unused")
    public void stopConsumers(String cubeName) {
        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cubeName);
        StopConsumersRequest request = new StopConsumersRequest();
        request.setCube(cubeName);
        try {
            for (Integer rsID : assignment.getReplicaSetIDs()) {
                ReplicaSet rs = streamMetadataStore.getReplicaSet(rsID);
                clusterManager.stopConsumersInReplicaSet(rs, request);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void unAssignFromReplicaSet(final ReplicaSet rs, final UnAssignRequest unAssignRequest) {
        for (Node receiver : rs.getNodes()) {
            try {
                unAssignToReceiver(receiver, unAssignRequest);
            } catch (IOException e) {
                logger.error("Error when roll back assignment", e);
            }
        }
    }

    // ================================================================================
    // ========================== Receiver related operation ==========================

    protected void assignToReceiver(final Node receiver, final AssignRequest request) throws IOException {
        receiverAdminClient.assign(receiver, request);
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

    protected void startConsumersForReceiver(final Node receiver, final StartConsumersRequest request)
            throws IOException {
        receiverAdminClient.startConsumers(receiver, request);
    }

    protected ConsumerStatsResponse stopConsumersForReceiver(final Node receiver, final StopConsumersRequest request)
            throws IOException {
        return receiverAdminClient.stopConsumers(receiver, request);
    }

    protected ConsumerStatsResponse pauseConsumersForReceiver(final Node receiver, final PauseConsumersRequest request)
            throws IOException {
        return receiverAdminClient.pauseConsumers(receiver, request);
    }

    protected ConsumerStatsResponse resumeConsumersForReceiver(final Node receiver, final ResumeConsumerRequest request)
            throws IOException {
        return receiverAdminClient.resumeConsumers(receiver, request);
    }

    protected void makeCubeImmutableForReceiver(final Node receiver, final String cubeName) throws IOException {
        receiverAdminClient.makeCubeImmutable(receiver, cubeName);
    }

    void notifyReceiverBuildSuccess(final Node receiver, final String cubeName, final String segmentName)
            throws IOException {
        receiverAdminClient.segmentBuildComplete(receiver, cubeName, segmentName);
    }

    // ================================================================================
    // ============================ Utility method ====================================

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    List<StreamingCubeInfo> getEnableStreamingCubes() {
        List<StreamingCubeInfo> allCubes = getStreamingCubes();
        List<StreamingCubeInfo> result = Lists.newArrayList();
        for (StreamingCubeInfo cube : allCubes) {
            CubeInstance cubeInstance = getCubeManager().getCube(cube.getCubeName());
            if (cubeInstance.getStatus() == RealizationStatusEnum.READY) {
                result.add(cube);
            }
        }
        return result;
    }

    List<StreamingCubeInfo> getStreamingCubes() {
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

    public StreamingCubeInfo getStreamCubeInfo(String cubeName) {
        CubeInstance cube = getCubeManager().getCube(cubeName);
        if (cube == null) {
            return null;
        }
        // count of consumers should be estimated by kylin admin and set in cube level
        int numOfConsumerTasks = cube.getConfig().getStreamingCubeConsumerTasksNum();
        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cube);
        StreamingTableSourceInfo tableSourceInfo = streamingSource.load(cubeName);
        return new StreamingCubeInfo(cubeName, tableSourceInfo, numOfConsumerTasks);
    }

    public void removeCubeHDFSFiles(String cubeName) {
        String segmentHDFSPath = HDFSUtil.getStreamingCubeFilePath(cubeName);
        try {
            FileSystem fs = HadoopUtil.getFileSystem(segmentHDFSPath);
            fs.delete(new Path(segmentHDFSPath), true);
        } catch (Exception e) {
            logger.error("Error when remove hdfs file, hdfs path:{}", segmentHDFSPath);
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

    public ReceiverClusterManager getClusterManager() {
        return clusterManager;
    }

    public boolean isLead() {
        return isLead;
    }

    public StreamMetadataStore getStreamMetadataStore() {
        return streamMetadataStore;
    }

    public ReceiverAdminClient getReceiverAdminClient() {
        return receiverAdminClient;
    }

    private class CoordinatorLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
        private LeaderSelector leaderSelector;

        public CoordinatorLeaderSelector() {
            String path = StreamingUtils.COORDINATOR_LEAD;
            leaderSelector = new LeaderSelector(zkClient, path, this);
            leaderSelector.autoRequeue();
        }

        @Override
        public void close() {
            leaderSelector.close();
        }

        public void start() {
            leaderSelector.start();
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            logger.info("Current node become the lead coordinator.");
            streamMetadataStore.setCoordinatorNode(NodeUtil.getCurrentNode(DEFAULT_PORT));
            isLead = true;
            // check job status every minute
            buildJobSubmitter.restore();
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
            logger.info("Become the follower coordinator.");
            isLead = false;
        }
    }
}
