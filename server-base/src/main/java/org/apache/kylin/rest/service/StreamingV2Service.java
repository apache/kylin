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

package org.apache.kylin.rest.service;

/**
 * StreamingCoordinatorService will try to forward request to corrdinator leader by HttpClient.
 */
public class StreamingV2Service extends BasicService {
//    private static final Logger logger = LoggerFactory.getLogger(StreamingV2Service.class);
//    private static final String CLUSTER_STATE = "cluster_state";
//
//    private StreamMetadataStore streamMetadataStore;
//
//    private ReceiverAdminClient receiverAdminClient;
//
//    private Cache<String, ClusterState> clusterStateCache = CacheBuilder.newBuilder()
//            .expireAfterWrite(10, TimeUnit.SECONDS).build();
//
//    private ExecutorService clusterStateExecutor = new ThreadPoolExecutor(0, 20, 60L, TimeUnit.SECONDS,
//            new LinkedBlockingQueue<>(), new NamedThreadFactory("fetch_receiver_state"));
//
//    public StreamingV2Service() {
//        streamMetadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
//        receiverAdminClient = new HttpReceiverAdminClient();
//    }
//
//    public List<StreamingSourceConfig> listAllStreamingConfigs(final String table) throws IOException {
//        List<StreamingSourceConfig> streamingSourceConfigs = Lists.newArrayList();
//        if (StringUtils.isEmpty(table)) {
//            streamingSourceConfigs = getStreamingManagerV2().listAllStreaming();
//        } else {
//            StreamingSourceConfig config = getStreamingManagerV2().getConfig(table);
//            if (config != null) {
//                streamingSourceConfigs.add(config);
//            }
//        }
//
//        return streamingSourceConfigs;
//    }
//
//    public List<StreamingSourceConfig> getStreamingConfigs(final String table, final Integer limit, final Integer offset)
//            throws IOException {
//        List<StreamingSourceConfig> streamingSourceConfigs;
//        streamingSourceConfigs = listAllStreamingConfigs(table);
//
//        if (limit == null || offset == null) {
//            return streamingSourceConfigs;
//        }
//
//        if ((streamingSourceConfigs.size() - offset) < limit) {
//            return streamingSourceConfigs.subList(offset, streamingSourceConfigs.size());
//        }
//
//        return streamingSourceConfigs.subList(offset, offset + limit);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#project, 'ADMINISTRATION')")
//    public StreamingSourceConfig createStreamingConfig(StreamingSourceConfig config, ProjectInstance project) throws IOException {
//        if (getStreamingManagerV2().getConfig(config.getName()) != null) {
//            throw new InternalErrorException("The streamingSourceConfig named " + config.getName() + " already exists");
//        }
//        StreamingSourceConfig streamingSourceConfig = getStreamingManagerV2().saveStreamingConfig(config);
//        return streamingSourceConfig;
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public StreamingSourceConfig updateStreamingConfig(StreamingSourceConfig config) throws IOException {
//        return getStreamingManagerV2().updateStreamingConfig(config);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void dropStreamingConfig(StreamingSourceConfig config) throws IOException {
//        getStreamingManagerV2().removeStreamingConfig(config);
//    }
//
//    public String getParserTemplate(final int sourceType, StreamingSourceConfig config) {
//        IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(new ISourceAware() {
//            @Override
//            public int getSourceType() {
//                return sourceType;
//            }
//
//            @Override
//            public KylinConfig getConfig() {
//                return getConfig();
//            }
//        });
//        return streamingSource.getMessageTemplate(config);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
//    public List<CubeAssignment> getStreamingCubeAssignments(final CubeInstance cube) {
//        if (cube == null) {
//            return streamMetadataStore.getAllCubeAssignments();
//        }
//        List<CubeAssignment> result = Lists.newArrayList();
//        CubeAssignment assignment = streamMetadataStore.getAssignmentsByCube(cube.getName());
//        if (assignment != null) {
//            result.add(assignment);
//        }
//        return result;
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public Map<Integer, Map<String, List<Partition>>> getStreamingReplicaSetAssignments(Integer replicaSetID) {
//        if (replicaSetID == null) {
//            return streamMetadataStore.getAllReplicaSetAssignments();
//        }
//        Map<Integer, Map<String, List<Partition>>> result = Maps.newHashMap();
//        Map<String, List<Partition>> assignment = streamMetadataStore.getAssignmentsByReplicaSet(replicaSetID);
//        if (assignment != null) {
//            result.put(replicaSetID, assignment);
//        }
//        return result;
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public Map<Integer, Map<String, List<Partition>>> reBalancePlan() {
//        return getCoordinatorClient().reBalanceRecommend();
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void reBalance(Map<Integer, Map<String, List<Partition>>> reBalancePlan) {
//        getCoordinatorClient().reBalance(reBalancePlan);
//    }
//
//    public List<String> getStreamingCubes() {
//        return streamMetadataStore.getCubes();
//    }
//
//    public StreamingCubeConsumeState getStreamingCubeConsumeState(String cubeName) {
//        return streamMetadataStore.getStreamingCubeConsumeState(cubeName);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
//    public void assignCube(CubeInstance cube) {
//        getCoordinatorClient().assignCube(cube.getName());
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
//    public void unAssignCube(CubeInstance cube) {
//        getCoordinatorClient().unAssignCube(cube.getName());
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void reAssignCube(String cubeName, CubeAssignment newAssignment) {
//        validateAssignment(newAssignment);
//        getCoordinatorClient().reAssignCube(cubeName, newAssignment);
//    }
//
//    private void validateAssignment(CubeAssignment newAssignment) {
//        Map<Integer, List<Partition>> assignments = newAssignment.getAssignments();
//        Set<Integer> inputReplicaSetIDs = assignments.keySet();
//        Set<Integer> allReplicaSetIDs = Sets.newHashSet(streamMetadataStore.getReplicaSetIDs());
//        for (Integer inputReplicaSetID : inputReplicaSetIDs) {
//            if (!allReplicaSetIDs.contains(inputReplicaSetID)) {
//                throw new IllegalArgumentException("the replica set id:" + inputReplicaSetID + " does not exist");
//            }
//        }
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
//    public void pauseConsumers(CubeInstance cube) {
//        getCoordinatorClient().pauseConsumers(cube.getName());
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
//    public void resumeConsumers(CubeInstance cube) {
//        getCoordinatorClient().resumeConsumers(cube.getName());
//    }
//
//    public StreamingSourceConfigManager getStreamingManagerV2() {
//        return StreamingSourceConfigManager.getInstance(getConfig());
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void removeCubeAssignment() {
//
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public List<Node> getReceivers() {
//        List<Node> result = streamMetadataStore.getReceivers();
//        return result;
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void removeReceiver(Node receiver) {
//        List<ReplicaSet> replicaSets = streamMetadataStore.getReplicaSets();
//        for (ReplicaSet replicaSet : replicaSets) {
//            Set<Node> receivers = replicaSet.getNodes();
//            if (receivers != null && receivers.contains(receiver)) {
//                throw new IllegalStateException("Before remove receiver, it must be firstly removed from replica set:"
//                        + replicaSet.getReplicaSetID());
//            }
//        }
//        streamMetadataStore.removeReceiver(receiver);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void createReplicaSet(ReplicaSet rs) {
//        getCoordinatorClient().createReplicaSet(rs);
//        clusterStateCache.invalidate(CLUSTER_STATE);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void removeReplicaSet(int rsID) {
//        getCoordinatorClient().removeReplicaSet(rsID);
//        clusterStateCache.invalidate(CLUSTER_STATE);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void addNodeToReplicaSet(Integer replicaSetID, String nodeID) {
//        getCoordinatorClient().addNodeToReplicaSet(replicaSetID, nodeID);
//        clusterStateCache.invalidate(CLUSTER_STATE);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public void removeNodeFromReplicaSet(Integer replicaSetID, String nodeID) {
//        getCoordinatorClient().removeNodeFromReplicaSet(replicaSetID, nodeID);
//        clusterStateCache.invalidate(CLUSTER_STATE);
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
//    public List<ReplicaSet> getReplicaSets() {
//        List<ReplicaSet> result = streamMetadataStore.getReplicaSets();
//        return result;
//    }
//
//    public ReceiverStats getReceiverStats(Node receiver) {
//        try {
//            return receiverAdminClient.getReceiverStats(receiver);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public ReceiverCubeStats getReceiverCubeStats(Node receiver, String cubeName) {
//        try {
//            return receiverAdminClient.getReceiverCubeStats(receiver, cubeName);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
//            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT') or hasPermission(#cube.getProjectInstance(), 'MANAGEMENT') or hasPermission(#cube.getProjectInstance(), 'ADMINISTRATION')")
//    public CubeRealTimeState getCubeRealTimeState(CubeInstance cube) {
//        CubeRealTimeState result = new CubeRealTimeState();
//        result.setCubeName(cube.getName());
//        CubeAssignment cubeAssignment = streamMetadataStore.getAssignmentsByCube(cube.getName());
//        Map<Integer, Map<Node, ReceiverCubeRealTimeState>> rsReceiverCubeStateMap = Maps.newHashMap();
//        for (Integer replicaSetID : cubeAssignment.getReplicaSetIDs()) {
//            ReplicaSet replicaSet = streamMetadataStore.getReplicaSet(replicaSetID);
//            Map<Node, ReceiverCubeRealTimeState> receiverCubeStateMap = Maps.newHashMap();
//            Set<Node> receivers = replicaSet.getNodes();
//            for (Node receiver : receivers) {
//                ReceiverCubeRealTimeState receiverCubeRealTimeState = new ReceiverCubeRealTimeState();
//                try {
//                    ReceiverCubeStats receiverCubeStats = receiverAdminClient.getReceiverCubeStats(receiver,
//                            cube.getName());
//                    receiverCubeRealTimeState.setState(ReceiverState.State.HEALTHY);
//                    receiverCubeRealTimeState.setReceiverCubeStats(receiverCubeStats);
//                } catch (IOException e) {
//                    logger.error("exception when get receiver cube stats", e);
//                    if (!isReceiverReachable(receiver)) {
//                        receiverCubeRealTimeState.setState(ReceiverState.State.UNREACHABLE);
//                    } else {
//                        receiverCubeRealTimeState.setState(ReceiverState.State.DOWN);
//                    }
//                }
//                receiverCubeStateMap.put(receiver, receiverCubeRealTimeState);
//            }
//            rsReceiverCubeStateMap.put(replicaSetID, receiverCubeStateMap);
//        }
//        result.setReceiverCubeStateMap(rsReceiverCubeStateMap);
//
//        return result;
//    }
//
//    /**
//     * Fetch and calculate total cluster state.
//     * @return
//     */
//    public ClusterState getClusterState() {
//        ClusterState clusterState = clusterStateCache.getIfPresent(CLUSTER_STATE);
//        if (clusterState != null) {
//            return clusterState;
//        }
//        List<ReplicaSet> replicaSets = streamMetadataStore.getReplicaSets();
//        List<Node> allReceivers = streamMetadataStore.getReceivers();
//        Map<Integer, Map<String, List<Partition>>> rsAssignments = streamMetadataStore.getAllReplicaSetAssignments();
//
//        Map<Node, Future<ReceiverStats>> statsFuturesMap = Maps.newHashMap();
//        for (final Node receiver : allReceivers) {
//            Future<ReceiverStats> receiverStatsFuture = clusterStateExecutor.submit(() -> receiverAdminClient.getReceiverStats(receiver));
//            statsFuturesMap.put(receiver, receiverStatsFuture);
//        }
//
//        clusterState = new ClusterState();
//        for (ReplicaSet replicaSet : replicaSets) {
//            ReplicaSetState replicaSetState = calReplicaSetState(replicaSet,
//                    rsAssignments.get(replicaSet.getReplicaSetID()), statsFuturesMap);
//            clusterState.addReplicaSetState(replicaSetState);
//            allReceivers.removeAll(replicaSet.getNodes());
//        }
//
//        // left receivers are not assigned receivers
//        for (Node receiver : allReceivers) {
//            Future<ReceiverStats> futureStats = statsFuturesMap.get(receiver);
//            ReceiverState receiverState = getReceiverStateFromStats(receiver, futureStats);
//            clusterState.addAvailableReveiverState(receiverState);
//        }
//        clusterState.setLastUpdateTime(System.currentTimeMillis());
//        clusterStateCache.put("cluster_state", clusterState);
//        return clusterState;
//    }
//
//    private ReplicaSetState calReplicaSetState(ReplicaSet replicaSet, Map<String, List<Partition>> rsAssignment,
//            Map<Node, Future<ReceiverStats>> statsFuturesMap) {
//        ReplicaSetState replicaSetState = new ReplicaSetState();
//        replicaSetState.setRsID(replicaSet.getReplicaSetID());
//        replicaSetState.setAssignment(rsAssignment);
//        Set<Node> receivers = replicaSet.getNodes();
//        if (receivers == null || receivers.isEmpty()) {
//            return replicaSetState;
//        }
//
//        Node leadReceiver = replicaSet.getLeader();
//        replicaSetState.setLead(leadReceiver);
//
//        Map<Node, ReceiverStats> receiverStatsMap = Maps.newHashMap();
//        for (Node receiver : receivers) {
//            Future<ReceiverStats> futureStats = statsFuturesMap.get(receiver);
//            try {
//                ReceiverStats receiverStats = futureStats.get();
//                receiverStatsMap.put(receiver, receiverStats);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            } catch (ExecutionException e) {
//                replicaSetState.addReveiverState(getReceiverStateFromException(receiver, e));
//                continue;
//            }
//        }
//
//        Map<String, Long> cubeLatestEventMap = Maps.newHashMap();
//        for (ReceiverStats receiverStats : receiverStatsMap.values()) {
//            Map<String, ReceiverCubeStats> cubeStatsMap = receiverStats.getCubeStatsMap();
//            for (Map.Entry<String, ReceiverCubeStats> cubeStatsEntry : cubeStatsMap.entrySet()) {
//                String cubeName = cubeStatsEntry.getKey();
//                ReceiverCubeStats cubeStats = cubeStatsEntry.getValue();
//                Long latestEventTime = cubeLatestEventMap.get(cubeName);
//                if (latestEventTime == null || latestEventTime < cubeStats.getLatestEventTime()) {
//                    cubeLatestEventMap.put(cubeName, cubeStats.getLatestEventTime());
//                }
//            }
//        }
//        long consumeEventLagThreshold = 5 * 60 * 1000L; // default lag warning threshold is 5 minutes
//        for (Map.Entry<Node, ReceiverStats> receiverStatsEntry : receiverStatsMap.entrySet()) {
//            Node receiver = receiverStatsEntry.getKey();
//            ReceiverStats receiverStats = receiverStatsEntry.getValue();
//            ReceiverState receiverState = new ReceiverState();
//            receiverState.setReceiver(receiver);
//            receiverState.setState(ReceiverState.State.HEALTHY);
//            Map<String, List<Partition>> receiverAssignment = receiverStats.getAssignments();
//            if (!assignmentEqual(receiverAssignment, rsAssignment)) {
//                ReceiverState.State state = ReceiverState.State.WARN;
//                receiverState.setState(state);
//                receiverState.addInfo("assignment is inconsistent");
//            }
//
//            if (receiverStats.isLead() && !receiver.equals(leadReceiver)) {
//                ReceiverState.State state = ReceiverState.State.WARN;
//                receiverState.setState(state);
//                receiverState.addInfo("lead state is inconsistent");
//            }
//
//            Map<String, ReceiverCubeStats> cubeStatsMap = receiverStats.getCubeStatsMap();
//            for (Map.Entry<String, ReceiverCubeStats> cubeStatsEntry : cubeStatsMap.entrySet()) {
//                String cubeName = cubeStatsEntry.getKey();
//                ReceiverCubeStats cubeStats = cubeStatsEntry.getValue();
//                Long latestEventTime = cubeLatestEventMap.get(cubeName);
//                if ((latestEventTime - cubeStats.getLatestEventTime()) >= consumeEventLagThreshold) {
//                    ReceiverState.State state = ReceiverState.State.WARN;
//                    receiverState.setState(state);
//                    receiverState.addInfo("cube:" + cubeName + " consuming is lagged");
//                }
//            }
//            receiverState.setRateInOneMin(calConsumeRate(receiver, receiverStats));
//            replicaSetState.addReveiverState(receiverState);
//        }
//        return replicaSetState;
//    }
//
//    private boolean assignmentEqual(Map<String, List<Partition>> receiverAssignment,
//            Map<String, List<Partition>> rsAssignment) {
//        if (emptyMap(receiverAssignment) && emptyMap(rsAssignment)) {
//            return true;
//        }
//
//        if (receiverAssignment != null) {
//            for (Map.Entry<String, List<Partition>> entry : receiverAssignment.entrySet()) {
//                Collections.sort(entry.getValue());
//                entry.setValue(entry.getValue());
//            }
//        }
//
//        if (rsAssignment != null) {
//            for (Map.Entry<String, List<Partition>> entry : rsAssignment.entrySet()) {
//                Collections.sort(entry.getValue());
//                entry.setValue(entry.getValue());
//            }
//        }
//
//        if (receiverAssignment != null && receiverAssignment.equals(rsAssignment)) {
//            return true;
//        }
//        return false;
//    }
//
//    private boolean emptyMap(Map map) {
//        if (map == null || map.isEmpty()) {
//            return true;
//        }
//        return false;
//    }
//
//    private ReceiverState getReceiverStateFromException(Node receiver, ExecutionException e) {
//        ReceiverState receiverState = new ReceiverState();
//        receiverState.setReceiver(receiver);
//        if (!isReceiverReachable(receiver)) {
//            receiverState.setState(ReceiverState.State.UNREACHABLE);
//        } else {
//            receiverState.setState(ReceiverState.State.DOWN);
//        }
//        return receiverState;
//    }
//
//    private boolean isReceiverReachable(Node receiver) {
//        try {
//            InetAddress address = InetAddress.getByName(receiver.getHost());//ping this IP
//            boolean reachable = address.isReachable(1000);
//            if (!reachable) {
//                return false;
//            }
//            return true;
//        } catch (Exception exception) {
//            logger.error("exception when try ping host:" + receiver.getHost(), exception);
//            return false;
//        }
//    }
//
//    private ReceiverState getReceiverStateFromStats(Node receiver, Future<ReceiverStats> futureStats) {
//        ReceiverState receiverState = new ReceiverState();
//        try {
//            ReceiverStats receiverStats = futureStats.get();
//            receiverState.setReceiver(receiver);
//            receiverState.setState(ReceiverState.State.HEALTHY);
//            receiverState.setRateInOneMin(calConsumeRate(receiver, receiverStats));
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } catch (ExecutionException e) {
//            receiverState = getReceiverStateFromException(receiver, e);
//        }
//        return receiverState;
//    }
//
//    private double calConsumeRate(Node receiver, ReceiverStats receiverStats) {
//        double result = 0;
//        Map<String, ReceiverCubeStats> cubeStatsMap = receiverStats.getCubeStatsMap();
//        for (Map.Entry<String, ReceiverCubeStats> receiverCubeStatsEntry : cubeStatsMap.entrySet()) {
//            ReceiverCubeStats cubeStats = receiverCubeStatsEntry.getValue();
//            ConsumerStats consumerStats = cubeStats.getConsumerStats();
//            if (consumerStats == null) {
//                logger.warn("no consumer stats exist for cube:{} in receiver:{}", receiverCubeStatsEntry.getKey(),
//                        receiver);
//                continue;
//            }
//            Map<Integer, PartitionConsumeStats> partitionConsumeStatsMap = consumerStats.getPartitionConsumeStatsMap();
//            for (PartitionConsumeStats partitionStats : partitionConsumeStatsMap.values()) {
//                result += partitionStats.getOneMinRate();
//            }
//        }
//        return result;
//    }
//
//    //    private Node.NodeStatus getReceiverStatus(Node receiver) {
//    //        try {
//    //            HealthCheckInfo healthCheckInfo = receiverAdminClient.healthCheck(receiver);
//    //            if (healthCheckInfo.getStatus() == HealthCheckInfo.Status.GOOD) {
//    //                return Node.NodeStatus.HEALTHY;
//    //            } else {
//    //                return Node.NodeStatus.STOPPED;
//    //            }
//    //        } catch (IOException e) {
//    //            return Node.NodeStatus.STOPPED;
//    //        }
//    //    }
//
//    private synchronized CoordinatorClient getCoordinatorClient() {
//        return CoordinatorClientFactory.createCoordinatorClient(streamMetadataStore);
//    }
}
