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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.stream.coordinator.assign.AssignmentUtil;
import org.apache.kylin.stream.coordinator.exception.StoreException;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.SegmentBuildState;
import org.apache.kylin.stream.core.model.StreamingCubeConsumeState;
import org.apache.kylin.stream.core.source.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class ZookeeperStreamMetadataStore implements StreamMetadataStore {
    public static final String REPLICA_SET_ROOT = "/replica_sets";
    public static final String RECEIVER_ROOT = "/receivers";
    public static final String CUBE_ROOT = "/cubes";
    public static final String COORDINATOR_NODE = "/coordinator";
    public static final String CUBE_BUILD_STATE = "build_state";
    public static final String CUBE_CONSUME_STATE = "consume_state";
    public static final String CUBE_ASSIGNMENT = "assignment";
    public static final String CUBE_CONSUME_SRC_STATE = "consume_source_state";
    public static final String CUBE_SRC_CHECKPOINT = "source_checkpoint";
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperStreamMetadataStore.class);
    private CuratorFramework client;
    private String zkRoot;
    private String replicaSetRoot;
    private String receiverRoot;
    private String cubeRoot;
    private String coordinatorRoot;

    private AtomicLong readSuccess = new AtomicLong();
    private AtomicLong readFail = new AtomicLong();
    private AtomicLong writeSuccess = new AtomicLong();
    private AtomicLong writeFail = new AtomicLong();

    public ZookeeperStreamMetadataStore() {
        this.client = StreamingUtils.getZookeeperClient();
        this.zkRoot = StreamingUtils.STREAM_ZK_ROOT;
        init();
    }

    private void init() {
        try {
            replicaSetRoot = zkRoot + REPLICA_SET_ROOT;
            receiverRoot = zkRoot + RECEIVER_ROOT;
            cubeRoot = zkRoot + CUBE_ROOT;
            coordinatorRoot = zkRoot + COORDINATOR_NODE;

            createZKNodeIfNotExist(zkRoot);
            createZKNodeIfNotExist(replicaSetRoot);
            createZKNodeIfNotExist(receiverRoot);
            createZKNodeIfNotExist(cubeRoot);
            createZKNodeIfNotExist(coordinatorRoot);
        } catch (Exception e) {
            logger.error("error when create zk nodes", e);
            throw new StoreException(e);
        }
    }

    private void createZKNodeIfNotExist(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
    }

    @Override
    public void removeCubeAssignment(String cubeName) {
        logger.trace("Remove cube assignment {}.", cubeName);
        checkPath(cubeName);
        try {
            client.delete().forPath(ZKPaths.makePath(cubeRoot, cubeName, CUBE_ASSIGNMENT));
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when remove cube assignment " + cubeName, e);
            throw new StoreException(e);
        }
    }

    @Override
    public List<CubeAssignment> getAllCubeAssignments() {
        try {
            List<CubeAssignment> cubeAssignmentList = Lists.newArrayList();
            List<String> cubes = client.getChildren().forPath(cubeRoot);
            for (String cube : cubes) {
                String cubeAssignmentPath = getCubeAssignmentPath(cube);
                if (client.checkExists().forPath(cubeAssignmentPath) != null) {
                    byte[] data = client.getData().forPath(cubeAssignmentPath);
                    CubeAssignment assignment = CubeAssignment.deserializeCubeAssignment(data);
                    cubeAssignmentList.add(assignment);
                }
            }
            readSuccess.getAndIncrement();
            return cubeAssignmentList;
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get assignments", e);
            throw new StoreException(e);
        }
    }

    @Override
    public Map<Integer, Map<String, List<Partition>>> getAllReplicaSetAssignments() {
        try {
            List<CubeAssignment> cubeAssignmentList = getAllCubeAssignments();
            readSuccess.getAndIncrement();
            return AssignmentUtil.convertCubeAssign2ReplicaSetAssign(cubeAssignmentList);
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get assignments", e);
            throw new StoreException(e);
        }
    }

    @Override
    public Map<String, List<Partition>> getAssignmentsByReplicaSet(int replicaSetID) {
        try {
            Map<Integer, Map<String, List<Partition>>> replicaSetAssignmentsMap = getAllReplicaSetAssignments();
            readSuccess.getAndIncrement();
            return replicaSetAssignmentsMap.get(replicaSetID);
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get assignment for replica set " + replicaSetID, e);
            throw new StoreException(e);
        }
    }

    @Override
    public CubeAssignment getAssignmentsByCube(String cubeName) {
        try {
            String cubeAssignmentPath = getCubeAssignmentPath(cubeName);
            if (client.checkExists().forPath(cubeAssignmentPath) == null) {
                logger.warn("Cannot find content at {}.", cubeAssignmentPath);
                return null;
            }
            byte[] data = client.getData().forPath(cubeAssignmentPath);
            readSuccess.getAndIncrement();
            CubeAssignment assignment = CubeAssignment.deserializeCubeAssignment(data);
            return assignment;
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get cube assignment for " + cubeName, e);
            throw new StoreException(e);
        }
    }

    @Override
    public List<ReplicaSet> getReplicaSets() {
        List<ReplicaSet> result = Lists.newArrayList();
        try {
            List<String> replicaSetIDs = client.getChildren().forPath(replicaSetRoot);
            readSuccess.getAndIncrement();
            for (String replicaSetID : replicaSetIDs) {
                ReplicaSet replicaSet = getReplicaSet(Integer.parseInt(replicaSetID));
                result.add(replicaSet);
            }
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get replica sets", e);
            throw new StoreException(e);
        }
        return result;
    }

    @Override
    public List<Integer> getReplicaSetIDs() {
        try {
            List<String> replicaSetIDs = client.getChildren().forPath(replicaSetRoot);
            readSuccess.getAndIncrement();
            return Lists.transform(replicaSetIDs, new Function<String, Integer>() {
                @Nullable
                @Override
                public Integer apply(@Nullable String input) {
                    return Integer.valueOf(input);
                }
            });
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get replica sets", e);
            throw new StoreException(e);
        }
    }

    @Override
    public int createReplicaSet(ReplicaSet rs) {
        try {
            List<String> rsList = client.getChildren().forPath(replicaSetRoot);
            List<Integer> rsIDList = Lists.transform(rsList, new Function<String, Integer>() {
                @Nullable
                @Override
                public Integer apply(@Nullable String input) {
                    Integer result;
                    try {
                        result = Integer.valueOf(input);
                    } catch (Exception e) {
                        result = 0;
                    }
                    return result;
                }
            });
            int currMaxID = -1;
            if (rsIDList != null && !rsIDList.isEmpty()) {
                currMaxID = Collections.max(rsIDList);
            }
            int newReplicaSetID = currMaxID + 1;
            logger.trace("Id of new replica set {} is {}.", rs, newReplicaSetID);
            rs.setReplicaSetID(newReplicaSetID);
            String replicaSetPath = ZKPaths.makePath(replicaSetRoot, String.valueOf(newReplicaSetID));
            client.create().creatingParentsIfNeeded().forPath(replicaSetPath, serializeReplicaSet(rs));
            writeSuccess.getAndIncrement();
            return newReplicaSetID;
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when create replicaSet " + rs, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void updateReplicaSet(ReplicaSet rs) {
        try {
            byte[] replicaSetData = serializeReplicaSet(rs);
            client.setData().forPath(ZKPaths.makePath(replicaSetRoot, String.valueOf(rs.getReplicaSetID())),
                    replicaSetData);
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("error when update replicaSet " + rs, e);
            throw new StoreException(e);
        }
    }

    @Override
    public Node getCoordinatorNode() {
        try {
            byte[] nodeData = client.getData().forPath(coordinatorRoot);
            readSuccess.getAndIncrement();
            return JsonUtil.readValue(nodeData, Node.class);
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get coordinator leader", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void setCoordinatorNode(Node coordinator) {
        try {
            byte[] coordinatorBytes = JsonUtil.writeValueAsBytes(coordinator);
            client.setData().forPath(coordinatorRoot, coordinatorBytes);
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when set coordinator leader to " + coordinator, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void saveSourceCheckpoint(String cubeName, String segmentName, int rsID, String sourceCheckpoint) {
        checkPath(cubeName, segmentName);
        logger.trace("Save remote checkpoint {} {} {} with content {}.", cubeName, segmentName, rsID, sourceCheckpoint);
        try {
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_SRC_CHECKPOINT, segmentName,
                    String.valueOf(rsID));
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                logger.warn("Checkpoint path already existed under path {}, overwrite with new one.", path);
            }
            client.setData().forPath(path, Bytes.toBytes(sourceCheckpoint));
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when save remote checkpoint for " + cubeName + " " + segmentName , e);
            throw new StoreException(e);
        }
    }

    @Override
    public Map<Integer, String> getSourceCheckpoint(String cubeName, String segmentName) {
        try {
            Map<Integer, String> result = Maps.newHashMap();
            String ckRoot = ZKPaths.makePath(cubeRoot, cubeName, CUBE_SRC_CHECKPOINT, segmentName);
            if (client.checkExists().forPath(ckRoot) == null) {
                return null;
            }
            List<String> children = client.getChildren().forPath(ckRoot);
            if (children == null) {
                return null;
            }
            for (String child : children) {
                String rsPath = ZKPaths.makePath(ckRoot, child);
                byte[] checkpointBytes = client.getData().forPath(rsPath);
                String sourcePos = Bytes.toString(checkpointBytes);
                result.put(Integer.valueOf(child), sourcePos);
            }
            readSuccess.getAndIncrement();
            return result;
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error to fetch remote checkpoint for " + cubeName + " " + segmentName, e);
            throw new StoreException(e);
        }
    }

    private byte[] serializeReplicaSet(ReplicaSet rs) throws Exception {
        String nodesStr = JsonUtil.writeValueAsString(rs);
        return Bytes.toBytes(nodesStr);
    }

    @Override
    public ReplicaSet getReplicaSet(int rsID) {
        try {
            ReplicaSet result = new ReplicaSet();
            result.setReplicaSetID(rsID);
            byte[] replicaSetData = client.getData().forPath(ZKPaths.makePath(replicaSetRoot, String.valueOf(rsID)));
            if (replicaSetData != null && replicaSetData.length > 0) {
                result = JsonUtil.readValue(Bytes.toString(replicaSetData), ReplicaSet.class);
            }
            readSuccess.getAndIncrement();
            return result;
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get replica set " + rsID, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void removeReplicaSet(int rsID) {
        try {
            client.delete().forPath(ZKPaths.makePath(replicaSetRoot, String.valueOf(rsID)));
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when remove replica set " + rsID, e);
            throw new StoreException(e);
        }
    }

    @Override
    public List<Node> getReceivers() {
        List<Node> result = Lists.newArrayList();
        try {
            List<String> receiverNames = client.getChildren().forPath(receiverRoot);
            for (String receiverName : receiverNames) {
                Node node = Node.from(receiverName.replace('_', ':'));
                result.add(node);
            }
            readSuccess.getAndIncrement();
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when fetch receivers", e);
            throw new StoreException(e);
        }
        return result;
    }

    @Override
    public List<String> getCubes() {
        try {
            List<String> res =  client.getChildren().forPath(cubeRoot);
            readSuccess.getAndIncrement();
            return res;
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when fetch cubes", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void addStreamingCube(String cube) {
        checkPath(cube);
        try {
            String path = ZKPaths.makePath(cubeRoot, cube);
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when add cube " + cube, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void removeStreamingCube(String cube) {
        logger.trace("Remove cube {}", cube);
        checkPath(cube);
        try {
            String path = ZKPaths.makePath(cubeRoot, cube);
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(ZKPaths.makePath(cubeRoot, cube));
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when remove cube " + cube, e);
            throw new StoreException(e);
        }
    }

    @Override
    public StreamingCubeConsumeState getStreamingCubeConsumeState(String cube) {
        try {
            String path = getCubeConsumeStatePath(cube);
            if (client.checkExists().forPath(path) != null) {
                byte[] cubeInfoData = client.getData().forPath(path);
                readSuccess.getAndIncrement();
                if (cubeInfoData != null && cubeInfoData.length > 0) {
                    return JsonUtil.readValue(cubeInfoData, StreamingCubeConsumeState.class);
                } else {
                    return StreamingCubeConsumeState.RUNNING;
                }
            } else {
                return StreamingCubeConsumeState.RUNNING;
            }
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Error when get streaming cube consume state " + cube, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void saveStreamingCubeConsumeState(String cube, StreamingCubeConsumeState state) {
        checkPath(cube);
        try {
            String path = getCubeConsumeStatePath(cube);
            if (client.checkExists().forPath(path) != null) {
                client.setData().forPath(path, JsonUtil.writeValueAsBytes(state));
            } else {
                client.create().creatingParentsIfNeeded().forPath(path, JsonUtil.writeValueAsBytes(state));
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when save streaming cube consume state " + cube + " with " + state, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void addReceiver(Node receiver) {
        logger.trace("Add {}.", receiver);
        try {
            String receiverPath = ZKPaths.makePath(receiverRoot, receiver.toNormalizeString());
            if (client.checkExists().forPath(receiverPath) == null) {
                client.create().creatingParentsIfNeeded().forPath(receiverPath);
            } else {
                logger.warn("{} exists.", receiverPath);
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when add new receiver " + receiver, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void removeReceiver(Node receiver) {
        logger.trace("Remove {}.", receiver);
        try {
            String receiverPath = ZKPaths.makePath(receiverRoot, receiver.toNormalizeString());
            if (client.checkExists().forPath(receiverPath) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(receiverPath);
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Error when remove receiver " + receiver, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void saveNewCubeAssignment(CubeAssignment newCubeAssignment) {
        logger.trace("Try saving new cube assignment for: {}.", newCubeAssignment);
        try {
            String path = getCubeAssignmentPath(newCubeAssignment.getCubeName());
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded()
                        .forPath(path, CubeAssignment.serializeCubeAssignment(newCubeAssignment));
            } else {
                client.setData().forPath(path, CubeAssignment.serializeCubeAssignment(newCubeAssignment));
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Fail to save cube assignment", e);
            throw new StoreException(e);
        }
    }

    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            logger.error("Exception throws when close assignmentManager", e);
        }
    }

    @Override
    public void addCompleteReplicaSetForSegmentBuild(String cubeName, String segmentName, int rsID) {
        logger.trace("Add completed rs {} to {} {}", rsID, cubeName, segmentName);
        checkPath(cubeName, segmentName);
        try {
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE, segmentName, "replica_sets",
                    String.valueOf(rsID));
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                logger.warn("ReplicaSet id {} existed under path {}", rsID, path);
            }
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Fail to add replicaSet Id to segment build state for " + segmentName + " " + rsID, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void updateSegmentBuildState(String cubeName, String segmentName, SegmentBuildState.BuildState state) {
        logger.trace("Update {} {} to state {}", cubeName, segmentName, state);
        checkPath(cubeName, segmentName);
        try {
            String stateStr = JsonUtil.writeValueAsString(state);
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE, segmentName);
            client.setData().forPath(path, Bytes.toBytes(stateStr));
            writeSuccess.getAndIncrement();
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Fail to update segment build state for " + segmentName + " to " + state, e);
            throw new StoreException(e);
        }
    }

    @Override
    public List<SegmentBuildState> getSegmentBuildStates(String cubeName) {
        try {
            String cubePath = getCubeBuildStatePath(cubeName);
            if (client.checkExists().forPath(cubePath) == null) {
                return Lists.newArrayList();
            }
            List<String> segments = client.getChildren().forPath(cubePath);
            readSuccess.getAndIncrement();
            List<SegmentBuildState> result = Lists.newArrayList();
            for (String segment : segments) {
                SegmentBuildState segmentState = doGetSegmentBuildState(cubePath, segment);
                result.add(segmentState);
            }
            return result;
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Fail to get segment build states " + cubeName, e);
            throw new StoreException(e);
        }
    }

    @Override
    public SegmentBuildState getSegmentBuildState(String cubeName, String segmentName) {
        try {
            String cubePath = getCubeBuildStatePath(cubeName);
            return doGetSegmentBuildState(cubePath, segmentName);
        } catch (Exception e) {
            readFail.getAndIncrement();
            logger.error("Fail to get segment build state for " + cubeName + " " +segmentName, e);
            throw new StoreException(e);
        }
    }

    private SegmentBuildState doGetSegmentBuildState(String cubePath, String segmentName) throws Exception {
        SegmentBuildState segmentState = new SegmentBuildState(segmentName);
        String segmentPath = ZKPaths.makePath(cubePath, segmentName);
        byte[] stateBytes = client.getData().forPath(segmentPath);
        readSuccess.getAndIncrement();
        SegmentBuildState.BuildState state;
        if (stateBytes != null && stateBytes.length > 0) {
            String stateStr = Bytes.toString(stateBytes);
            state = JsonUtil.readValue(stateStr, SegmentBuildState.BuildState.class);
            segmentState.setState(state);
        }
        String replicaSetsPath = ZKPaths.makePath(segmentPath, "replica_sets");
        List<String> replicaSets = client.getChildren().forPath(replicaSetsPath);
        for (String replicaSetID : replicaSets) {
            segmentState.addCompleteReplicaSet(Integer.valueOf(replicaSetID));
        }
        return segmentState;
    }

    @Override
    public boolean removeSegmentBuildState(String cubeName, String segmentName) {
        logger.trace("Remove {} {}", cubeName, segmentName);
        checkPath(cubeName, segmentName);
        try {
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE, segmentName);
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                writeSuccess.getAndIncrement();
                return true;
            } else {
                logger.warn("Cube segment deep store state does not exisit!, path {} ", path);
                return false;
            }
        } catch (Exception e) {
            writeFail.getAndIncrement();
            logger.error("Fail to remove cube segment deep store state " + cubeName + " " + segmentName, e);
            throw new StoreException(e);
        }
    }

    private String getCubeAssignmentPath(String cubeName) {
        return ZKPaths.makePath(cubeRoot, cubeName, CUBE_ASSIGNMENT);
    }

    private String getCubeBuildStatePath(String cubeName) {
        return ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE);
    }

    private String getCubeConsumeStatePath(String cubeName) {
        return ZKPaths.makePath(cubeRoot, cubeName, CUBE_CONSUME_STATE);
    }

    String reportTemplate = "[StreamMetadataStoreStats]  read : {} ; write: {} ; read failed: {} ; write failed: {} .";
    private AtomicLong lastReport = new AtomicLong();
    private static final long REPORT_DURATION = 300L * 1000;

    @Override
    public void reportStat() {
        if (writeFail.get() > 0 || readFail.get() > 0) {
            logger.warn(reportTemplate, readSuccess.get(), writeSuccess.get(), readFail.get(), writeFail.get());
        } else {
            if (System.currentTimeMillis() - lastReport.get() >= REPORT_DURATION) {
                logger.debug(reportTemplate, readSuccess.get(), writeSuccess.get(), readFail.get(), writeFail.get());
            } else {
                return;
            }
        }
        lastReport.set(System.currentTimeMillis());
    }

    private void checkPath(String... paths){
        for (String path : paths){
            if (path == null || path.length() == 0) {
                throw new IllegalArgumentException("Illegal zookeeper path.");
            }
        }
    }
}
