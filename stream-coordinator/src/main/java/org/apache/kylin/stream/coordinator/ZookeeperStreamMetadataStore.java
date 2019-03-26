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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
        try {
            client.delete().forPath(ZKPaths.makePath(cubeRoot, cubeName, CUBE_ASSIGNMENT));
        } catch (Exception e) {
            logger.error("error when remove cube assignment", e);
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
            return cubeAssignmentList;
        } catch (Exception e) {
            logger.error("error when get assignments");
            throw new StoreException(e);
        }
    }

    @Override
    public Map<Integer, Map<String, List<Partition>>> getAllReplicaSetAssignments() {
        try {
            List<CubeAssignment> cubeAssignmentList = getAllCubeAssignments();
            return AssignmentUtil.convertCubeAssign2ReplicaSetAssign(cubeAssignmentList);
        } catch (Exception e) {
            logger.error("error when get assignments");
            throw new StoreException(e);
        }
    }

    @Override
    public Map<String, List<Partition>> getAssignmentsByReplicaSet(int replicaSetID) {
        try {
            Map<Integer, Map<String, List<Partition>>> replicaSetAssignmentsMap = getAllReplicaSetAssignments();
            return replicaSetAssignmentsMap.get(replicaSetID);
        } catch (Exception e) {
            logger.error("error when get assignments");
            throw new StoreException(e);
        }
    }

    @Override
    public CubeAssignment getAssignmentsByCube(String cubeName) {
        try {
            String cubeAssignmentPath = getCubeAssignmentPath(cubeName);
            if (client.checkExists().forPath(cubeAssignmentPath) == null) {
                return null;
            }
            byte[] data = client.getData().forPath(cubeAssignmentPath);
            CubeAssignment assignment = CubeAssignment.deserializeCubeAssignment(data);
            return assignment;
        } catch (Exception e) {
            logger.error("error when get cube assignment");
            throw new StoreException(e);
        }
    }

    @Override
    public List<ReplicaSet> getReplicaSets() {
        List<ReplicaSet> result = Lists.newArrayList();
        try {
            List<String> replicaSetIDs = client.getChildren().forPath(replicaSetRoot);
            for (String replicaSetID : replicaSetIDs) {
                ReplicaSet replicaSet = getReplicaSet(Integer.parseInt(replicaSetID));
                result.add(replicaSet);
            }
        } catch (Exception e) {
            logger.error("error when get replica sets", e);
            throw new StoreException(e);
        }
        return result;
    }

    @Override
    public List<Integer> getReplicaSetIDs() {
        try {
            List<String> replicaSetIDs = client.getChildren().forPath(replicaSetRoot);
            return Lists.transform(replicaSetIDs, new Function<String, Integer>() {
                @Nullable
                @Override
                public Integer apply(@Nullable String input) {
                    return Integer.valueOf(input);
                }
            });
        } catch (Exception e) {
            logger.error("error when get replica sets", e);
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
            rs.setReplicaSetID(newReplicaSetID);
            String replicaSetPath = ZKPaths.makePath(replicaSetRoot, String.valueOf(newReplicaSetID));
            client.create().creatingParentsIfNeeded().forPath(replicaSetPath, serializeReplicaSet(rs));
            return newReplicaSetID;
        } catch (Exception e) {
            logger.error("error when create replicaSet:" + rs);
            throw new StoreException(e);
        }
    }

    @Override
    public void updateReplicaSet(ReplicaSet rs) {
        try {
            byte[] replicaSetData = serializeReplicaSet(rs);
            client.setData().forPath(ZKPaths.makePath(replicaSetRoot, String.valueOf(rs.getReplicaSetID())),
                    replicaSetData);
        } catch (Exception e) {
            logger.error("error when update replicaSet:" + rs.getReplicaSetID());
            throw new StoreException(e);
        }
    }

    @Override
    public Node getCoordinatorNode() {
        try {
            byte[] nodeData = client.getData().forPath(coordinatorRoot);
            return JsonUtil.readValue(nodeData, Node.class);
        } catch (Exception e) {
            logger.error("error when get coordinator", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void setCoordinatorNode(Node coordinator) {
        try {
            byte[] coordinatorBytes = JsonUtil.writeValueAsBytes(coordinator);
            client.setData().forPath(coordinatorRoot, coordinatorBytes);
        } catch (Exception e) {
            logger.error("error when set coordinator", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void saveSourceCheckpoint(String cubeName, String segmentName, int rsID, String sourceCheckpoint) {
        try {
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_SRC_CHECKPOINT, segmentName,
                    String.valueOf(rsID));
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                logger.warn("checkpoint path already existed under path {}", path);
            }
            client.setData().forPath(path, Bytes.toBytes(sourceCheckpoint));
        } catch (Exception e) {
            logger.error("fail to add replicaSet Id to segment build state", e);
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
            return result;
        } catch (Exception e) {
            logger.error("fail to add replicaSet Id to segment build state", e);
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

            return result;
        } catch (Exception e) {
            logger.error("error when get replica set:" + rsID);
            throw new StoreException(e);
        }
    }

    @Override
    public void removeReplicaSet(int rsID) {
        try {
            client.delete().forPath(ZKPaths.makePath(replicaSetRoot, String.valueOf(rsID)));
        } catch (Exception e) {
            logger.error("error when remove replica set:" + rsID);
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
        } catch (Exception e) {
            logger.error("error when fetch receivers", e);
            throw new StoreException(e);
        }
        return result;
    }

    @Override
    public List<String> getCubes() {
        try {
            return client.getChildren().forPath(cubeRoot);
        } catch (Exception e) {
            logger.error("error when fetch cubes", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void addStreamingCube(String cube) {
        try {
            String path = ZKPaths.makePath(cubeRoot, cube);
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
        } catch (Exception e) {
            logger.error("error when add cube", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void removeStreamingCube(String cube) {
        try {
            String path = ZKPaths.makePath(cubeRoot, cube);
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(ZKPaths.makePath(cubeRoot, cube));
            }
        } catch (Exception e) {
            logger.error("error when remove cube", e);
            throw new StoreException(e);
        }
    }

    @Override
    public StreamingCubeConsumeState getStreamingCubeConsumeState(String cube) {
        try {
            String path = getCubeConsumeStatePath(cube);
            if (client.checkExists().forPath(path) != null) {
                byte[] cubeInfoData = client.getData().forPath(path);
                if (cubeInfoData != null && cubeInfoData.length > 0) {
                    return JsonUtil.readValue(cubeInfoData, StreamingCubeConsumeState.class);
                } else {
                    return StreamingCubeConsumeState.RUNNING;
                }
            } else {
                return StreamingCubeConsumeState.RUNNING;
            }
        } catch (Exception e) {
            logger.error("error when get streaming cube consume state", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void saveStreamingCubeConsumeState(String cube, StreamingCubeConsumeState state) {
        try {
            String path = getCubeConsumeStatePath(cube);
            if (client.checkExists().forPath(path) != null) {
                client.setData().forPath(path, JsonUtil.writeValueAsBytes(state));
            } else {
                client.create().creatingParentsIfNeeded().forPath(path, JsonUtil.writeValueAsBytes(state));
            }
        } catch (Exception e) {
            logger.error("error when save streaming cube consume state", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void addReceiver(Node receiver) {
        try {
            String receiverPath = ZKPaths.makePath(receiverRoot, receiver.toNormalizeString());
            if (client.checkExists().forPath(receiverPath) == null) {
                client.create().creatingParentsIfNeeded().forPath(receiverPath);
            }
        } catch (Exception e) {
            logger.error("error when add new receiver", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void removeReceiver(Node receiver) {
        try {
            String receiverPath = ZKPaths.makePath(receiverRoot, receiver.toNormalizeString());
            if (client.checkExists().forPath(receiverPath) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(receiverPath);
            }
        } catch (Exception e) {
            logger.error("error when remove receiver:" + receiver, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void saveNewCubeAssignment(CubeAssignment newCubeAssignment) {
        logger.info("try saving new cube assignment for:" + newCubeAssignment.getCubeName());
        try {
            String path = getCubeAssignmentPath(newCubeAssignment.getCubeName());
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded()
                        .forPath(path, CubeAssignment.serializeCubeAssignment(newCubeAssignment));
            } else {
                client.setData().forPath(path, CubeAssignment.serializeCubeAssignment(newCubeAssignment));
            }
        } catch (Exception e) {
            logger.error("fail to save cube assignment", e);
            throw new StoreException(e);
        }
    }

    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            logger.error("exception throws when close assignmentManager", e);
        }
    }

    @Override
    public void addCompleteReplicaSetForSegmentBuild(String cubeName, String segmentName, int rsID) {
        try {
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE, segmentName, "replica_sets",
                    String.valueOf(rsID));
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            } else {
                logger.warn("ReplicaSet id {} existed under path {}", rsID, path);
            }
        } catch (Exception e) {
            logger.error("fail to add replicaSet Id to segment build state", e);
            throw new StoreException(e);
        }
    }

    @Override
    public void updateSegmentBuildState(String cubeName, String segmentName, SegmentBuildState.BuildState state) {
        try {
            String stateStr = JsonUtil.writeValueAsString(state);
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE, segmentName);
            client.setData().forPath(path, Bytes.toBytes(stateStr));
        } catch (Exception e) {
            logger.error("fail to update segment build state", e);
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
            List<SegmentBuildState> result = Lists.newArrayList();
            for (String segment : segments) {
                SegmentBuildState segmentState = doGetSegmentBuildState(cubePath, segment);
                result.add(segmentState);
            }
            return result;
        } catch (Exception e) {
            logger.error("fail to get segment build states", e);
            throw new StoreException(e);
        }
    }

    @Override
    public SegmentBuildState getSegmentBuildState(String cubeName, String segmentName) {
        try {
            String cubePath = getCubeBuildStatePath(cubeName);
            return doGetSegmentBuildState(cubePath, segmentName);
        } catch (Exception e) {
            logger.error("fail to get cube segment remote store state", e);
            throw new StoreException(e);
        }
    }

    private SegmentBuildState doGetSegmentBuildState(String cubePath, String segmentName) throws Exception {
        SegmentBuildState segmentState = new SegmentBuildState(segmentName);
        String segmentPath = ZKPaths.makePath(cubePath, segmentName);
        byte[] stateBytes = client.getData().forPath(segmentPath);
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
        try {
            String path = ZKPaths.makePath(cubeRoot, cubeName, CUBE_BUILD_STATE, segmentName);
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                return true;
            } else {
                logger.warn("cube segment deep store state does not exisit!, path {} ", path);
                return false;
            }
        } catch (Exception e) {
            logger.error("fail to remove cube segment deep store state", e);
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
}
