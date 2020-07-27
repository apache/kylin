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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("streamingCoordinatorService")
public class StreamingCoordinatorService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(StreamingCoordinatorService.class);

//    private StreamMetadataStore streamMetadataStore;
//
//    private CoordinatorClient streamingCoordinator;
//
//    public StreamingCoordinatorService() {
//        streamMetadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
//        if (KylinConfig.getInstanceFromEnv().isNewCoordinatorEnabled()) {
//            logger.info("Use new version coordinator.");
//            streamingCoordinator = StreamingCoordinator.getInstance();
//        } else {
//            logger.info("Use old version coordinator.");
//            streamingCoordinator = Coordinator.getInstance();
//        }
//    }
//
//    public synchronized Map<Integer, Map<String, List<Partition>>> reBalanceRecommend() {
//        return streamingCoordinator.reBalanceRecommend();
//    }
//
//    public synchronized void reBalance(Map<Integer, Map<String, List<Partition>>> reBalancePlan) {
//        streamingCoordinator.reBalance(reBalancePlan);
//    }
//
//    public void assignCube(String cubeName) {
//        streamingCoordinator.assignCube(cubeName);
//    }
//
//    public void unAssignCube(String cubeName) {
//        streamingCoordinator.unAssignCube(cubeName);
//    }
//
//    public void reAssignCube(String cubeName, CubeAssignment newAssignment) {
//        validateAssignment(newAssignment);
//        streamingCoordinator.reAssignCube(cubeName, newAssignment);
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
//    public void pauseConsumers(String cubeName) {
//        streamingCoordinator.pauseConsumers(cubeName);
//    }
//
//    public void resumeConsumers(String cubeName) {
//        streamingCoordinator.resumeConsumers(cubeName);
//    }
//
//    public void replicaSetLeaderChange(int replicaSetID, Node newLeader) {
//        streamingCoordinator.replicaSetLeaderChange(replicaSetID, newLeader);
//    }
//
//    public void createReplicaSet(ReplicaSet rs) {
//        streamingCoordinator.createReplicaSet(rs);
//    }
//
//    public void removeReplicaSet(int rsID) {
//        streamingCoordinator.removeReplicaSet(rsID);
//    }
//
//    public void addNodeToReplicaSet(Integer replicaSetID, String nodeID) {
//        streamingCoordinator.addNodeToReplicaSet(replicaSetID, nodeID);
//    }
//
//    public void removeNodeFromReplicaSet(Integer replicaSetID, String nodeID) {
//        streamingCoordinator.removeNodeFromReplicaSet(replicaSetID, nodeID);
//    }
//
//    public void onSegmentRemoteStoreComplete(String cubeName, Pair<Long, Long> segment, Node receiver) {
//        streamingCoordinator.segmentRemoteStoreComplete(receiver, cubeName, segment);
//    }
}
