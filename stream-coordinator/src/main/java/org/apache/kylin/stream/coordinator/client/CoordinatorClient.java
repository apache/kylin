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

package org.apache.kylin.stream.coordinator.client;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.source.Partition;

public interface CoordinatorClient {

    // ================================================================================
    // ============================= Receiver side ====================================

    /**
     * Notified by a receiver that a part of segment data has been uploaded to Deep Storage.
     * Coordinator will try to find any segment which is ready to build into HBase and sumbit building job.
     */
    void segmentRemoteStoreComplete(Node receiverNode, String cubeName, Pair<Long, Long> segmentRange);

    /**
     * Notified by replica set leader that a leader has been changed to it. No influence now.
     */
    void replicaSetLeaderChange(int replicaSetId, Node newLeader);


    // ================================================================================
    // =========================== Coordinator side ===================================

    Map<Integer, Map<String, List<Partition>>> reBalanceRecommend();

    void reBalance(Map<Integer, Map<String, List<Partition>>> reBalancePlan);

    void assignCube(String cubeName);

    void unAssignCube(String cubeName);

    void reAssignCube(String cubeName, CubeAssignment newAssignments);

    void createReplicaSet(ReplicaSet rs);

    void removeReplicaSet(int rsID);

    void addNodeToReplicaSet(Integer replicaSetID, String nodeID);

    void removeNodeFromReplicaSet(Integer replicaSetID, String nodeID);

    void pauseConsumers(String cubeName);

    void resumeConsumers(String cubeName);
}
