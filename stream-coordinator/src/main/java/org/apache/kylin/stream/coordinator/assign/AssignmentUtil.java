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

package org.apache.kylin.stream.coordinator.assign;

import java.util.List;
import java.util.Map;

import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.source.Partition;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class AssignmentUtil {

    /**
     * 
     * @param cubeAssignmentList
     * @return ReplicaSet assignment map, key is the replicaSet ID, value is the
     *         streaming cube and its assigned partition map.
     */
    public static Map<Integer, Map<String, List<Partition>>> convertCubeAssign2ReplicaSetAssign(
            List<CubeAssignment> cubeAssignmentList) {
        Map<Integer, Map<String, List<Partition>>> nodeAssignmentsMap = Maps.newHashMap();
        for (CubeAssignment cubeAssignment : cubeAssignmentList) {
            String cubeName = cubeAssignment.getCubeName();
            for (Integer replicaSetID : cubeAssignment.getReplicaSetIDs()) {
                List<Partition> partitions = cubeAssignment.getPartitionsByReplicaSetID(replicaSetID);
                Map<String, List<Partition>> nodeAssignment = nodeAssignmentsMap.get(replicaSetID);
                if (nodeAssignment == null) {
                    nodeAssignment = Maps.newLinkedHashMap();
                    nodeAssignmentsMap.put(replicaSetID, nodeAssignment);
                }
                nodeAssignment.put(cubeName, partitions);
            }
        }
        return nodeAssignmentsMap;
    }

    /**
     * 
     * @param replicaSetAssignmentsMap
     * @return CubeAssignment list
     */
    public static List<CubeAssignment> convertReplicaSetAssign2CubeAssign(
            Map<Integer, Map<String, List<Partition>>> replicaSetAssignmentsMap) {
        Map<String, Map<Integer, List<Partition>>> cubeAssignmentsMap = Maps.newHashMap();
        for (Map.Entry<Integer, Map<String, List<Partition>>> entry : replicaSetAssignmentsMap.entrySet()) {
            Integer replicaSetID = entry.getKey();
            Map<String, List<Partition>> nodeAssignmentsInfo = entry.getValue();
            for (Map.Entry<String, List<Partition>> assignment : nodeAssignmentsInfo.entrySet()) {
                String cubeName = assignment.getKey();
                List<Partition> partitions = assignment.getValue();
                Map<Integer, List<Partition>> cubeAssignment = cubeAssignmentsMap.get(cubeName);
                if (cubeAssignment == null) {
                    cubeAssignment = Maps.newHashMap();
                    cubeAssignmentsMap.put(cubeName, cubeAssignment);
                }
                cubeAssignment.put(replicaSetID, partitions);
            }
        }

        List<CubeAssignment> result = Lists.newArrayList();
        for (Map.Entry<String, Map<Integer, List<Partition>>> cubeAssignmentsEntry : cubeAssignmentsMap.entrySet()) {
            CubeAssignment cubeAssignment = new CubeAssignment(cubeAssignmentsEntry.getKey(),
                    cubeAssignmentsEntry.getValue());
            result.add(cubeAssignment);
        }
        return result;
    }
}
