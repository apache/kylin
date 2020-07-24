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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.stream.coordinator.StreamingCubeInfo;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.source.Partition;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.FluentIterable;
import org.apache.kylin.shaded.com.google.common.collect.ImmutableSet;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class CubePartitionRoundRobinAssigner implements Assigner {

    @Override
    public Map<Integer, Map<String, List<Partition>>> reBalancePlan(List<ReplicaSet> replicaSets,
                                                                    List<StreamingCubeInfo> cubes, List<CubeAssignment> existingAssignments) {
        Map<Integer, Map<String, List<Partition>>> newPlan = Maps.newHashMap();
        if (replicaSets == null || cubes == null || cubes.size() == 0 || replicaSets.size() == 0) {
            return newPlan;
        }

        ImmutableSet<Integer> rsIdSet = FluentIterable.from(replicaSets).transform(new Function<ReplicaSet, Integer>() {
            @Override
            public Integer apply(ReplicaSet rs) {
                return rs.getReplicaSetID();
            }
        }).toSet();

        Map<Integer, Map<String, List<Partition>>> existingRSAssignmentsMap = AssignmentUtil
                .convertCubeAssign2ReplicaSetAssign(existingAssignments);

        //It is possible that there are new partitions coming from the streaming source that are never assigned before.
        Set<CubePartition> cubePartitions = expandCubePartitions(cubes);
        int avgCubePartitionsPerNode = cubePartitions.size() / replicaSets.size();

        Set<CubePartition> cubePartitionsNeedReassign = Sets.newTreeSet(cubePartitions);

        for (Map.Entry<Integer, Map<String, List<Partition>>> groupAssignmentEntry : existingRSAssignmentsMap
                .entrySet()) {
            Integer replicaSetID = groupAssignmentEntry.getKey();
            Map<String, List<Partition>> existNodeAssignment = groupAssignmentEntry.getValue();

            // some groups are removed
            if (!rsIdSet.contains(replicaSetID)) {
                continue;
            }
            List<CubePartition> existCubePartitions = expandAndIntersectCubePartitions(existNodeAssignment);
            for (CubePartition existCubePartition : existCubePartitions) {
                if (cubePartitions.contains(existCubePartition)) {
                    Map<String, List<Partition>> newGroupAssignment = newPlan.get(replicaSetID);
                    if (newGroupAssignment == null) {
                        newGroupAssignment = Maps.newHashMap();
                        newPlan.put(replicaSetID, newGroupAssignment);
                    }
                    int cubePartitionCnt = calCubePartitionCnt(newGroupAssignment.values());
                    if (cubePartitionCnt < avgCubePartitionsPerNode + 1) {
                        addToGroupAssignment(newGroupAssignment, existCubePartition.cubeName,
                                existCubePartition.partition);
                        cubePartitionsNeedReassign.remove(existCubePartition);
                    }
                }
            }
        }
        int rsIdx = 0;
        int rsSize = replicaSets.size();

        LinkedList<CubePartition> cubePartitionsNeedReassignList = Lists.newLinkedList(cubePartitionsNeedReassign);
        while (!cubePartitionsNeedReassignList.isEmpty()) {
            CubePartition cubePartition = cubePartitionsNeedReassignList.peek();
            String cubeName = cubePartition.cubeName;

            Integer replicaSetID = replicaSets.get(rsIdx).getReplicaSetID();
            Map<String, List<Partition>> newGroupAssignment = newPlan.get(replicaSetID);

            if (newGroupAssignment == null) {
                newGroupAssignment = Maps.newHashMap();
                newPlan.put(replicaSetID, newGroupAssignment);
            }
            int cubePartitionCnt = calCubePartitionCnt(newGroupAssignment.values());
            if (cubePartitionCnt < avgCubePartitionsPerNode + 1) {
                addToGroupAssignment(newGroupAssignment, cubeName, cubePartition.partition);
                cubePartitionsNeedReassignList.remove();
            }

            rsIdx = (rsIdx + 1) % rsSize;
        }
        return newPlan;
    }

    @Override
    public CubeAssignment assign(StreamingCubeInfo cube, List<ReplicaSet> replicaSets,
                                 List<CubeAssignment> existingAssignments) {
        int existingTotalPartitionNum = 0;
        int totalPartitionNum = 0;
        final Map<Integer, Integer> replicaSetPartitionNumMap = Maps.newHashMap();
        for (CubeAssignment cubeAssignment : existingAssignments) {
            Set<Integer> replicaSetIDs = cubeAssignment.getReplicaSetIDs();
            for (Integer rsID : replicaSetIDs) {
                int rsPartitionNum = cubeAssignment.getPartitionsByReplicaSetID(rsID).size();
                Integer replicaSetPartitionNum = replicaSetPartitionNumMap.get(rsID);
                if (replicaSetPartitionNum == null) {
                    replicaSetPartitionNumMap.put(rsID, rsPartitionNum);
                } else {
                    replicaSetPartitionNumMap.put(rsID, rsPartitionNum + replicaSetPartitionNum);
                }
                existingTotalPartitionNum += rsPartitionNum;
            }
        }

        List<Partition> partitionsOfCube = cube.getStreamingTableSourceInfo().getPartitions();
        int cubePartitionNum = partitionsOfCube.size();
        totalPartitionNum += (existingTotalPartitionNum + cubePartitionNum);
        int replicaSetsNum = replicaSets.size();
        int avgPartitionsPerRS = totalPartitionNum / replicaSetsNum;

        // Sort the ReplicaSet by partitions number on it
        Collections.sort(replicaSets, new Comparator<ReplicaSet>() {
            @Override
            public int compare(ReplicaSet o1, ReplicaSet o2) {
                Integer partitionNum1Obj = replicaSetPartitionNumMap.get(o1.getReplicaSetID());
                int partitionNum1 = partitionNum1Obj == null ? 0 : partitionNum1Obj;
                Integer partitionNum2Obj = replicaSetPartitionNumMap.get(o2.getReplicaSetID());
                int partitionNum2 = partitionNum2Obj == null ? 0 : partitionNum2Obj;
                return partitionNum1 - partitionNum2;
            }
        });

        int nextAssignPartitionIdx = 0;
        Map<Integer, List<Partition>> assignments = Maps.newHashMap();
        for (ReplicaSet rs : replicaSets) {
            if (nextAssignPartitionIdx >= cubePartitionNum) {
                break;
            }
            Integer replicaSetID = rs.getReplicaSetID();
            Integer partitionNumObj = replicaSetPartitionNumMap.get(replicaSetID);
            int partitionNum = partitionNumObj == null ? 0 : partitionNumObj;
            int availableRoom = avgPartitionsPerRS - partitionNum;
            if (availableRoom <= 0) {
                continue;
            }
            int end = (nextAssignPartitionIdx + availableRoom) < cubePartitionNum ? (nextAssignPartitionIdx + availableRoom)
                    : cubePartitionNum;
            assignments.put(replicaSetID, Lists.newArrayList(partitionsOfCube.subList(nextAssignPartitionIdx, end)));
            nextAssignPartitionIdx = end;
        }

        if (nextAssignPartitionIdx < cubePartitionNum) {
            for (ReplicaSet rs : replicaSets) {
                if (nextAssignPartitionIdx >= cubePartitionNum) {
                    break;
                }
                Integer replicaSetID = rs.getReplicaSetID();
                Partition part = partitionsOfCube.get(nextAssignPartitionIdx);
                List<Partition> partitions = assignments.get(replicaSetID);
                if (partitions == null) {
                    partitions = Lists.newArrayList();
                    assignments.put(replicaSetID, partitions);
                }
                partitions.add(part);
                nextAssignPartitionIdx++;
            }
        }

        CubeAssignment cubeAssignment = new CubeAssignment(cube.getCubeName(), assignments);
        return cubeAssignment;
    }

    private int calCubePartitionCnt(Collection<List<Partition>> allPartitions) {
        int size = 0;
        for (List<Partition> partitions : allPartitions) {
            if (partitions != null) {
                size += partitions.size();
            }
        }
        return size;
    }

    private Set<CubePartition> expandCubePartitions(List<StreamingCubeInfo> cubes) {
        Set<CubePartition> result = Sets.newHashSet();
        for (StreamingCubeInfo cube : cubes) {
            String cubeName = cube.getCubeName();
            List<Partition> partitionsOfCube = cube.getStreamingTableSourceInfo().getPartitions();
            for (Partition partition : partitionsOfCube) {
                result.add(new CubePartition(cubeName, partition));
            }
        }
        return result;
    }

    /**
     * expand the node assignment to cube partition list, and the list is intersect by cube name,
     * for example:
     *
     * input node assignment is: {cube1:[1,2,3], cube2:[1,2,3,4], cube3:[1]}, the output would be:
     * [[cube1,1],[cube2,1],[cube3,1],[cube1,2],[cube2,2],[cube1,3],[cube2,3],[cube2,4]]
     * @param nodeAssignment
     * @return
     */
    protected List<CubePartition> expandAndIntersectCubePartitions(Map<String, List<Partition>> nodeAssignment) {
        List<CubePartition> result = Lists.newArrayList();

        Map<Partition, Set<String>> reverseMap = Maps.newTreeMap();
        for (Map.Entry<String, List<Partition>> cubePartitionEntry : nodeAssignment.entrySet()) {
            String cubeName = cubePartitionEntry.getKey();
            List<Partition> partitions = cubePartitionEntry.getValue();
            for (Partition partition : partitions) {
                Set<String> cubes = reverseMap.get(partition);
                if (cubes == null) {
                    cubes = Sets.newTreeSet();
                    reverseMap.put(partition, cubes);
                }
                cubes.add(cubeName);
            }
        }

        for (Map.Entry<Partition, Set<String>> partitionCubesEntry : reverseMap.entrySet()) {
            Partition partition = partitionCubesEntry.getKey();
            Set<String> cubes = partitionCubesEntry.getValue();
            for (String cube : cubes) {
                CubePartition cubePartition = new CubePartition(cube, partition);
                result.add(cubePartition);
            }
        }

        return result;
    }

    public void addToGroupAssignment(Map<String, List<Partition>> groupAssignment, String cubeName, Partition partition) {
        List<Partition> partitions = groupAssignment.get(cubeName);
        if (partitions == null) {
            partitions = Lists.newArrayList();
            groupAssignment.put(cubeName, partitions);
        }
        partitions.add(partition);
    }

    protected static class CubePartition implements Comparable<CubePartition> {
        public String cubeName;
        public Partition partition;

        public CubePartition(String cubeName, Partition partition) {
            this.cubeName = cubeName;
            this.partition = partition;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((cubeName == null) ? 0 : cubeName.hashCode());
            result = prime * result + ((partition == null) ? 0 : partition.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CubePartition other = (CubePartition) obj;
            if (cubeName == null) {
                if (other.cubeName != null)
                    return false;
            } else if (!cubeName.equals(other.cubeName))
                return false;
            if (partition == null) {
                if (other.partition != null)
                    return false;
            } else if (!partition.equals(other.partition))
                return false;
            return true;
        }

        @Override
        public int compareTo(CubePartition other) {
            int result = cubeName.compareTo(other.cubeName);
            if (result != 0) {
                return result;
            }

            return partition.getPartitionId() - other.partition.getPartitionId();
        }

        @Override
        public String toString() {
            return "CubePartition{" + "cubeName='" + cubeName + '\'' + ", partition=" + partition + '}';
        }
    }

}
