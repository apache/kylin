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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.stream.coordinator.StreamingCubeInfo;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.source.Partition;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for Assigner, assign according to the consumer task number for each cube
 *
 */
public class DefaultAssigner implements Assigner {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAssigner.class);

    @Override
    public Map<Integer, Map<String, List<Partition>>> reBalancePlan(List<ReplicaSet> replicaSets,
            List<StreamingCubeInfo> cubes, List<CubeAssignment> existingAssignments) {
        Map<Integer, Map<String, List<Partition>>> newPlan = Maps.newHashMap();
        if (replicaSets == null || cubes == null || cubes.size() == 0 || replicaSets.size() == 0) {
            return newPlan;
        }
        Map<Integer, Map<String, List<Partition>>> existingRSAssignmentsMap = AssignmentUtil
                .convertCubeAssign2ReplicaSetAssign(existingAssignments);
        Map<String, CubeAssignment> cubeAssignmentMap = Maps.newHashMap();
        for (CubeAssignment existingAssignment : existingAssignments) {
            cubeAssignmentMap.put(existingAssignment.getCubeName(), existingAssignment);
        }
        Set<Integer> currReplicaSetIDs = Sets.newHashSet();
        for (ReplicaSet replicaSet : replicaSets) {
            currReplicaSetIDs.add(replicaSet.getReplicaSetID());
        }
        int totalTasks = 0;

        // find partitions changed cube
        Map<String, List<List<Partition>>> requireReassignTaskMap = Maps.newHashMap();
        Set<String> partitionChangeCubes = Sets.newHashSet();
        for (StreamingCubeInfo cube : cubes) {
            CubeAssignment existingAssignment = cubeAssignmentMap.get(cube.getCubeName());
            int prevPartitionNum = existingAssignment.getPartitionIDs().size();
            int currPartitionNum = cube.getStreamingTableSourceInfo().getPartitions().size();
            if (prevPartitionNum != currPartitionNum) {
                List<List<Partition>> cubeConsumeTasks = splitCubeConsumeTasks(cube, replicaSets.size());
                requireReassignTaskMap.put(cube.getCubeName(), cubeConsumeTasks);
                partitionChangeCubes.add(cube.getCubeName());
            }
            totalTasks += cube.getNumOfConsumerTasks();
        }

        int avgTasks = totalTasks / replicaSets.size();

        for (Entry<Integer, Map<String, List<Partition>>> rsAssignmentEntry : existingRSAssignmentsMap.entrySet()) {
            Integer rsId = rsAssignmentEntry.getKey();
            Map<String, List<Partition>> rsAssignment = rsAssignmentEntry.getValue();
            if (!currReplicaSetIDs.contains(rsId)) { // handle removed replica set assignments
                throw new IllegalStateException("current replica sets don't contain rs:" + rsId);
            }
            Map<String, List<Partition>> newRsTaskMap = newPlan.get(rsId);
            if (newRsTaskMap == null) {
                newRsTaskMap = Maps.newHashMap();
                newPlan.put(rsId, newRsTaskMap);
            }
            for (Entry<String, List<Partition>> taskEntry : rsAssignment.entrySet()) {
                String cubeName = taskEntry.getKey();
                List<Partition> partitions = taskEntry.getValue();
                if (partitionChangeCubes.contains(cubeName)) {
                    continue;
                }
                if (newRsTaskMap.size() < avgTasks) {
                    newRsTaskMap.put(cubeName, partitions);
                } else {
                    List<List<Partition>> cubeTasks = requireReassignTaskMap.get(cubeName);
                    if (cubeTasks == null) {
                        cubeTasks = Lists.newArrayList();
                        requireReassignTaskMap.put(cubeName, cubeTasks);
                    }
                    cubeTasks.add(partitions);
                }
            }
        }

        for (Entry<String, List<List<Partition>>> requireReassignTaskEntry : requireReassignTaskMap.entrySet()) {
            setNewPlanForCube(requireReassignTaskEntry.getKey(), requireReassignTaskEntry.getValue(), replicaSets, newPlan);
        }
        return newPlan;
    }

    private void setNewPlanForCube(String cubeName, List<List<Partition>> tasks, List<ReplicaSet> replicaSets, final Map<Integer, Map<String, List<Partition>>> newPlan) {
        Collections.sort(replicaSets, new Comparator<ReplicaSet>() {
            @Override
            public int compare(ReplicaSet o1, ReplicaSet o2) {
                Map<String, List<Partition>> rs1Assign = newPlan.get(o1.getReplicaSetID());
                Map<String, List<Partition>> rs2Assign = newPlan.get(o2.getReplicaSetID());
                int taskNum1 = rs1Assign == null ? 0 : rs1Assign.size();
                int taskNum2 = rs2Assign == null ? 0 : rs2Assign.size();
                return taskNum1 - taskNum2;
            }
        });

        for (int i = 0; i < tasks.size(); i++) {
            List<Partition> task = tasks.get(i);
            int rsId = replicaSets.get(i).getReplicaSetID();
            Map<String, List<Partition>> cubeTaskMap = newPlan.get(rsId);
            if (cubeTaskMap == null) {
                cubeTaskMap = Maps.newHashMap();
                newPlan.put(rsId, cubeTaskMap);
            }
            cubeTaskMap.put(cubeName, task);
        }
    }

    @Override
    public CubeAssignment assign(StreamingCubeInfo cube, List<ReplicaSet> replicaSets,
            List<CubeAssignment> existingAssignments) {
        final Map<Integer, Integer> replicaSetTaskNumMap = Maps.newHashMap();
        for (CubeAssignment cubeAssignment : existingAssignments) {
            Set<Integer> rsIds = cubeAssignment.getReplicaSetIDs();
            for (Integer rsId : rsIds) {
                Integer taskNum = replicaSetTaskNumMap.get(rsId);
                if (taskNum != null) {
                    taskNum = taskNum + 1;
                } else {
                    taskNum = 1;
                }
                replicaSetTaskNumMap.put(rsId, taskNum);
            }
        }

        Collections.sort(replicaSets, new Comparator<ReplicaSet>() {
            @Override
            public int compare(ReplicaSet o1, ReplicaSet o2) {
                Integer value1 = replicaSetTaskNumMap.get(o1.getReplicaSetID());
                Integer value2 = replicaSetTaskNumMap.get(o2.getReplicaSetID());
                int taskNum1 = value1 == null ? 0 : value1;
                int taskNum2 = value2 == null ? 0 : value2;
                return taskNum1 - taskNum2;
            }
        });

        List<List<Partition>> taskPartitions = splitCubeConsumeTasks(cube, replicaSets.size());

        Map<Integer, List<Partition>> assignment = Maps.newHashMap();
        for (int i = 0; i < taskPartitions.size(); i++) {
            assignment.put(replicaSets.get(i).getReplicaSetID(), taskPartitions.get(i));
        }
        CubeAssignment cubeAssignment = new CubeAssignment(cube.getCubeName(), assignment);
        return cubeAssignment;
    }

    private int getCubeConsumerTasks(StreamingCubeInfo cube, int replicaSetNum) {
        int cubeConsumerTaskNum = cube.getNumOfConsumerTasks();
        if (cubeConsumerTaskNum <= 0) {
            cubeConsumerTaskNum = 1;
        }
        List<Partition> partitionsOfCube = cube.getStreamingTableSourceInfo().getPartitions();
        if (cubeConsumerTaskNum > replicaSetNum) {
            cubeConsumerTaskNum = replicaSetNum;
        }
        if (cubeConsumerTaskNum > partitionsOfCube.size()) {
            cubeConsumerTaskNum = partitionsOfCube.size();
        }
        return cubeConsumerTaskNum;
    }

    private List<List<Partition>> splitCubeConsumeTasks(StreamingCubeInfo cube, int replicaSetNum) {
        List<Partition> partitionsOfCube = cube.getStreamingTableSourceInfo().getPartitions();
        int cubeConsumerTaskNum = getCubeConsumerTasks(cube, replicaSetNum);
        int partitionsPerReceiver = partitionsOfCube.size()/cubeConsumerTaskNum ;
        if (partitionsPerReceiver > 3 && replicaSetNum > cubeConsumerTaskNum) {
            logger.info(
                    "You may consider improve `kylin.stream.cube-num-of-consumer-tasks` because you still some quta left.");
        }
        List<List<Partition>> result = Lists.newArrayListWithCapacity(cubeConsumerTaskNum);
        for (int i = 0; i < cubeConsumerTaskNum; i++) {
            result.add(Lists.<Partition>newArrayList());
        }
        for (int i = 0; i < partitionsOfCube.size(); i++) {
            result.get(i % cubeConsumerTaskNum).add(partitionsOfCube.get(i));
        }
        return result;
    }
}
