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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kylin.stream.coordinator.StreamingCubeInfo;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingTableSourceInfo;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class CubePartitionRoundRobinAssignerTest {
    CubePartitionRoundRobinAssigner assigner;

    @Before
    public void setup() {
        assigner = new CubePartitionRoundRobinAssigner();
    }

    @Test
    public void initBalanceTest() {
        List<Node> receivers = Lists.newArrayList();
        Node node1 = new Node("host1", 9090);
        Node node2 = new Node("host2", 9090);
        Node node3 = new Node("host3", 9090);
        receivers.add(node1);
        receivers.add(node2);
        receivers.add(node3);

        List<ReplicaSet> replicaSets = Lists.newArrayList();
        ReplicaSet rs1 = new ReplicaSet();
        rs1.setReplicaSetID(1);
        ReplicaSet rs2 = new ReplicaSet();
        rs2.setReplicaSetID(2);
        ReplicaSet rs3 = new ReplicaSet();
        rs3.setReplicaSetID(3);
        replicaSets.add(rs1);
        replicaSets.add(rs2);
        replicaSets.add(rs3);

        List<StreamingCubeInfo> cubeInfos = Lists.newArrayList();

        createCubeInfoFromMeta("cube1", 3, cubeInfos);
        createCubeInfoFromMeta("cube2", 4, cubeInfos);
        createCubeInfoFromMeta("cube3", 2, cubeInfos);
        createCubeInfoFromMeta("cube4", 1, cubeInfos);
        createCubeInfoFromMeta("cube5", 7, cubeInfos);

        List<CubeAssignment> existingAssignments = Lists.newArrayList();
        Map<Integer, Map<String, List<Partition>>> assignmentMap = assigner.reBalancePlan(replicaSets, cubeInfos,
                existingAssignments);

        Map<String, List<Partition>> node1Assignments = assignmentMap.get(rs1.getReplicaSetID());
        int node1PartitionCnt = calCubePartitionCnt(node1Assignments.values());
        assertTrue(node1PartitionCnt == 6 || node1PartitionCnt == 5);

        Map<String, List<Partition>> node2Assignments = assignmentMap.get(rs2.getReplicaSetID());
        int node2PartitionCnt = calCubePartitionCnt(node2Assignments.values());
        assertTrue(node2PartitionCnt == 6 || node2PartitionCnt == 5);

        Map<String, List<Partition>> node3Assignments = assignmentMap.get(rs3.getReplicaSetID());

        int node3PartitionCnt = calCubePartitionCnt(node3Assignments.values());
        assertTrue(node3PartitionCnt == 6 || node3PartitionCnt == 5);
        assertEquals(17, node1PartitionCnt + node2PartitionCnt + node3PartitionCnt);
    }

    @Test
    public void reBalanceTest2() {
        List<Node> receivers = Lists.newArrayList();
        Node node1 = new Node("host1", 9090);
        Node node2 = new Node("host2", 9090);
        Node node3 = new Node("host3", 9090);
        receivers.add(node1);
        receivers.add(node2);
        receivers.add(node3);

        List<ReplicaSet> replicaSets = Lists.newArrayList();
        ReplicaSet rs1 = new ReplicaSet();
        rs1.setReplicaSetID(1);
        ReplicaSet rs2 = new ReplicaSet();
        rs2.setReplicaSetID(2);
        ReplicaSet rs3 = new ReplicaSet();
        rs3.setReplicaSetID(3);
        replicaSets.add(rs1);
        replicaSets.add(rs2);
        replicaSets.add(rs3);

        List<StreamingCubeInfo> cubeInfos = Lists.newArrayList();
        createCubeInfoFromMeta("cube1", 3, cubeInfos);
        createCubeInfoFromMeta("cube2", 4, cubeInfos);
        createCubeInfoFromMeta("cube3", 2, cubeInfos);
        createCubeInfoFromMeta("cube4", 1, cubeInfos);
        createCubeInfoFromMeta("cube5", 7, cubeInfos);

        List<CubeAssignment> existingAssignments = Lists.newArrayList();
        //Cube1 has 3 partitions
        Map<Integer, List<Partition>> cube1Assignment = Maps.newHashMap();
        cube1Assignment.put(1, Arrays.asList(new Partition(1), new Partition(3)));
        cube1Assignment.put(2, Arrays.asList(new Partition(2)));
        existingAssignments.add(new CubeAssignment("cube1", cube1Assignment));

        //Cube2 has 4 partitions
        Map<Integer, List<Partition>> cube2Assignment = Maps.newHashMap();
        cube2Assignment.put(1, Arrays.asList(new Partition(2), new Partition(4)));
        cube2Assignment.put(2, Arrays.asList(new Partition(1), new Partition(3)));
        existingAssignments.add(new CubeAssignment("cube2", cube2Assignment));

        //Cube3 has 2 partitions
        Map<Integer, List<Partition>> cube3Assignment = Maps.newHashMap();
        cube3Assignment.put(1, Arrays.asList(new Partition(2)));
        cube3Assignment.put(2, Arrays.asList(new Partition(1)));
        existingAssignments.add(new CubeAssignment("cube3", cube3Assignment));

        //Cube4 has 1 partition
        Map<Integer, List<Partition>> cube4Assignment = Maps.newHashMap();
        cube4Assignment.put(2, Arrays.asList(new Partition(1)));
        existingAssignments.add(new CubeAssignment("cube4", cube4Assignment));

        //Cube5 has 7 partitions
        Map<Integer, List<Partition>> cube5Assignment = Maps.newHashMap();
        cube5Assignment.put(1, Arrays.asList(new Partition(1), new Partition(3), new Partition(5), new Partition(7)));
        cube5Assignment.put(2, Arrays.asList(new Partition(2), new Partition(4), new Partition(6)));
        existingAssignments.add(new CubeAssignment("cube5", cube5Assignment));

        Map<Integer, Map<String, List<Partition>>> assignmentMap = assigner.reBalancePlan(replicaSets, cubeInfos,
                existingAssignments);

        Map<String, List<Partition>> node1Assignments = assignmentMap.get(rs1.getReplicaSetID());
        int node1PartitionCnt = calCubePartitionCnt(node1Assignments.values());
        assertTrue(node1PartitionCnt == 6 || node1PartitionCnt == 5);

        Map<String, List<Partition>> node2Assignments = assignmentMap.get(rs2.getReplicaSetID());
        int node2PartitionCnt = calCubePartitionCnt(node2Assignments.values());
        assertTrue(node2PartitionCnt == 6 || node2PartitionCnt == 5);

        Map<String, List<Partition>> node3Assignments = assignmentMap.get(rs3.getReplicaSetID());
        int node3PartitionCnt = calCubePartitionCnt(node3Assignments.values());
        assertTrue(node3PartitionCnt == 6 || node3PartitionCnt == 5);

        assertEquals(17, node1PartitionCnt + node2PartitionCnt + node3PartitionCnt);

    }

    @Test
    public void assignCubeTest() {
        List<ReplicaSet> replicaSets = Lists.newArrayList();
        ReplicaSet rs1 = new ReplicaSet();
        rs1.setReplicaSetID(1);
        ReplicaSet rs2 = new ReplicaSet();
        rs2.setReplicaSetID(2);
        ReplicaSet rs3 = new ReplicaSet();
        rs3.setReplicaSetID(3);
        replicaSets.add(rs1);
        replicaSets.add(rs2);
        replicaSets.add(rs3);

        List<StreamingCubeInfo> cubeInfos = Lists.newArrayList();
        createCubeInfoFromMeta("cube5", 7, cubeInfos);
        StreamingCubeInfo cubeInfo = cubeInfos.get(0);

        List<CubeAssignment> existingAssignments = Lists.newArrayList();

        CubeAssignment cubeAssignment = assigner.assign(cubeInfo, replicaSets, existingAssignments);
        assertEquals(cubeAssignment.getAssignments().get(rs1.getReplicaSetID()).size(), 3);
        assertEquals(cubeAssignment.getAssignments().get(rs2.getReplicaSetID()).size(), 2);
        assertEquals(cubeAssignment.getAssignments().get(rs3.getReplicaSetID()).size(), 2);
    }

    @Test
    public void testExpandAndIntersectCubePartitions() {
        Map<String, List<Partition>> nodeAssignment = Maps.newHashMap();
        nodeAssignment.put("cube1", Arrays.asList(new Partition(1), new Partition(3)));
        nodeAssignment.put("cube2", Arrays.asList(new Partition(2), new Partition(4)));
        nodeAssignment.put("cube3", Arrays.asList(new Partition(2)));
        nodeAssignment.put("cube5",
                Arrays.asList(new Partition(1), new Partition(3), new Partition(5), new Partition(7)));
        List<CubePartitionRoundRobinAssigner.CubePartition> cubePartitions = assigner
                .expandAndIntersectCubePartitions(nodeAssignment);
        System.out.println(cubePartitions);
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

    @Test
    public void testSplit() {
        int calculatorNum = 8;
        long[] cuboidIds = new long[2523];
        for (int i = 0; i < 2523; i++) {
            cuboidIds[i] = i;
        }
        int splitSize = cuboidIds.length / calculatorNum;
        if (splitSize <= 0) {
            splitSize = 1;
        }
        for (int i = 0; i < calculatorNum; i++) {
            long[] cuboidIdSplit;
            int start = i * splitSize;
            int end = (i + 1) * splitSize;
            if (i == calculatorNum - 1) {
                end = cuboidIds.length;
            }
            if (start > cuboidIds.length) {
                break;
            }
            cuboidIdSplit = Arrays.copyOfRange(cuboidIds, start, end);
            StringBuilder sb = new StringBuilder();
            for (long l : cuboidIdSplit) {
                sb.append(l + ",");
            }
            System.out.println(i + ":" + sb.toString());
        }

    }

    private void createCubeInfoFromMeta(String cubeName, int partitionNum, List<StreamingCubeInfo> cubeInfos) {
        List<Partition> partitions = Lists.newArrayList();
        for (int i = 1; i <= partitionNum; i++) {
            partitions.add(new Partition(i));
        }
        StreamingCubeInfo cubeInfo = new StreamingCubeInfo(cubeName, new StreamingTableSourceInfo(partitions), 1);
        cubeInfos.add(cubeInfo);
    }

}
