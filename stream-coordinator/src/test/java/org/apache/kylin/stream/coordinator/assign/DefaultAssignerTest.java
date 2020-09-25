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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.stream.coordinator.StreamingCubeInfo;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingTableSourceInfo;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class DefaultAssignerTest {
    DefaultAssigner assigner;

    @Before
    public void setup() {
        assigner = new DefaultAssigner();
    }

    @Test
    public void initAssignTest() {
        List<ReplicaSet> rsList = Lists.newArrayList();
        ReplicaSet rs1 = new ReplicaSet();
        rs1.setReplicaSetID(0);
        ReplicaSet rs2 = new ReplicaSet();
        rs2.setReplicaSetID(1);
        ReplicaSet rs3 = new ReplicaSet();
        rs3.setReplicaSetID(2);
        rsList.add(rs1);
        rsList.add(rs2);
        rsList.add(rs3);

        List<CubeAssignment> existingAssignments = Lists.newArrayList();
        Map<Integer, List<Partition>> cube1Assignment = Maps.newHashMap();
        cube1Assignment.put(0, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment1 = new CubeAssignment("cube1", cube1Assignment);

        Map<Integer, List<Partition>> cube2Assignment = Maps.newHashMap();
        cube2Assignment.put(0, Arrays.asList(new Partition(0), new Partition(1)));
        cube2Assignment.put(1, Arrays.asList(new Partition(2)));
        CubeAssignment assignment2 = new CubeAssignment("cube2", cube2Assignment);

        Map<Integer, List<Partition>> cube3Assignment = Maps.newHashMap();
        cube3Assignment.put(2, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment3 = new CubeAssignment("cube3", cube3Assignment);

        Map<Integer, List<Partition>> cube4Assignment = Maps.newHashMap();
        cube4Assignment.put(0, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment4 = new CubeAssignment("cube4", cube4Assignment);

        Map<Integer, List<Partition>> cube5Assignment = Maps.newHashMap();
        cube5Assignment.put(2, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment5 = new CubeAssignment("cube5", cube5Assignment);

        existingAssignments.add(assignment1);
        existingAssignments.add(assignment2);
        existingAssignments.add(assignment3);
        existingAssignments.add(assignment4);
        existingAssignments.add(assignment5);

        StreamingCubeInfo cube5 = new StreamingCubeInfo("cube5", new StreamingTableSourceInfo(Arrays.asList(
                new Partition(0), new Partition(1))), 2);
        CubeAssignment cubeAssignment = assigner.assign(cube5, rsList, existingAssignments);

        assertEquals(cubeAssignment.getAssignments().size(), 2);
        assertTrue(cubeAssignment.getReplicaSetIDs().contains(1));
        assertTrue(cubeAssignment.getReplicaSetIDs().contains(2));

        cube5 = new StreamingCubeInfo("cube5", new StreamingTableSourceInfo(Arrays.asList(new Partition(0),
                new Partition(1))), 3);
        cubeAssignment = assigner.assign(cube5, rsList, existingAssignments);

        assertEquals(cubeAssignment.getAssignments().size(), 2);
        assertTrue(cubeAssignment.getReplicaSetIDs().contains(1));
        assertTrue(cubeAssignment.getReplicaSetIDs().contains(2));

        cube5 = new StreamingCubeInfo("cube5", new StreamingTableSourceInfo(Arrays.asList(new Partition(0),
                new Partition(1), new Partition(2))), 5);
        cubeAssignment = assigner.assign(cube5, rsList, existingAssignments);

        assertEquals(cubeAssignment.getAssignments().size(), 3);
    }

    @Test
    public void reBalanceTest() {
        List<ReplicaSet> rsList = Lists.newArrayList();
        ReplicaSet rs1 = new ReplicaSet();
        rs1.setReplicaSetID(0);
        ReplicaSet rs2 = new ReplicaSet();
        rs2.setReplicaSetID(1);
        ReplicaSet rs3 = new ReplicaSet();
        rs3.setReplicaSetID(2);
        rsList.add(rs1);
        rsList.add(rs2);
        rsList.add(rs3);

        List<CubeAssignment> existingAssignments = Lists.newArrayList();
        Map<Integer, List<Partition>> cube1Assignment = Maps.newHashMap();
        cube1Assignment.put(0, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment1 = new CubeAssignment("cube1", cube1Assignment);

        Map<Integer, List<Partition>> cube2Assignment = Maps.newHashMap();
        cube2Assignment.put(0, Arrays.asList(new Partition(0), new Partition(1)));
        cube2Assignment.put(1, Arrays.asList(new Partition(2)));
        CubeAssignment assignment2 = new CubeAssignment("cube2", cube2Assignment);

        Map<Integer, List<Partition>> cube3Assignment = Maps.newHashMap();
        cube3Assignment.put(2, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment3 = new CubeAssignment("cube3", cube3Assignment);

        Map<Integer, List<Partition>> cube4Assignment = Maps.newHashMap();
        cube4Assignment.put(0, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment4 = new CubeAssignment("cube4", cube4Assignment);

        Map<Integer, List<Partition>> cube5Assignment = Maps.newHashMap();
        cube5Assignment.put(2, Arrays.asList(new Partition(0), new Partition(1), new Partition(2)));
        CubeAssignment assignment5 = new CubeAssignment("cube5", cube5Assignment);

        existingAssignments.add(assignment1);
        existingAssignments.add(assignment2);
        existingAssignments.add(assignment3);
        existingAssignments.add(assignment4);
        existingAssignments.add(assignment5);

        StreamingCubeInfo cube1 = new StreamingCubeInfo("cube1", new StreamingTableSourceInfo(Arrays.asList(
                new Partition(0), new Partition(1), new Partition(2))), 1);
        StreamingCubeInfo cube2 = new StreamingCubeInfo("cube2", new StreamingTableSourceInfo(Arrays.asList(
                new Partition(0), new Partition(1), new Partition(2))), 2);
        StreamingCubeInfo cube3 = new StreamingCubeInfo("cube3", new StreamingTableSourceInfo(Arrays.asList(
                new Partition(0), new Partition(1), new Partition(2))), 1);
        StreamingCubeInfo cube4 = new StreamingCubeInfo("cube4", new StreamingTableSourceInfo(Arrays.asList(
                new Partition(0), new Partition(1), new Partition(2))), 1);
        StreamingCubeInfo cube5 = new StreamingCubeInfo("cube5", new StreamingTableSourceInfo(Arrays.asList(
                new Partition(0), new Partition(1), new Partition(2))), 1);
        List<StreamingCubeInfo> cubes = Arrays.asList(cube1, cube2, cube3, cube4, cube5);
        Map<Integer, Map<String, List<Partition>>> result = assigner.reBalancePlan(rsList, cubes, existingAssignments);
        for (Map<String, List<Partition>> rsAssignment : result.values()) {
            assertEquals(2, rsAssignment.size());
        }

        ReplicaSet rs4 = new ReplicaSet();
        rs4.setReplicaSetID(3);
        ReplicaSet rs5 = new ReplicaSet();
        rs5.setReplicaSetID(4);
        ReplicaSet rs6 = new ReplicaSet();
        rs6.setReplicaSetID(5);
        rsList.add(rs4);
        rsList.add(rs5);
        rsList.add(rs6);
        result = assigner.reBalancePlan(rsList, cubes, existingAssignments);
        for (Map<String, List<Partition>> rsAssignment : result.values()) {
            assertEquals(1, rsAssignment.size());
        }

    }
}
