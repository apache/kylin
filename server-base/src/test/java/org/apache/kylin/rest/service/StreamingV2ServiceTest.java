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

import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.source.Partition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class StreamingV2ServiceTest {

    @Test
    public void testValidateAssignment() {
        Map<Integer, List<Partition>> assignmentMap = new HashMap<>();
        Partition p1 = new Partition(1);
        Partition p2 = new Partition(2);
        Partition p3 = new Partition(3);
        List<Integer> replicaSets = new ArrayList<>();
        replicaSets.add(0);
        replicaSets.add(1);
        List<Partition> l1 = new ArrayList<>();
        l1.add(p1);
        l1.add(p2);
        List<Partition> l2 = new ArrayList<>();
        l2.add(p3);
        assignmentMap.put(0, l1);
        assignmentMap.put(1, l2);

        CubeAssignment cubeAssignment = new CubeAssignment("test", assignmentMap);
        StreamMetadataStore metadataStore = mock(StreamMetadataStore.class);
        when(metadataStore.getReplicaSetIDs()).thenReturn(replicaSets);
        StreamingV2Service streamingV2Service = new StreamingV2Service(metadataStore, null);
        Exception exception = null;

        // normal case
        streamingV2Service.validateAssignment(cubeAssignment);

        // bad case 1
        l2.add(p2);
        try {
            streamingV2Service.validateAssignment(cubeAssignment);
        } catch (IllegalArgumentException ill) {
            exception = ill;
            ill.printStackTrace();
        }
        Assert.assertNotNull("Intersection detected between : 0 with 1", exception);

        // bad case 2
        l2.clear();
        exception = null;
        try {
            streamingV2Service.validateAssignment(cubeAssignment);
        } catch (IllegalArgumentException ill) {
            exception = ill;
            ill.printStackTrace();
        }
        Assert.assertNotNull("PartitionList is empty", exception);
    }
}
