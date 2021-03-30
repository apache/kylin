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

package org.apache.kylin.stream.core.model;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.stream.core.source.Partition;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * Represents the Kylin streaming cube assignment.
 *
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class CubeAssignment {
    @JsonProperty("cube_name")
    private String cubeName;

    @JsonProperty("assignments")
    // Map between the replicaSet Id and partitions
    private Map<Integer, List<Partition>> assignments;

    // Map between partition Id and replicaSet Id
    private Map<Integer, Integer> partitionReplicaSetIDMap;

    @JsonCreator
    public CubeAssignment(@JsonProperty("cube_name") String cubeName,
            @JsonProperty("assignments") Map<Integer, List<Partition>> assignments) {
        this.cubeName = cubeName;
        this.assignments = assignments;
        this.partitionReplicaSetIDMap = Maps.newHashMap();
        for (Map.Entry<Integer, List<Partition>> assignEntry : assignments.entrySet()) {
            for (Partition partition : assignEntry.getValue()) {
                partitionReplicaSetIDMap.put(partition.getPartitionId(), assignEntry.getKey());
            }
        }
    }

    public static byte[] serializeCubeAssignment(CubeAssignment cubeAssignment) throws IOException {
        return JsonUtil.writeValueAsBytes(cubeAssignment);
    }

    public static CubeAssignment deserializeCubeAssignment(byte[] assignmentData) throws IOException {
        return JsonUtil.readValue(assignmentData, CubeAssignment.class);
    }

    public String getCubeName() {
        return cubeName;
    }

    public Map<Integer, List<Partition>> getAssignments() {
        return assignments;
    }

    public Set<Integer> getReplicaSetIDs() {
        return assignments.keySet();
    }

    public Set<Integer> getPartitionIDs() {
        return partitionReplicaSetIDMap.keySet();
    }

    public List<Partition> getPartitionsByReplicaSetID(Integer replicaSetID) {
        List<Partition> result = assignments.get(replicaSetID);
        if (result == null) {
            result = Lists.newArrayList();
        }
        return result;
    }

    public void addAssignment(Integer replicaSetID, List<Partition> partitions) {
        assignments.put(replicaSetID, partitions);
    }

    public void removeAssignment(Integer replicaSetID) {
        assignments.remove(replicaSetID);
    }

    public Integer getReplicaSetIDByPartition(Integer partitionID) {
        return partitionReplicaSetIDMap.get(partitionID);
    }

    public Integer getPartitionNum() {
        return partitionReplicaSetIDMap.size();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((assignments == null) ? 0 : assignments.hashCode());
        result = prime * result + ((cubeName == null) ? 0 : cubeName.hashCode());
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
        CubeAssignment other = (CubeAssignment) obj;
        if (assignments == null) {
            if (other.assignments != null)
                return false;
        } else if (!assignments.equals(other.assignments))
            return false;
        if (cubeName == null) {
            if (other.cubeName != null)
                return false;
        } else if (!cubeName.equals(other.cubeName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CubeAssignment [cubeName=" + cubeName + ", assignments=" + assignments + "]";
    }
}
