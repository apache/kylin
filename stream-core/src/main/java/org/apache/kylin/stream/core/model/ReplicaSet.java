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

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
/**
 * 
 * A ReplicaSet ensures that a specified number of kylin receiver nodes “replicas” are running at any given time to provide HA service.
 *
 */
public class ReplicaSet {
    @JsonProperty("rs_id")
    private int replicaSetID;
    @JsonProperty("nodes")
    private Set<Node> nodes;
    @JsonProperty("leader")
    private Node leader;

    public ReplicaSet() {
        this.nodes = Sets.newHashSet();
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void removeNode(Node node) {
        nodes.remove(node);
    }

    public Set<Node> getNodes() {
        return nodes;
    }

    public void setNodes(Set<Node> nodes) {
        this.nodes = nodes;
    }

    public Node getLeader() {
        return leader;
    }

    public void setLeader(Node leader) {
        this.leader = leader;
    }

    public int getReplicaSetID() {
        return replicaSetID;
    }

    public void setReplicaSetID(int replicaSetID) {
        this.replicaSetID = replicaSetID;
    }

    public boolean containPhysicalNode(Node node) {
        return nodes.contains(node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ReplicaSet that = (ReplicaSet) o;

        return replicaSetID == that.replicaSetID;

    }

    @Override
    public int hashCode() {
        return replicaSetID;
    }

    @Override
    public String toString() {
        return "ReplicaSet{" + "replicaSetID=" + replicaSetID + ", nodes=" + nodes + '}';
    }
}
