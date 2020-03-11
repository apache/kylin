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

package org.apache.kylin.stream.core.model.stats;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ClusterState {
    @JsonProperty("last_update_time")
    private long lastUpdateTime;

    @JsonProperty("rs_states")
    private List<ReplicaSetState> replicaSetStates = Lists.newArrayList();

    /**
     * Receiver which don't belong to any replica set.
     */
    @JsonProperty("available_receivers")
    private List<ReceiverState> availableReceivers = Lists.newArrayList();

    @SuppressWarnings("unused")
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @SuppressWarnings("unused")
    public List<ReplicaSetState> getReplicaSetStates() {
        return replicaSetStates;
    }

    @SuppressWarnings("unused")
    public void setReplicaSetStates(List<ReplicaSetState> replicaSetStates) {
        this.replicaSetStates = replicaSetStates;
    }

    @SuppressWarnings("unused")
    public List<ReceiverState> getAvailableReceivers() {
        return availableReceivers;
    }

    @SuppressWarnings("unused")
    public void setAvailableReceivers(List<ReceiverState> availableReceivers) {
        this.availableReceivers = availableReceivers;
    }

    public void addReplicaSetState(ReplicaSetState replicaSetState) {
        if (replicaSetStates == null) {
            replicaSetStates = Lists.newArrayList();
        }
        replicaSetStates.add(replicaSetState);
    }

    public void addAvailableReveiverState(ReceiverState receiverState) {
        if (availableReceivers == null) {
            availableReceivers = Lists.newArrayList();
        }
        availableReceivers.add(receiverState);
    }
}
