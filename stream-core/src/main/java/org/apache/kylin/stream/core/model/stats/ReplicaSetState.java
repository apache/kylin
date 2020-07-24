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
import java.util.Map;

import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.source.Partition;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ReplicaSetState {
    @JsonProperty("rs_id")
    private int rsID;

    @JsonProperty("lead")
    private Node lead;

    @JsonProperty("assignment")
    private Map<String, List<Partition>> assignment;

    @JsonProperty("receiver_states")
    private List<ReceiverState> receiverStates;

    public int getRsID() {
        return rsID;
    }

    public void setRsID(int rsID) {
        this.rsID = rsID;
    }

    @SuppressWarnings("unused")
    public List<ReceiverState> getReceiverStates() {
        return receiverStates;
    }

    @SuppressWarnings("unused")
    public void setReceiverStates(List<ReceiverState> receiverStates) {
        this.receiverStates = receiverStates;
    }

    public Node getLead() {
        return lead;
    }

    public void setLead(Node lead) {
        this.lead = lead;
    }

    public Map<String, List<Partition>> getAssignment() {
        return assignment;
    }

    public void setAssignment(Map<String, List<Partition>> assignment) {
        this.assignment = assignment;
    }

    public void addReveiverState(ReceiverState receiverState) {
        if (receiverStates == null) {
            receiverStates = Lists.newArrayList();
        }
        receiverStates.add(receiverState);
    }

    @SuppressWarnings("unused")
    public ReceiverState getReceiverState(Node receiver) {
        for (ReceiverState receiverState : receiverStates) {
            if (receiverState.getReceiver().equals(receiver)) {
                return receiverState;
            }
        }
        return null;
    }
}
