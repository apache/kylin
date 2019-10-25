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

import java.util.HashSet;
import java.util.Set;

public class SegmentBuildState implements Comparable<SegmentBuildState> {

    private String segmentName;
    private Set<Integer> completeReplicaSets = new HashSet<>();
    private BuildState state;

    public SegmentBuildState(String segmentName) {
        this.segmentName = segmentName;
    }

    public Set<Integer> getCompleteReplicaSets() {
        return completeReplicaSets;
    }

    public void setCompleteReplicaSets(Set<Integer> completeReplicaSets) {
        this.completeReplicaSets = completeReplicaSets;
    }

    public void addCompleteReplicaSet(int replicaSetID) {
        this.completeReplicaSets.add(replicaSetID);
    }

    public BuildState getState() {
        return state;
    }

    public void setState(BuildState state) {
        this.state = state;
    }

    public boolean isInBuilding() {
        if (state == null) {
            return false;
        }

        return BuildState.State.BUILDING.equals(state.getState());
    }

    public boolean isInWaiting() {
        return state == null || BuildState.State.WAIT.equals(state.getState());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SegmentBuildState that = (SegmentBuildState) o;

        if (segmentName != null ? !segmentName.equals(that.segmentName) : that.segmentName != null)
            return false;
        if (completeReplicaSets != null ? !completeReplicaSets.equals(that.completeReplicaSets)
                : that.completeReplicaSets != null)
            return false;
        return state != null ? state.equals(that.state) : that.state == null;

    }

    @Override
    public int hashCode() {
        int result = segmentName != null ? segmentName.hashCode() : 0;
        result = 31 * result + (completeReplicaSets != null ? completeReplicaSets.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SegmentBuildState{" + "segmentName='" + segmentName + '\'' + ", completeReplicaSets="
                + completeReplicaSets + ", state=" + state + '}';
    }

    @Override
    public int compareTo(SegmentBuildState o) {
        return this.segmentName.compareTo(o.segmentName);
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public static class BuildState {
        private State state = State.WAIT;
        private long buildStartTime;
        private String jobId;

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        public long getBuildStartTime() {
            return buildStartTime;
        }

        public void setBuildStartTime(long buildStartTime) {
            this.buildStartTime = buildStartTime;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public enum State {
            WAIT, BUILDING
        }
    }
}
