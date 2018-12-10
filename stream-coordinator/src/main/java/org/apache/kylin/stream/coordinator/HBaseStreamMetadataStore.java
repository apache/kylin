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

package org.apache.kylin.stream.coordinator;

import java.util.List;
import java.util.Map;

import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.SegmentBuildState;
import org.apache.kylin.stream.core.model.StreamingCubeConsumeState;
import org.apache.kylin.stream.core.source.Partition;

//TODO implement this later
public class HBaseStreamMetadataStore implements StreamMetadataStore {

    @Override
    public List<Node> getReceivers() {
        return null;
    }

    @Override
    public List<String> getCubes() {
        return null;
    }

    @Override
    public void addStreamingCube(String cubeName) {

    }

    @Override
    public void removeStreamingCube(String cubeName) {

    }

    @Override
    public StreamingCubeConsumeState getStreamingCubeConsumeState(String cubeName) {
        return null;
    }

    @Override
    public void saveStreamingCubeConsumeState(String cubeName, StreamingCubeConsumeState state) {

    }

    @Override
    public void addReceiver(Node receiver) {

    }

    @Override
    public void removeReceiver(Node receiver) {

    }

    @Override
    public void removeCubeAssignment(String cubeName) {

    }

    @Override
    public void saveNewCubeAssignment(CubeAssignment newCubeAssignment) {

    }

    @Override
    public List<CubeAssignment> getAllCubeAssignments() {
        return null;
    }

    @Override
    public Map<Integer, Map<String, List<Partition>>> getAllReplicaSetAssignments() {
        return null;
    }

    @Override
    public Map<String, List<Partition>> getAssignmentsByReplicaSet(int replicaSetID) {
        return null;
    }

    @Override
    public CubeAssignment getAssignmentsByCube(String cubeName) {
        return null;
    }

    @Override
    public List<ReplicaSet> getReplicaSets() {
        return null;
    }

    @Override
    public List<Integer> getReplicaSetIDs() {
        return null;
    }

    @Override
    public ReplicaSet getReplicaSet(int rsID) {
        return null;
    }

    @Override
    public void removeReplicaSet(int rsID) {

    }

    @Override
    public int createReplicaSet(ReplicaSet rs) {
        return -1;
    }

    @Override
    public void updateReplicaSet(ReplicaSet rs) {

    }

    @Override
    public Node getCoordinatorNode() {
        return null;
    }

    @Override
    public void setCoordinatorNode(Node coordinator) {
    }

    @Override
    public void saveSourceCheckpoint(String cubeName, String segmentName, int rsID, String sourceCheckpoint) {

    }

    @Override
    public Map<Integer, String> getSourceCheckpoint(String cubeName, String segmentName) {
        return null;
    }

    @Override
    public void addCompleteReplicaSetForSegmentBuild(String cubeName, String segmentName, int rsID) {

    }

    @Override
    public void updateSegmentBuildState(String cubeName, String segmentName, SegmentBuildState.BuildState state) {

    }

    @Override
    public List<SegmentBuildState> getSegmentBuildStates(String cubeName) {
        return null;
    }

    @Override
    public SegmentBuildState getSegmentBuildState(String cubeName, String segmentName) {
        return null;
    }

    @Override
    public boolean removeSegmentBuildState(String cubeName, String segmentName) {
        return true;
    }

}
