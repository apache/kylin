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

package org.apache.kylin.stream.core.client;

import java.io.IOException;

import org.apache.kylin.stream.core.model.AssignRequest;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.HealthCheckInfo;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ResumeConsumerRequest;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.model.UnAssignRequest;
import org.apache.kylin.stream.core.model.stats.ReceiverCubeStats;
import org.apache.kylin.stream.core.model.stats.ReceiverStats;

/**
 * StreamingCoordinator send admin request to speicifc receiver
 * (received by org.apache.kylin.stream.server.rest.controller.AdminController).
 */
public interface ReceiverAdminClient {

    /**
     * Notify receiver that it has been assign to consumption task with AssignRequest#partitions of cube.
     * If AssignRequest#startConsumers is set to true, receiver has to start consumption at once.
     */
    void assign(Node receiver, AssignRequest assignRequest) throws IOException;

    /**
     * <pre>
     * Notify receiver that it has been unassign to specific cube.
     * Receiver will stop consumption and delete all local segment cache.
     * </pre>
     */
    void unAssign(Node receiver, UnAssignRequest unAssignRequest) throws IOException;

    /**
     * <pre>
     * Ask receiver to start consumption (create IStreamingConnector).
     *
     * Start position is decided by StartConsumersRequest#startProtocol:
     * 1. when StartConsumersRequest#startProtocol is null, cosume from checkpoint:
     *     1. if local checkpoint exists, cosume from local checkpoint
     *     2. if local checkpoint not exists, cosume from remote checkpoint(see StreamMetadata#getSourceCheckpoint)
     *     3. if both not exists,  start position decided by KylinConfig#isStreamingConsumeFromLatestOffsets
     * 2. when StartConsumersRequest#startProtocol is not null:
     *     1. when startProtocol.getStartPosition() is vaild, cosume from startProtocol.getStartPosition()
     *     2. when startProtocol.getStartPosition() is not vaild, start position decided by StartProtocol#ConsumerStartMode
     *
     * See KafkaSource#createStreamingConnector
     * </pre>
     */
    void startConsumers(Node receiver, StartConsumersRequest startRequest) throws IOException;

    /**
     * <pre>
     * Ask receiver to stop consumption (destroy IStreamingConnector) and flush data into disk.
     * If StopConsumersRequest#removeData set to true, all segment data will be deleted.
     * </pre>
     */
    ConsumerStatsResponse stopConsumers(Node receiver, StopConsumersRequest stopRequest) throws IOException;

    /**
     * Ask receiver to pause consumption (don't destroy IStreamingConnector).
     */
    ConsumerStatsResponse pauseConsumers(Node receiver, PauseConsumersRequest request) throws IOException;

    /**
     * <pre>
     * 1. When ResumeConsumerRequest#resumeToPosition is null, just ask receiver to resume consumption.
     * 2. When ResumeConsumerRequest#resumeToPosition is not null, ask receiver to resume to that position and stop consumption,
     *  so it is something like ReceiverAdminClient#stopConsumers. This case is used in reAssign action.
     *  Please check ReceiverClusterManager#syncAndStopConsumersInRs for detail.
     *
     *  It is a synchronous method.
     * </pre>
     */
    ConsumerStatsResponse resumeConsumers(Node receiver, ResumeConsumerRequest request) throws IOException;

    /**
     * Ask receiver to remove all data related to specific cube in receiver side.
     */
    void removeCubeSegment(Node receiver, String cubeName, String segmentName) throws IOException;

    /**
     * Ask receiver to stop consumption and convert all segments to Immutable.
     *
     * If a replica set is removed from consumption task, coordinator will notify
     *    its receivers and ask them to upload all data asap.
     * Often happend in reassign action.
     */
    void makeCubeImmutable(Node receiver, String cubeName) throws IOException;

    /**
     * When a segment has been promoted to HBase Ready Segment(historical part),
     *  segment cache in receiver(realtime part) is useless and need to be deleted.
     */
    void segmentBuildComplete(Node receiver, String cubeName, String segmentName) throws IOException;

    /**
     * <pre>
     * Notify receiver that it has been added into a new replica set, recevier will do
     *  1. add itself the replica set's leader candidate
     *  2. fetch assignment from Metadata and try start consumption task
     * </pre>
     */
    void addToReplicaSet(Node receiver, int replicaSetID) throws IOException;

    /**
     * <pre>
     * Notify receiver that it has been removed from replica set, recevier will do
     *  1. remove assigment and remove itself the replica set's leader candidate
     *  2. stop consumption
     *  3. remove all local segment cache
     * </pre>
     */
    void removeFromReplicaSet(Node receiver) throws IOException;

    ReceiverStats getReceiverStats(Node receiver) throws IOException;

    ReceiverCubeStats getReceiverCubeStats(Node receiver, String cubeName) throws IOException;

    HealthCheckInfo healthCheck(Node receiver) throws IOException;
}
