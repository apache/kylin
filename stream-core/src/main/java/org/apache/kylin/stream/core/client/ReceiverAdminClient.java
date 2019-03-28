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

public interface ReceiverAdminClient {
    void assign(Node receiver, AssignRequest assignRequest) throws IOException;

    void unAssign(Node receiver, UnAssignRequest unAssignRequest) throws IOException;

    void startConsumers(Node receiver, StartConsumersRequest startRequest) throws IOException;

    ConsumerStatsResponse stopConsumers(Node receiver, StopConsumersRequest stopRequest) throws IOException;

    ConsumerStatsResponse pauseConsumers(Node receiver, PauseConsumersRequest request) throws IOException;

    ConsumerStatsResponse resumeConsumers(Node receiver, ResumeConsumerRequest request) throws IOException;

    void removeCubeSegment(Node receiver, String cubeName, String segmentName) throws IOException;

    void makeCubeImmutable(Node receiver, String cubeName) throws IOException;

    void segmentBuildComplete(Node receiver, String cubeName, String segmentName) throws IOException;

    void addToReplicaSet(Node receiver, int replicaSetID) throws IOException;

    void removeFromReplicaSet(Node receiver) throws IOException;

    ReceiverStats getReceiverStats(Node receiver) throws IOException;

    ReceiverCubeStats getReceiverCubeStats(Node receiver, String cubeName) throws IOException;

    HealthCheckInfo healthCheck(Node receiver) throws IOException;
}
