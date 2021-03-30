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

package org.apache.kylin.stream.coordinator.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.RemoteStoreCompleteRequest;
import org.apache.kylin.stream.core.model.ReplicaSetLeaderChangeRequest;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.util.RestService;
import org.apache.kylin.stream.core.util.RetryCallable;
import org.apache.kylin.stream.core.util.RetryCaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCoordinatorClient implements CoordinatorClient {
    private static final Logger logger = LoggerFactory.getLogger(HttpCoordinatorClient.class);

    private static final String CUBES = "/cubes/";
    private StreamMetadataStore streamMetadataStore;
    private RestService restService;
    private Node coordinatorNode;
    private RetryCaller retryCaller;

    public HttpCoordinatorClient(StreamMetadataStore metadataStore) {
        this.streamMetadataStore = metadataStore;
        this.coordinatorNode = metadataStore.getCoordinatorNode();
        int maxRetry = 10;

        this.retryCaller = new RetryCaller(maxRetry, 1000);
        int connectionTimeout = KylinConfig.getInstanceFromEnv().getCoordinatorHttpClientTimeout();
        int readTimeout = 10000;
        this.restService = new RestService(connectionTimeout, readTimeout);
    }

    @Override
    public void segmentRemoteStoreComplete(Node receiverNode, String cubeName, Pair<Long, Long> segmentRange) {
        logger.info("send receiver remote store complete message to coordinator");
        try {
            RemoteStoreCompleteRequest completeRequest = new RemoteStoreCompleteRequest();
            completeRequest.setCubeName(cubeName);
            completeRequest.setReceiverNode(receiverNode);
            completeRequest.setSegmentStart(segmentRange.getFirst());
            completeRequest.setSegmentEnd(segmentRange.getSecond());

            String content = JsonUtil.writeValueAsIndentString(completeRequest);
            postRequest("/remoteStoreComplete", content);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void replicaSetLeaderChange(int replicaSetId, Node newLeader) {
        logger.info("send replicaSet lead change notification to coordinator");
        try {
            ReplicaSetLeaderChangeRequest changeRequest = new ReplicaSetLeaderChangeRequest();
            changeRequest.setReplicaSetID(replicaSetId);
            changeRequest.setNewLeader(newLeader);
            String content = JsonUtil.writeValueAsIndentString(changeRequest);
            postRequest("/replicaSetLeaderChange", content);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public Map<Integer, Map<String, List<Partition>>> reBalanceRecommend() {
        logger.info("send reBalance recommend request to coordinator");
        try {
            Object response = getRequest("/balance/recommend");
            return (Map) response;
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void reBalance(Map<Integer, Map<String, List<Partition>>> reBalancePlan) {
        logger.info("send reBalance request to coordinator");
        try {
            String content = JsonUtil.writeValueAsIndentString(reBalancePlan);
            postRequest("/balance", content);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void assignCube(String cubeName) {
        logger.info("send assign request to coordinator");
        try {
            putRequest(CUBES + cubeName + "/assign");
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void unAssignCube(String cubeName) {
        logger.info("send unAssign request to coordinator");
        try {
            putRequest(CUBES + cubeName + "/unAssign");
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void reAssignCube(String cubeName, CubeAssignment newAssignments) {
        logger.info("send reassign request to coordinator");
        try {
            String path = CUBES + cubeName + "/reAssign";
            String content = JsonUtil.writeValueAsIndentString(newAssignments);
            postRequest(path, content);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void createReplicaSet(ReplicaSet rs) {
        logger.info("send create replicaSet request to coordinator");
        try {
            String path = "/replicaSet";
            String content = JsonUtil.writeValueAsIndentString(rs);
            postRequest(path, content);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void removeReplicaSet(int rsID) {
        logger.info("send remove replicaSet request to coordinator");
        try {
            String path = "/replicaSet/" + rsID;
            deleteRequest(path);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void addNodeToReplicaSet(Integer replicaSetID, String nodeID) {
        logger.info("send add node to replicaSet request to coordinator");
        try {
            String path = "/replicaSet/" + replicaSetID + "/" + nodeID;
            putRequest(path);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void removeNodeFromReplicaSet(Integer replicaSetID, String nodeID) {
        logger.info("send remove node from replicaSet request to coordinator");
        try {
            String path = "/replicaSet/" + replicaSetID + "/" + nodeID;
            deleteRequest(path);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void pauseConsumers(String cubeName) {
        logger.info("send cube pause request to coordinator: {}", cubeName);
        try {
            String path = CUBES + cubeName + "/pauseConsume";
            putRequest(path);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public void resumeConsumers(String cubeName) {
        logger.info("send cube resume request to coordinator: {}", cubeName);
        try {
            String path = CUBES + cubeName + "/resumeConsume";
            putRequest(path);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    private Object postRequest(final String path, final String requestContent) throws IOException {
        final String url = getBaseUrl() + path;
        CoordinatorResponse response = retryCaller.call(new CoordinatorRetryCallable() {
            @Override
            public CoordinatorResponse call() throws Exception {
                String msg = restService.postRequest(url, requestContent);
                return JsonUtil.readValue(msg, CoordinatorResponse.class);
            }
        });
        return response.getData();
    }

    private Object getRequest(String path) throws IOException {
        final String url = getBaseUrl() + path;
        CoordinatorResponse response = retryCaller.call(new CoordinatorRetryCallable() {
            @Override
            public CoordinatorResponse call() throws Exception {
                String msg = restService.getRequest(url);
                return JsonUtil.readValue(msg, CoordinatorResponse.class);
            }
        });
        return response.getData();
    }

    private Object putRequest(String path) throws IOException {
        final String url = getBaseUrl() + path;
        CoordinatorResponse response = retryCaller.call(new CoordinatorRetryCallable() {
            @Override
            public CoordinatorResponse call() throws Exception {
                String msg = restService.putRequest(url);
                return JsonUtil.readValue(msg, CoordinatorResponse.class);
            }
        });
        return response.getData();
    }

    private Object deleteRequest(String path) throws IOException {
        final String url = getBaseUrl() + path;
        CoordinatorResponse response = retryCaller.call(new CoordinatorRetryCallable() {
            @Override
            public CoordinatorResponse call() throws Exception {
                String msg = restService.deleteRequest(url);
                return JsonUtil.readValue(msg, CoordinatorResponse.class);
            }
        });
        return response.getData();
    }

    private String getBaseUrl() {
        Node coordinatorNode = getCoordinator();
        return "http://" + coordinatorNode.getHost() + ":" + coordinatorNode.getPort()
                + "/kylin/api/streaming_coordinator";
    }

    private void updateCoordinatorCache() {
        this.coordinatorNode = streamMetadataStore.getCoordinatorNode();
    }

    private Node getCoordinator() {
        return coordinatorNode;
    }

    private abstract class CoordinatorRetryCallable implements RetryCallable<CoordinatorResponse> {

        @Override
        public boolean isResultExpected(CoordinatorResponse result) {
            try {
                if (result.getCode() == CoordinatorResponse.SUCCESS) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                logger.error("result is not expected", e);
                return false;
            }
        }

        @Override
        public void update() {
            updateCoordinatorCache();
        }
    }

}
