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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
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
import org.apache.kylin.stream.core.util.RestService;
import org.apache.kylin.stream.core.util.RetryCallable;
import org.apache.kylin.stream.core.util.RetryCaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpReceiverAdminClient implements ReceiverAdminClient {
    private static final Logger logger = LoggerFactory.getLogger(HttpReceiverAdminClient.class);
    private RestService restService;
    private int maxRetry;
    private int retryPauseTime;
    private RetryCaller retryCaller;

    public HttpReceiverAdminClient() {
        int connectionTimeout = KylinConfig.getInstanceFromEnv().getReceiverHttpClientTimeout();
        int readTimeout = 30000;
        this.maxRetry = 3;
        this.retryPauseTime = 1000;
        this.retryCaller = new RetryCaller(maxRetry, retryPauseTime);
        this.restService = new RestService(connectionTimeout, readTimeout);
    }

    @Override
    public void assign(Node receiver, AssignRequest assignRequest) throws IOException {
        logger.info("send assign request:{} to receiver:{}", assignRequest, receiver);
        final String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/assign";
        final String content = JsonUtil.writeValueAsString(assignRequest);

        retryPostRequest(url, content);
    }

    @Override
    public void unAssign(Node receiver, UnAssignRequest unAssignRequest) throws IOException {
        logger.info("send unAssign request:{} to receiver:{}", unAssignRequest, receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/unAssign";
        String content = JsonUtil.writeValueAsString(unAssignRequest);

        retryPostRequest(url, content);
    }

    @Override
    public void startConsumers(Node receiver, StartConsumersRequest startRequest) throws IOException {
        logger.info("send start request:{} to receiver:{}", startRequest, receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/consumers/start";
        String content = JsonUtil.writeValueAsString(startRequest);

        retryPostRequest(url, content);
    }

    @Override
    public ConsumerStatsResponse stopConsumers(Node receiver, StopConsumersRequest stopRequest) throws IOException {
        logger.info("send stop consume request:{} to receiver:{}", stopRequest, receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/consumers/stop";
        String content = JsonUtil.writeValueAsString(stopRequest);
        String retMsg = retryPostRequest(url, content);
        return JsonUtil.readValue(retMsg, ConsumerStatsResponse.class);
    }

    @Override
    public ConsumerStatsResponse pauseConsumers(Node receiver, PauseConsumersRequest suspendRequest) throws IOException {
        logger.info("send pause consumer request:{} to receiver:{}", suspendRequest, receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/consumers/pause";
        String content = JsonUtil.writeValueAsString(suspendRequest);
        String retMsg = retryPostRequest(url, content);
        return JsonUtil.readValue(retMsg, ConsumerStatsResponse.class);
    }

    @Override
    public ConsumerStatsResponse resumeConsumers(Node receiver, ResumeConsumerRequest resumeRequest) throws IOException {
        logger.info("send resume consumer request:{} to receiver:{}", resumeRequest, receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/consumers/resume";
        String content = JsonUtil.writeValueAsString(resumeRequest);
        String retMsg = retryPostRequest(url, content);
        return JsonUtil.readValue(retMsg, ConsumerStatsResponse.class);
    }

    @Override
    public void removeCubeSegment(Node receiver, String cubeName, String segmentName) throws IOException {
        logger.info("send request to receiver:{} to remove cube segment: {}", receiver, cubeName + "-" + segmentName);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/data/" + cubeName
                + "/" + segmentName;
        retryDeleteRequest(url);
    }

    @Override
    public void makeCubeImmutable(Node receiver, String cubeName) throws IOException {
        logger.info("send request to receiver:{} to make cube immutable: {}", receiver, cubeName);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/data/" + cubeName
                + "/immutable";

        retryPutRequest(url);
    }

    @Override
    public void segmentBuildComplete(Node receiver, String cubeName, String segmentName) throws IOException {
        logger.info("send request to receiver:{} to notify cube segment build complete: {}", receiver, cubeName + "-"
                + segmentName);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort()
                + "/kylin/api/admin/segment_build_complete/" + cubeName + "/" + segmentName;

        retryPutRequest(url);
    }

    @Override
    public void addToReplicaSet(Node receiver, int replicaSetID) throws IOException {
        logger.info("send request to receiver:{} to add to replica set: {}", receiver, replicaSetID);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/replica_set/"
                + replicaSetID + "/add";

        retryPutRequest(url);
    }

    @Override
    public void removeFromReplicaSet(Node receiver) throws IOException {
        logger.info("send request to receiver:{} to remove receiver from replica set", receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/admin/replica_set/remove";

        retryPutRequest(url);
    }

    @Override
    public ReceiverStats getReceiverStats(Node receiver) throws IOException {
        logger.info("send request to receiver:{} to get receiver stats ", receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/stats";

        String msg = restService.getRequest(url);
        return JsonUtil.readValue(msg, ReceiverStats.class);

    }

    @Override
    public ReceiverCubeStats getReceiverCubeStats(Node receiver, String cubeName) throws IOException {
        logger.info("send request to receiver:{} to get cube stats for cube:{}", receiver, cubeName);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/stats/cubes/" + cubeName;

        String msg = retryGetRequest(url);
        return JsonUtil.readValue(msg, ReceiverCubeStats.class);
    }

    @Override
    public HealthCheckInfo healthCheck(Node receiver) throws IOException {
        logger.info("send request to receiver:{} to do health check", receiver);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/stats/healthCheck";

        String msg = restService.getRequest(url);
        return JsonUtil.readValue(msg, HealthCheckInfo.class);
    }

    private String retryPostRequest(final String url, final String postContent) throws IOException {
        return retryCaller.call(new RetryCallable<String>() {
            @Override
            public String call() throws Exception {
                return restService.postRequest(url, postContent);
            }

            @Override
            public boolean isResultExpected(String result) {
                return true;
            }

            @Override
            public void update() {
            }
        });
    }

    private String retryDeleteRequest(final String url) throws IOException {
        return retryCaller.call(new RetryCallable<String>() {
            @Override
            public String call() throws Exception {
                return restService.deleteRequest(url);
            }

            @Override
            public boolean isResultExpected(String result) {
                return true;
            }

            @Override
            public void update() {
            }
        });
    }

    private String retryGetRequest(final String url) throws IOException {
        return retryCaller.call(new RetryCallable<String>() {
            @Override
            public String call() throws Exception {
                return restService.getRequest(url);
            }

            @Override
            public boolean isResultExpected(String result) {
                return true;
            }

            @Override
            public void update() {
            }
        });
    }

    private String retryPutRequest(final String url) throws IOException {
        return retryCaller.call(new RetryCallable<String>() {
            @Override
            public String call() throws Exception {
                return restService.putRequest(url);
            }

            @Override
            public boolean isResultExpected(String result) {
                return true;
            }

            @Override
            public void update() {
            }
        });
    }
}
