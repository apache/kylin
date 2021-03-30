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

package org.apache.kylin.stream.server.rest.controller;

import org.apache.kylin.stream.core.model.AssignRequest;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ResumeConsumerRequest;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.model.UnAssignRequest;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.server.StreamingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * @see org.apache.kylin.stream.core.client.ReceiverAdminClient
 */
@Controller
@RequestMapping(value = "/admin")
public class AdminController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(AdminController.class);

    private StreamingServer streamingServer;

    public AdminController() {
        streamingServer = StreamingServer.getInstance();
    }

    @RequestMapping(value = "/assign", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public void assign(@RequestBody AssignRequest assignRequest) {
        logger.info("receive assign request:{}", assignRequest);
        streamingServer.assign(assignRequest.getCubeName(), assignRequest.getPartitions());
        if (assignRequest.isStartConsumers()) {
            streamingServer.startConsumers(Lists.newArrayList(assignRequest.getCubeName()));
        }
    }

    @RequestMapping(value = "/unAssign", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public void unAssign(@RequestBody UnAssignRequest unAssignRequest) {
        logger.info("receive unassign request:{}", unAssignRequest);
        streamingServer.unAssign(unAssignRequest.getCube());
    }

    @RequestMapping(value = "/consumers/start", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public void startConsumers(@RequestBody StartConsumersRequest startRequest) {
        logger.info("receive start consumer request:{}", startRequest);
        streamingServer.startConsumer(startRequest.getCube(), startRequest.getStartProtocol());
    }

    @RequestMapping(value = "/consumers/stop", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public ConsumerStatsResponse stopConsumers(@RequestBody StopConsumersRequest request) {
        logger.info("receive stop consumer request:{}", request);
        ConsumerStatsResponse response = streamingServer.stopConsumer(request.getCube());
        if (request.isRemoveData()) {
            streamingServer.removeCubeData(request.getCube());
        }
        return response;
    }

    @RequestMapping(value = "/consumers/pause", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public ConsumerStatsResponse pauseConsumers(@RequestBody PauseConsumersRequest request) {
        logger.info("receive pause consumer request:{}", request);
        return streamingServer.pauseConsumer(request.getCube());
    }

    @RequestMapping(value = "/consumers/resume", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public ConsumerStatsResponse resumeConsumers(@RequestBody ResumeConsumerRequest request) {
        logger.info("receive resume consumer request:{}", request);
        return streamingServer.resumeConsumer(request.getCube(), request.getResumeToPosition());
    }

    @RequestMapping(value = "/segment_build_complete/{cubeName}/{segmentName}", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public void segmentBuildComplete(@PathVariable(value = "cubeName") String cubeName,
            @PathVariable(value = "segmentName") String segmentName) {
        logger.info("receive segment build complete, cube:{}, segment:{}", cubeName, segmentName);
        streamingServer.remoteSegmentBuildComplete(cubeName, segmentName);
    }

    @RequestMapping(value = "/data/{cubeName}/{segmentName}", method = RequestMethod.DELETE, produces = { "application/json" })
    @ResponseBody
    public void removeSegment(@PathVariable(value = "cubeName") String cubeName,
            @PathVariable(value = "segmentName") String segmentName) {
        logger.info("receive remove segment request, cube:{}, segment:{}", cubeName, segmentName);
        StreamingSegmentManager segmentManager = streamingServer.getStreamingSegmentManager(cubeName);
        segmentManager.purgeSegment(segmentName);
    }

    @RequestMapping(value = "/data/{cubeName}", method = RequestMethod.DELETE, produces = { "application/json" })
    @ResponseBody
    public void removeCubeData(@PathVariable(value = "cubeName") String cubeName) {
        logger.info("receive remove cube request, cube:{}", cubeName);
        streamingServer.removeCubeData(cubeName);
    }

    @RequestMapping(value = "/data/{cubeName}/immutable", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public void immuteCube(@PathVariable(value = "cubeName") String cubeName) {
        logger.info("receive make cube immutable request, cube:{}", cubeName);
        streamingServer.makeCubeImmutable(cubeName);
    }

    @RequestMapping(value = "/data/{cubeName}/{segmentName}/immutable", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public void immuteCubeSegment(@PathVariable(value = "cubeName") String cubeName,
            @PathVariable(value = "segmentName") String segmentName) {
        logger.info("receive make cube segment immutable request, cube:{} segment:{}", cubeName, segmentName);
        streamingServer.makeCubeSegmentImmutable(cubeName, segmentName);
    }

    /**
     * <pre>
     * If some receiver failed to upload local segment cache to HDFS automatically for some reason,
     *  coordinator cannot sumbit a building job because data is incomplete.
     * In this case, kylin admin may use this API to re-upload local segment cache.
     * </pre>
     */
    @RequestMapping(value = "/data/{cubeName}/{segmentName}/reSubmit", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public void reSubmitCubeSegment(@PathVariable(value = "cubeName") String cubeName,
            @PathVariable(value = "segmentName") String segmentName) {
        logger.info("receive reSubmit segment request, cube:{} segment:{}", cubeName, segmentName);
        streamingServer.reSubmitCubeSegment(cubeName, segmentName);
    }

    @RequestMapping(value = "/replica_set/{rsID}/add", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public void addToReplicaSet(@PathVariable(value = "rsID") int replicaSetID) {
        logger.info("receive add to replica set request, rsID:{}", replicaSetID);
        streamingServer.addToReplicaSet(replicaSetID);
    }

    @RequestMapping(value = "/replica_set/remove", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public void removeFromReplicaSet() {
        logger.info("receive remove from replica set request");
        streamingServer.removeFromReplicaSet();
    }
}
