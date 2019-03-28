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

import org.apache.kylin.stream.core.model.HealthCheckInfo;
import org.apache.kylin.stream.core.model.stats.ReceiverCubeStats;
import org.apache.kylin.stream.core.model.stats.ReceiverStats;
import org.apache.kylin.stream.server.StreamingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handle statistics requests.
 *
 */
@Controller
@RequestMapping(value = "/stats")
public class StatsController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(StatsController.class);

    private StreamingServer streamingServer;

    public StatsController() {
        streamingServer = StreamingServer.getInstance();
    }

    @RequestMapping(method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public ReceiverStats getAllStats() {
        return streamingServer.getReceiverStats();
    }

    @RequestMapping(value = "/cubes/{cubeName}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public ReceiverCubeStats getCubeStats(@PathVariable String cubeName) {
        return streamingServer.getCubeStats(cubeName);
    }

    @RequestMapping(value = "/healthCheck", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public HealthCheckInfo healthCheck() {
        HealthCheckInfo result = new HealthCheckInfo();
        result.setStatus(HealthCheckInfo.Status.GOOD);
        return result;
    }

}
