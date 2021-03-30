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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.ServiceDiscoveryStateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;

@Controller
@RequestMapping(value = "/service_discovery/state")
public class ServiceDiscoveryStateController extends BasicController {

    @Autowired
    @Qualifier("serviceDiscoveryStateService")
    ServiceDiscoveryStateService serviceDiscoveryStateService;

    @RequestMapping(value = "/is_active_job_node", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public boolean isActiveJobNode() {
        checkCuratorSchedulerEnabled();
        return serviceDiscoveryStateService.isActiveJobNode();
    }

    @RequestMapping(value = "/all", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<ServiceDiscoveryState> getAllNodeStates() throws IOException {
        checkCuratorSchedulerEnabled();
        Set<String> allNodes = new HashSet<>();
        Set<String> queryNodes = new HashSet<>();
        Set<String> jobNodes = new HashSet<>();
        Set<String> leaders = new HashSet<>();

        // get all nodes and query nodes
        for (String serverWithMode : KylinConfig.getInstanceFromEnv().getRestServersWithMode()) {
            String[] split = serverWithMode.split(":");
            Preconditions.checkArgument(split.length == 3,
                    "String should be \"host:server:mode\", actual:" + serverWithMode);
            String server = split[0] + ":" + split[1];
            String mode = split[2];
            allNodes.add(server);
            if (mode.equals("query") || mode.equals("all")) {
                queryNodes.add(server);
            }
            if (mode.equals("job") || mode.equals("all")) {
                jobNodes.add(server);
            }
        }

        // Get all selection participants(only job nodes will participate in the election) and selected leaders
        Set<Participant> allParticipants = serviceDiscoveryStateService.getAllParticipants();
        if (!allParticipants.isEmpty()) {
            jobNodes = allParticipants.stream() //
                    .map(Participant::getId) //
                    .collect(Collectors.toSet()); //

            // There should only one leader, if there are more than one leader, means something wrong happened
            leaders = allParticipants.stream() //
                    .filter(Participant::isLeader) //
                    .map(Participant::getId) //
                    .collect(Collectors.toSet()); //
        }

        // Ask for other nodes for its job server state
        // current Kylin only has one active job node
        // If there are more than one active job node, means something wrong happened
        Set<String> activeJobNodes = getActiveJobNodes(allNodes);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                new ServiceDiscoveryState(allNodes, jobNodes, queryNodes, leaders, activeJobNodes),
                "get service discovery's state");
    }

    private void checkCuratorSchedulerEnabled() {
        // 100 means CuratorScheduler
        // This monitor only meaningful to CuratorScheduler
        if (KylinConfig.getInstanceFromEnv().getSchedulerType() != 100) {
            throw new UnsupportedOperationException("Only meaningful when scheduler is CuratorScheduler, "
                    + "try set kylin.job.scheduler.default to 100 to enable CuratorScheduler.");
        }
    }

    private Set<String> getActiveJobNodes(Collection<String> allNodes) throws IOException {
        Map<String, String> jobServerState = serviceDiscoveryStateService.getJobServerState(allNodes);
        Set<String> activeJobNodes = jobServerState.entrySet().stream() //
                .filter(e -> e.getValue().equals("true")) //
                .map(Map.Entry::getKey) //
                .collect(Collectors.toSet()); //
        return activeJobNodes;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE) //
    static class ServiceDiscoveryState implements Serializable {
        @JsonProperty()
        Set<String> allNodes;
        @JsonProperty()
        Set<String> jobNodes;
        @JsonProperty()
        Set<String> queryNodes;
        @JsonProperty()
        Set<String> selectedLeaders;
        @JsonProperty()
        Set<String> activeJobNodes;

        ServiceDiscoveryState(Set<String> allNodes, Set<String> participants, Set<String> queryNodes,
                Set<String> leaders, Set<String> activeJobNodes) {
            this.allNodes = allNodes;
            this.jobNodes = participants;
            this.queryNodes = queryNodes;
            this.selectedLeaders = leaders;
            this.activeJobNodes = activeJobNodes;
        }
    }
}
