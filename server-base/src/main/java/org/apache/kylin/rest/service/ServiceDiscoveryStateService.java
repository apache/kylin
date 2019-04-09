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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.impl.curator.CuratorLeaderSelector;
import org.apache.kylin.job.impl.curator.CuratorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("serviceDiscoveryStateService")
public class ServiceDiscoveryStateService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscoveryStateService.class);

    public Set<Participant> getAllParticipants() {
        CuratorLeaderSelector leaderSelector = CuratorScheduler.getLeaderSelector();
        if (leaderSelector != null) {
            return leaderSelector.getParticipants();
        } else {
            return Collections.emptySet();
        }
    }

    public boolean isActiveJobNode() {
        CuratorLeaderSelector leaderSelector = CuratorScheduler.getLeaderSelector();
        return leaderSelector != null && leaderSelector.hasDefaultSchedulerStarted();
    }

    public Map<String, String> getJobServerState(Collection<String> servers) throws IOException {
        Map<String, String> r = new HashMap<>();
        for (String server : servers) {
            RestClient client = new RestClient(server);
            Pair<String, String> jobServerState = client.getJobServerWithState();
            logger.info("Ask server: {} for its job server state, is active job node: {}" //
                    , jobServerState.getFirst() //
                    , jobServerState.getSecond());//
            r.put(jobServerState.getFirst(), jobServerState.getSecond());
        }
        return r;
    }
}
