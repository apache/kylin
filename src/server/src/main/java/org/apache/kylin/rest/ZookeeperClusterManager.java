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
package org.apache.kylin.rest;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.discovery.KylinServiceDiscoveryCache;
import org.apache.kylin.rest.discovery.KylinServiceDiscoveryClient;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnZookeeperEnabled
@Component
@Slf4j
public class ZookeeperClusterManager implements ClusterManager {

    @Autowired
    private KylinServiceDiscoveryCache serviceCache;

    @Autowired
    private KylinServiceDiscoveryClient discoveryClient;

    public ZookeeperClusterManager() {
    }

    @Override
    public String getLocalServer() {
        return discoveryClient.getLocalServiceServer();
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.QUERY);
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return getServers();
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.JOB);
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.QUERY);
    }

    private List<ServerInfoResponse> getServerByMode(@Nullable ServerModeEnum... serverModeEnum) {
        List<ServerInfoResponse> servers = new ArrayList<>();

        if (ArrayUtils.isEmpty(serverModeEnum)) {
            return servers;
        }

        for (val nodeModeType : serverModeEnum) {
            servers.addAll(serviceCache.getServerInfoByServerMode(nodeModeType));
        }
        return servers;
    }

}
