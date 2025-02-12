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

package org.apache.kylin.rest.discovery;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.metadata.system.NodeRegistry;
import org.apache.kylin.metadata.system.NodeRegistryManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@ConditionalOnNodeRegistryJdbcEnabled
@Component
@Slf4j
public class JdbcClusterManager implements ClusterManager {

    @Override
    public String getLocalServer() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(KylinConfig.getInstanceFromEnv());
        NodeRegistry.NodeInstance localInstance = manager.getLocalNodeInstance();
        return instance2Str(localInstance);
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

    private static List<ServerInfoResponse> getServerByMode(
            @Nullable ClusterConstant.ServerModeEnum... serverModeEnum) {
        List<ServerInfoResponse> servers = new ArrayList<>();
        if (serverModeEnum == null || serverModeEnum.length == 0) {
            return servers;
        }
        NodeRegistryManager manager = NodeRegistryManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (ClusterConstant.ServerModeEnum mode : serverModeEnum) {
            servers.addAll(manager.getNodeInstances(mode).stream()
                    .map(instance -> new ServerInfoResponse(instance2Str(instance), instance.getServerMode().getName()))
                    .collect(Collectors.toList()));
        }
        return servers;
    }

    private static String instance2Str(NodeRegistry.NodeInstance instance) {
        return String.format(Locale.ROOT, "%s:%s", instance.getHost(), instance.getPort());
    }
}
