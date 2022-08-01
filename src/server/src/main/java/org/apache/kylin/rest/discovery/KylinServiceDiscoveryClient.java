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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperDiscoveryClient;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnZookeeperEnabled
@Component
@Slf4j
public class KylinServiceDiscoveryClient implements KylinServiceDiscovery {

    private final Registration registration;
    private final ZookeeperDiscoveryClient discoveryClient;

    public KylinServiceDiscoveryClient(Registration registration, ZookeeperDiscoveryClient discoveryClient) {
        this.registration = registration;
        this.discoveryClient = discoveryClient;
    }

    public String getLocalServiceServer() {
        return registration.getHost() + ":" + registration.getPort();
    }

    @Override
    public List<ServerInfoResponse> getServerInfoByServerMode(@Nullable ServerModeEnum... serverModeEnums) {
        List<ServerInfoResponse> serverInfoRespLists = new ArrayList<>();

        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return serverInfoRespLists;
        }

        for (val nodeType : serverModeEnums) {
            for (val host : getServerStrByServerMode(nodeType)) {
                serverInfoRespLists.add(new ServerInfoResponse(host, nodeType.getName()));
            }
        }

        return serverInfoRespLists;
    }

    private List<String> getServerStrByServerMode(@Nonnull ServerModeEnum serverModeEnum) {
        Preconditions.checkNotNull(serverModeEnum, "server mode is null");

        String serverModeName = serverModeEnum.getName();
        val list = discoveryClient.getInstances(serverModeName);
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }

        return list.stream().map(serviceInstance -> serviceInstance.getHost() + ":" + serviceInstance.getPort())
                .collect(Collectors.toList());
    }

}
