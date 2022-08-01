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
package org.apache.kylin.rest.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.rest.response.ServerInfoResponse;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class DefaultClusterManager implements ClusterManager {

    int port;

    @Override
    public String getLocalServer() {
        try {
            return InetAddress.getLocalHost().getHostName() + ":" + port;
        } catch (UnknownHostException e) {
            log.warn("cannot get hostname", e);
            return "localhost:" + port;
        }
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        List<ServerInfoResponse> servers = new ArrayList<>();
        ServerInfoResponse server = new ServerInfoResponse();
        server.setHost(getLocalServer());
        server.setMode(ClusterConstant.ALL);
        servers.add(server);
        return servers;
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return getQueryServers();
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getQueryServers();
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        return getQueryServers();
    }
}
