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

import java.util.List;

import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.rest.response.ServerInfoResponse;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class MockClusterManager implements ClusterManager {

    public static final String DEFAULT_ADDRESS = "127.0.0.1:7070";
    public static final String DEFAULT_ADDRESS_HA = "127.0.0.1:7071";

    @Override
    public String getLocalServer() {
        return DEFAULT_ADDRESS;
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return Lists.newArrayList(new ServerInfoResponse(DEFAULT_ADDRESS, ClusterConstant.QUERY),
                new ServerInfoResponse(DEFAULT_ADDRESS_HA, ClusterConstant.QUERY));
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return Lists.newArrayList(new ServerInfoResponse(DEFAULT_ADDRESS, ClusterConstant.ALL));
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return Lists.newArrayList(new ServerInfoResponse(DEFAULT_ADDRESS, ClusterConstant.ALL));
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        return Lists.newArrayList(new ServerInfoResponse(DEFAULT_ADDRESS, ClusterConstant.ALL));
    }
}
