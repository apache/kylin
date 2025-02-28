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
import java.util.Locale;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.util.SpringContext;

public interface ClusterManager {

    String getLocalServer();

    List<ServerInfoResponse> getQueryServers();

    List<ServerInfoResponse> getServersFromCache();

    List<ServerInfoResponse> getJobServers();

    List<ServerInfoResponse> getServers();

    default ServerInfoResponse getServerById(String serverId) {
        throw new KylinRuntimeException(
                String.format(Locale.ROOT, "Method `getServerById` is not supported in class <%s>", this.getClass()));
    }

    static ClusterManager getInstance() {
        return SpringContext.getApplicationContext().getBean(ClusterManager.class);
    }
}
