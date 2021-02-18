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

package org.apache.kylin.common.util;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ServerMode {
    public final static String SERVER_MODE_QUERY = "query";
    public final static String SERVER_MODE_JOB = "job";
    public final static String SERVER_MODE_STREAM_COORDINATOR = "stream_coordinator";
    public final static String SERVER_MODE_ALL = "all";

    private List<String> serverModes;

    public ServerMode(List<String> serverModes) {
        this.serverModes = serverModes;
    }

    public boolean canServeQuery() {
        return serverModes.contains(SERVER_MODE_ALL) || serverModes.contains(SERVER_MODE_QUERY);
    }

    public boolean canServeJobBuild() {
        return serverModes.contains(SERVER_MODE_ALL) || serverModes.contains(SERVER_MODE_JOB);
    }

    public boolean canServeStreamingCoordinator() {
        return serverModes.contains(SERVER_MODE_ALL)
                || serverModes.contains(SERVER_MODE_STREAM_COORDINATOR);
    }

    public boolean canServeAll() {
        return serverModes.contains(SERVER_MODE_ALL);
    }

    @Override
    public String toString() {
        return serverModes.toString();
    }

    public static ServerMode SERVER_MODE = getServerMode();

    private static ServerMode getServerMode() {
        return getServerMode(KylinConfig.getInstanceFromEnv());
    }

    public static ServerMode getServerMode(KylinConfig kylinConfig) {
        String serverModeStr = kylinConfig.getServerMode();
        List<String> serverModes = Lists.newArrayList();
        String[] serverModeArray = serverModeStr.split("\\s*,\\s*");
        for (String serverMode : serverModeArray) {
            serverModes.add(serverMode.toLowerCase(Locale.ROOT));
        }
        return new ServerMode(serverModes);
    }
}
