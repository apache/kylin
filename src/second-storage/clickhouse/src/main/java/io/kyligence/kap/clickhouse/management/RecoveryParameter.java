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

package io.kyligence.kap.clickhouse.management;

import lombok.Data;

@Data
public class RecoveryParameter {
    public static final String CLICKHOUSE_DEFAULT_DATA_PATH = "/var/lib/clickhouse";

    private String sourceIp;
    private int sourceSshPort = 22;
    private String sourceSshUser;
    private String sourceSshPassword;
    private int sourceCHPort = 9000;
    private String sourceCHUser = "default";
    private String sourceCHPassword = "";
    private String targetIp;
    private int targetSshPort = 22;
    private String targetSshUser;
    private String targetSshPassword;
    private int targetCHPort = 9000;
    private String targetCHUser = "default";
    private String targetCHPassword = "";
    private String database;
    private String table;
    private String sourceDataPath = CLICKHOUSE_DEFAULT_DATA_PATH;
    private String targetDataPath = CLICKHOUSE_DEFAULT_DATA_PATH;

    public String getSourceJdbcUrl() {
        return getJdbcUrl(sourceIp, sourceCHPort);
    }

    public String getTargetJdbcUrl() {
        return getJdbcUrl(targetIp, targetCHPort);
    }

    private String getJdbcUrl(String ip, int port) {
        return "jdbc:clickhouse://" + ip + ":" + port;
    }
}
