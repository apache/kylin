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
package org.apache.kylin.tool.daemon;

import javax.crypto.SecretKey;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.tool.restclient.RestClient;

import lombok.Getter;

public class Worker {

    @Getter
    private static KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    @Getter
    private static RestClient restClient;

    @Getter
    private static CliCommandExecutor commandExecutor;

    @Getter
    private static SecretKey kgSecretKey;

    @Getter
    private static String KE_PID;

    static {
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        restClient = new RestClient("127.0.0.1", serverPort, null, null);

        commandExecutor = new CliCommandExecutor();
    }

    public static String getKylinHome() {
        return KylinConfig.getKylinHome();
    }

    public static String getServerPort() {
        return kylinConfig.getServerPort();
    }

    public synchronized void setKEPid(String pid) {
        KE_PID = pid;
    }

    public synchronized void setKgSecretKey(SecretKey secretKey) {
        kgSecretKey = secretKey;
    }

}
