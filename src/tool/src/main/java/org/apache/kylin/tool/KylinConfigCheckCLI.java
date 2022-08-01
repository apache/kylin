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

package org.apache.kylin.tool;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;

/**
 * When performing check-env, this class is used to check kylin config
 */
public class KylinConfigCheckCLI {

    private static final String SERVER_CONFIG_PREFIX = "server.";
    private static final String SPRING_CONFIG_PREFIX = "spring.";
    private static final String KYLIN_CONFIG_PREFIX = "kylin.";
    /**
     * Not recommended set the configuration items at the beginning with kap
     */
    private static final String KAP_CONFIG_PREFIX = "kap.";

    public static void main(String[] args) {
        execute();
        Unsafe.systemExit(0);
    }

    public static void execute() {
        Properties config = KylinConfig.newKylinConfig().exportToProperties();
        for (String key : config.stringPropertyNames()) {
            if (!StringUtils.startsWith(key, KYLIN_CONFIG_PREFIX) && !StringUtils.startsWith(key, SERVER_CONFIG_PREFIX)
                    && !StringUtils.startsWith(key, SPRING_CONFIG_PREFIX)
                    && !StringUtils.startsWith(key, KAP_CONFIG_PREFIX)) {
                System.out.println(key);
                break;
            }
        }
    }
}
