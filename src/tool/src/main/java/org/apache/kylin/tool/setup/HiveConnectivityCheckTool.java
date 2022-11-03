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

package org.apache.kylin.tool.setup;

import org.apache.kylin.common.util.Unsafe;
import org.apache.spark.util.KylinHiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveConnectivityCheckTool {

    public static final Logger logger = LoggerFactory.getLogger(HiveConnectivityCheckTool.class);

    public boolean listDatabases() {
        try {
            KylinHiveUtils.checkHiveIsAccessible("show databases");
            return true;
        } catch (Exception e) {
            logger.error(String.format("Connection test failed %s", e.getMessage()));
            return false;
        }
    }

    public static void main(String[] args) {

        HiveConnectivityCheckTool kapGetHiveConnectivity = new HiveConnectivityCheckTool();
        if (kapGetHiveConnectivity.listDatabases()) {
            logger.error("Test hive connectivity successd.");
            Unsafe.systemExit(0);
        } else {
            logger.error("Test hive connectivity Failed.");
            Unsafe.systemExit(1);
        }

    }

}