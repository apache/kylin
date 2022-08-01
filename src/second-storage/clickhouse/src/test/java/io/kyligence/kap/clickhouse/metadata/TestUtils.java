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

package io.kyligence.kap.clickhouse.metadata;

import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import org.apache.kylin.common.util.Unsafe;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.util.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;

public class TestUtils {
    public static void createEmptyClickHouseConfig() {
        ClusterInfo cluster = new ClusterInfo();
        cluster.setKeepAliveTimeout("600000");
        cluster.setSocketTimeout("600000");
        cluster.setCluster(new HashMap<>());
        int i = 1;
        File file = null;
        try {
            file = File.createTempFile("clickhouse", ".yaml");
            ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster),
                    Map.class), new PrintWriter(file, Charset.defaultCharset().name()));
            Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }
}
