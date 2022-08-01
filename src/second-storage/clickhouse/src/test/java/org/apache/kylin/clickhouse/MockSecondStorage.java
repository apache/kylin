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

package org.apache.kylin.clickhouse;

import static org.apache.kylin.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.clickhouse.management.ClickHouseConfigLoader;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.Assert;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.secondstorage.SecondStorage;
import org.apache.kylin.secondstorage.SecondStorageUtil;
import org.apache.kylin.secondstorage.config.ClusterInfo;
import org.apache.kylin.secondstorage.config.Node;
import lombok.val;

public class MockSecondStorage {
    public static void mock() throws IOException {
        Unsafe.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        ClusterInfo cluster = new ClusterInfo();
        cluster.setKeepAliveTimeout("600000");
        cluster.setSocketTimeout("600000");
        cluster.setCluster(Collections.emptyMap());
        File file = File.createTempFile("clickhouse", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster),
                Map.class), new PrintWriter(file, Charset.defaultCharset().name()));
        Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        SecondStorage.init(true);
    }

    public static void mock(String project, List<Node> nodes, NLocalFileMetadataTestCase testCase) throws IOException {
        testCase.overwriteSystemProp("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        ClusterInfo cluster = new ClusterInfo();
        cluster.setKeepAliveTimeout("600000");
        cluster.setSocketTimeout("600000");
        Map<String, List<Node>> clusterNodes = new HashMap<>();
        cluster.setCluster(clusterNodes);
        val it = nodes.listIterator();
        while (it.hasNext()) {
            clusterNodes.put("pair" + it.nextIndex(), Collections.singletonList(it.next()));
        }
        File file = File.createTempFile("clickhouse", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster),
                Map.class), new PrintWriter(file, Charset.defaultCharset().name()));
        Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        SecondStorage.init(true);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(testCase.getTestConfig(), project);
            Assert.assertTrue(nodeGroupManager.isPresent());
            return nodeGroupManager.get().makeSureRootEntity("");
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

}
