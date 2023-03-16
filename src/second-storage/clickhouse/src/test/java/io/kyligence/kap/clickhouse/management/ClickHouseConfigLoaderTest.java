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

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;

public class ClickHouseConfigLoaderTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testTransformNode() throws IOException {
        ClusterInfo cluster = new ClusterInfo().setKeepAliveTimeout("600000").setSocketTimeout("600000");
        Map<String, List<Node>> clusterNode = new HashMap<>();
        clusterNode.put("pair0", Lists.newArrayList(new Node("node01", "127.0.0.1", 9000, "default", "", 111)));
        clusterNode.put("pair1", Lists.newArrayList(new Node("node02", "127.0.0.2", 9000, "default", "")));
        cluster.setCluster(clusterNode);
        cluster.transformNode();
        int size = cluster.getNodes().stream().filter(node -> {
            return node.getSSHPort() == 22;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, size);

        size = cluster.getNodes().stream().filter(node -> {
            return node.getSSHPort() == 111;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, size);

        File file = File.createTempFile("clickhouseTemp", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(
                JsonUtil.readValue(JsonUtil.writeValueAsString(cluster), Map.class),
                new PrintWriter(file, Charset.defaultCharset().name()));
        overwriteSystemProp(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());

        ClusterInfo clusterReader = ClickHouseConfigLoader.getInstance().getCluster();
        List<Node> allNodes = clusterReader.getNodes();
        int portSize = allNodes.stream().filter(node -> {
            return node.getSSHPort() == 22;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, portSize);

        portSize = allNodes.stream().filter(node -> {
            return node.getSSHPort() == 111;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, portSize);
    }
}
