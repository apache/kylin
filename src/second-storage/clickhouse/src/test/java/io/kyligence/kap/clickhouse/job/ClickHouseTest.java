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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import lombok.val;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        initNodeHelper();
    }

    public void initNodeHelper() {
        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNodes = new HashMap<>();
        cluster.setCluster(clusterNodes);
        clusterNodes.put("pair1", Collections.singletonList(new Node().setName("node01").setIp("127.0.0.1").setPort(9000).setUser("default").setPassword("123456")));
        clusterNodes.put("pair2", Collections.singletonList(new Node().setName("node02").setIp("127.0.0.1").setPort(9000).setUser("default")));
        clusterNodes.put("pair3", Collections.singletonList(new Node().setName("node03").setIp("127.0.0.1").setPort(9000)));
        SecondStorageNodeHelper.initFromCluster(
                cluster,
                node -> ClickHouse.buildUrl(node.getIp(), node.getPort(), ClickHouseStorage.getJdbcUrlProperties(cluster, node)),
                null);
    }

    @Test
    public void createClickHouse() throws SQLException {
        ClickHouse clickHouse1 = new ClickHouse(SecondStorageNodeHelper.resolve("node01"));
        Assert.assertEquals("127.0.0.1:9000", clickHouse1.getShardName());
        ClickHouse clickHouse2 = new ClickHouse(SecondStorageNodeHelper.resolve("node02"));
        Assert.assertEquals("127.0.0.1:9000", clickHouse2.getShardName());
        ClickHouse clickHouse3 = new ClickHouse(SecondStorageNodeHelper.resolve("node03"));
        Assert.assertEquals("127.0.0.1:9000", clickHouse3.getShardName());
    }

    @Test
    public void extractParam() {
        val param = ClickHouse.extractParam(SecondStorageNodeHelper.resolve("node01"));
        Assert.assertEquals(3, param.size());
        Assert.assertEquals("3", param.get("connect_timeout"));
        val param2 = ClickHouse.extractParam(SecondStorageNodeHelper.resolve("node03"));
        Assert.assertEquals(0, param2.size());
    }

    @Test
    public void testJdbcUrlProperties() {
        Node node = new Node("node01", "127.0.0.1", 9000, "default", "123456");
        ClusterInfo cluster = new ClusterInfo();
        cluster.setKeepAliveTimeout("1000");
        cluster.setSocketTimeout("1000");
        Map<String, String> properties = ClickHouseStorage.getJdbcUrlProperties(cluster, node);
        Assert.assertEquals(properties.get(ClickHouse.KEEP_ALIVE_TIMEOUT), cluster.getKeepAliveTimeout());
        Assert.assertEquals(properties.get(ClickHouse.SOCKET_TIMEOUT), cluster.getSocketTimeout());
        Assert.assertEquals(properties.get(ClickHouse.USER), node.getUser());
        Assert.assertEquals(properties.get(ClickHouse.PASSWORD), node.getPassword());

        String url = ClickHouse.buildUrl(node.getIp(), node.getPort(), properties);
        Assert.assertEquals("jdbc:clickhouse://127.0.0.1:9000?socket_timeout=600000&keepAliveTimeout=600000&password=123456&user=default&connect_timeout=3", url);
    }
}