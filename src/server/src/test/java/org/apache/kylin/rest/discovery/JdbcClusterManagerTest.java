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

package org.apache.kylin.rest.discovery;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.system.NodeRegistry;
import org.apache.kylin.metadata.system.NodeRegistryManager;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class JdbcClusterManagerTest extends NLocalFileMetadataTestCase {

    private JdbcClusterManager jdbcClusterManager;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        createTestMetadata();
        overwriteSystemProp("kylin.server.mode", "all");

        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        manager.createNodeRegistryIfNotExists();

        CachedCrudAssist<NodeRegistry> crud = (CachedCrudAssist<NodeRegistry>) ReflectionTestUtils.getField(manager,
                "crud");
        assertNotNull(crud);

        {
            NodeRegistry nodeRegistry = crud.copyForWrite(manager.getNodeRegistry());
            NodeRegistry.NodeInstance instance = new NodeRegistry.NodeInstance("123.123.123.101", 7070,
                    ClusterConstant.ServerModeEnum.QUERY);
            nodeRegistry.registerOrUpdate(instance);
            crud.save(nodeRegistry);
        }
        {
            NodeRegistry nodeRegistry = crud.copyForWrite(manager.getNodeRegistry());
            NodeRegistry.NodeInstance instance = new NodeRegistry.NodeInstance("123.123.123.102", 7070,
                    ClusterConstant.ServerModeEnum.JOB);
            nodeRegistry.registerOrUpdate(instance);
            crud.save(nodeRegistry);
        }

        assertEquals(3, manager.getNodeRegistry().getRegistry().values().size());
        assertEquals(1, manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL).size());
        assertEquals(1, manager.getNodeRegistry().getNodeInstances(ServerModeEnum.QUERY).size());
        assertEquals(1, manager.getNodeRegistry().getNodeInstances(ServerModeEnum.JOB).size());

        jdbcClusterManager = new JdbcClusterManager();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testGetLocalServer() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        NodeRegistry.NodeInstance instance = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL).get(0);
        assertEquals(toIpPortStr(instance.getHost(), instance.getPort()), jdbcClusterManager.getLocalServer());
    }

    @Test
    public void testGetQueryServers() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        NodeRegistry.NodeInstance all = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL).get(0);
        NodeRegistry.NodeInstance query = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.QUERY).get(0);

        List<ServerInfoResponse> servers = jdbcClusterManager.getQueryServers();
        assertEquals(2, servers.size());

        assertEquals(toIpPortStr(all.getHost(), all.getPort()), servers.get(0).getHost());
        assertEquals(all.getServerMode().getName(), servers.get(0).getMode());
        assertEquals(toIpPortStr(query.getHost(), query.getPort()), servers.get(1).getHost());
        assertEquals(query.getServerMode().getName(), servers.get(1).getMode());
    }

    @Test
    public void testGetJobServers() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        NodeRegistry.NodeInstance all = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL).get(0);
        NodeRegistry.NodeInstance job = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.JOB).get(0);

        List<ServerInfoResponse> servers = jdbcClusterManager.getJobServers();
        assertEquals(2, servers.size());

        assertEquals(toIpPortStr(all.getHost(), all.getPort()), servers.get(0).getHost());
        assertEquals(all.getServerMode().getName(), servers.get(0).getMode());
        assertEquals(toIpPortStr(job.getHost(), job.getPort()), servers.get(1).getHost());
        assertEquals(job.getServerMode().getName(), servers.get(1).getMode());
    }

    @Test
    public void testGetServers() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        NodeRegistry.NodeInstance all = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL).get(0);
        NodeRegistry.NodeInstance job = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.JOB).get(0);
        NodeRegistry.NodeInstance query = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.QUERY).get(0);

        List<ServerInfoResponse> servers = jdbcClusterManager.getServersFromCache();
        assertEquals(3, servers.size());

        assertEquals(toIpPortStr(all.getHost(), all.getPort()), servers.get(0).getHost());
        assertEquals(all.getServerMode().getName(), servers.get(0).getMode());
        assertEquals(toIpPortStr(job.getHost(), job.getPort()), servers.get(1).getHost());
        assertEquals(job.getServerMode().getName(), servers.get(1).getMode());
        assertEquals(toIpPortStr(query.getHost(), query.getPort()), servers.get(2).getHost());
        assertEquals(query.getServerMode().getName(), servers.get(2).getMode());
    }

    private static String toIpPortStr(String ip, int port) {
        return String.format(Locale.ROOT, "%s:%s", ip, port);
    }
}
