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

package org.apache.kylin.metadata.system;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class NodeRegistryTest extends NLocalFileMetadataTestCase {

    private NodeRegistry nodeRegistry;
    private NodeRegistry.NodeInstance instanceAll1;
    private NodeRegistry.NodeInstance instanceAll2;
    private NodeRegistry.NodeInstance instanceJob1;

    @Before
    public void setup() throws Exception {
        nodeRegistry = new NodeRegistry();
        instanceAll1 = new NodeRegistry.NodeInstance("host1", 7070, ServerModeEnum.ALL);
        instanceAll2 = new NodeRegistry.NodeInstance("host1", 7070, ServerModeEnum.ALL);
        instanceJob1 = new NodeRegistry.NodeInstance("host2", 8080, ServerModeEnum.JOB);

        // Mock KylinConfig to control expiration threshold
        createTestMetadata();
        overwriteSystemProp("kylin.server.node-registry.jdbc.expire-threshold", "1s");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRegisterOrUpdate_AddNewNode() {
        nodeRegistry.registerOrUpdate(instanceAll1);
        List<NodeRegistry.NodeInstance> instances = nodeRegistry.getNodeInstances(ServerModeEnum.ALL);
        assertEquals(1, instances.size());
        assertEquals(instanceAll1, instances.get(0));
    }

    @Test
    public void testRegisterOrUpdate_UpdateExistingNode() throws InterruptedException {
        nodeRegistry.registerOrUpdate(instanceAll1);
        // Simulate update with new heartbeat time
        NodeRegistry.NodeInstance updatedInstance = new NodeRegistry.NodeInstance("host1", 7070, ServerModeEnum.ALL);
        nodeRegistry.registerOrUpdate(updatedInstance);

        List<NodeRegistry.NodeInstance> instances = nodeRegistry.getNodeInstances(ServerModeEnum.ALL);
        assertEquals(1, instances.size());
        assertNotEquals(instanceAll1.getLastHeartbeatTS(), instances.get(0).getLastHeartbeatTS());
    }

    @Test
    public void testDeregister_ExistingNode() {
        nodeRegistry.registerOrUpdate(instanceAll1);
        nodeRegistry.deregister(instanceAll1);
        assertTrue(nodeRegistry.getNodeInstances(ServerModeEnum.ALL).isEmpty());
    }

    @Test
    public void testDeregister_NonExistingNode() {
        nodeRegistry.registerOrUpdate(instanceAll1);
        nodeRegistry.deregister(instanceJob1); // Different mode
        assertEquals(1, nodeRegistry.getNodeInstances(ServerModeEnum.ALL).size());
    }

    @Test
    public void testRemoveExpiredRegistrations() throws Exception {
        // Create expired instance (older than threshold)
        NodeRegistry.NodeInstance expiredInstance = new NodeRegistry.NodeInstance("host3", 9090, ServerModeEnum.QUERY);
        ReflectionTestUtils.setField(expiredInstance, "lastHeartbeatTS", System.currentTimeMillis() - 2000);

        // Create valid instance
        NodeRegistry.NodeInstance validInstance = new NodeRegistry.NodeInstance("host4", 9091, ServerModeEnum.QUERY);
        ReflectionTestUtils.setField(validInstance, "lastHeartbeatTS", System.currentTimeMillis() - 500);

        nodeRegistry.registerOrUpdate(expiredInstance);
        nodeRegistry.registerOrUpdate(validInstance);

        nodeRegistry.removeExpiredRegistrations();

        List<NodeRegistry.NodeInstance> instances = nodeRegistry.getNodeInstances(ServerModeEnum.QUERY);
        assertEquals(1, instances.size());
        assertEquals(validInstance, instances.get(0));
    }

    @Test
    public void testIsEmpty() {
        assertTrue(nodeRegistry.isEmpty());
        nodeRegistry.registerOrUpdate(instanceAll1);
        assertFalse(nodeRegistry.isEmpty());
        nodeRegistry.deregister(instanceAll1);
        assertTrue(nodeRegistry.isEmpty());
    }

    @Test
    public void testNodeInstanceEqualsAndHashCode() {
        assertEquals(instanceAll1, instanceAll2);
        assertEquals(instanceAll1.hashCode(), instanceAll2.hashCode());

        NodeRegistry.NodeInstance differentPort = new NodeRegistry.NodeInstance("host1", 7071, ServerModeEnum.ALL);
        assertNotEquals(instanceAll1, differentPort);
    }

    @Test
    public void testHostAndPortEquals() {
        NodeRegistry.NodeInstance differentMode = new NodeRegistry.NodeInstance("host1", 7070, ServerModeEnum.JOB);
        assertTrue(instanceAll1.hostAndPortEquals(differentMode));
    }

    @Test
    public void testGetNodeInstancesByMode() {
        nodeRegistry.registerOrUpdate(instanceAll1);
        nodeRegistry.registerOrUpdate(instanceJob1);

        List<NodeRegistry.NodeInstance> allInstances = nodeRegistry.getNodeInstances(ServerModeEnum.ALL);
        assertEquals(1, allInstances.size());

        List<NodeRegistry.NodeInstance> jobInstances = nodeRegistry.getNodeInstances(ServerModeEnum.JOB);
        assertEquals(1, jobInstances.size());
    }
}