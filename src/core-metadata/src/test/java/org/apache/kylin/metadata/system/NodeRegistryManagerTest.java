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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NodeRegistryManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        overwriteSystemProp("kylin.server.mode", "all");
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testCreateNodeRegistry() throws Exception {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        manager.createNodeRegistryIfNotExists();

        List<NodeRegistry.NodeInstance> nodeInstances = manager.getNodeRegistry()
                .getNodeInstances(ClusterConstant.ServerModeEnum.ALL);
        assertEquals(1, nodeInstances.size());
        NodeRegistry.NodeInstance instance = nodeInstances.get(0);

        // test renew condition on registry existing
        manager.createNodeRegistryIfNotExists();
        List<NodeRegistry.NodeInstance> newNodeInstances = manager.getNodeRegistry()
                .getNodeInstances(ClusterConstant.ServerModeEnum.ALL);
        assertEquals(1, newNodeInstances.size());
        NodeRegistry.NodeInstance newInstance = newNodeInstances.get(0);

        assertTrue(newInstance.getLastHeartbeatTS() > instance.getLastHeartbeatTS());
    }
}
