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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.system.NodeRegistry;
import org.apache.kylin.metadata.system.NodeRegistryManager;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.test.util.ReflectionTestUtils;

public class NodeRegistryServiceTest extends NLocalFileMetadataTestCase {

    private NodeRegistryService nodeRegistryService;

    @Before
    public void setUp() {
        createTestMetadata();
        overwriteSystemProp("kylin.env.zookeeper.enabled", "false");
        overwriteSystemProp("kylin.server.node-registry.type", "jdbc");
        overwriteSystemProp("kylin.metadata.distributed-lock-impl", "org.apache.kylin.common.lock.LocalLockFactory");
        overwriteSystemProp("kylin.server.mode", "all");

        nodeRegistryService = Mockito.spy(new NodeRegistryService());

        ApplicationContext applicationContext = Mockito.mock(ApplicationContext.class);
        when(applicationContext.getBean(NodeRegistryService.class)).thenReturn(nodeRegistryService);
        SpringContext.setApplicationContextImpl(applicationContext);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testInitOnApplicationReady() {
        doNothing().when(nodeRegistryService).manuallyScheduleTasks();
        nodeRegistryService.initOnApplicationReady(null);
        verify(nodeRegistryService, times(1)).tryCreateNodeRegistry();
        verify(nodeRegistryService, times(1)).manuallyScheduleTasks();
        assertNotNull(ReflectionTestUtils.getField(nodeRegistryService, "scheduler"));
    }

    @Test
    public void testManuallyScheduleTasks() {
        try {
            nodeRegistryService.manuallyScheduleTasks();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("scheduler not initialized"));
        }

        ThreadPoolTaskScheduler scheduler = mock(ThreadPoolTaskScheduler.class);
        ReflectionTestUtils.setField(nodeRegistryService, "scheduler", scheduler);
        nodeRegistryService.manuallyScheduleTasks();
        verify(scheduler, times(2)).schedule(any(Runnable.class), any(CronTrigger.class));
    }

    @Test
    public void testRenew() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        manager.createNodeRegistryIfNotExists();

        List<NodeRegistry.NodeInstance> nodeInstances = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL);
        assertEquals(1, nodeInstances.size());
        NodeRegistry.NodeInstance instance = nodeInstances.get(0);

        nodeRegistryService.scheduleRenew();
        List<NodeRegistry.NodeInstance> newNodeInstances = manager.getNodeRegistry()
                .getNodeInstances(ServerModeEnum.ALL);
        assertEquals(1, newNodeInstances.size());
        NodeRegistry.NodeInstance newInstance = newNodeInstances.get(0);

        assertTrue(newInstance.getLastHeartbeatTS() > instance.getLastHeartbeatTS());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCheckAndClean() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        manager.createNodeRegistryIfNotExists();

        CachedCrudAssist<NodeRegistry> crud = (CachedCrudAssist<NodeRegistry>) ReflectionTestUtils.getField(manager,
                "crud");
        assertNotNull(crud);
        NodeRegistry nodeRegistry = crud.copyForWrite(manager.getNodeRegistry());
        NodeRegistry.NodeInstance timeoutInstance = new NodeRegistry.NodeInstance("123.123.123.123", 7070,
                ServerModeEnum.QUERY);
        ReflectionTestUtils.setField(timeoutInstance, "lastHeartbeatTS",
                System.currentTimeMillis() - getTestConfig().getNodeRegistryJdbcExpireThreshold() - 500);
        nodeRegistry.registerOrUpdate(timeoutInstance);
        crud.save(nodeRegistry);

        List<NodeRegistry.NodeInstance> nodeInstances = manager.getNodeRegistry()
                .getNodeInstances(ServerModeEnum.QUERY);
        assertEquals(1, nodeInstances.size());

        overwriteSystemProp("kylin.metadata.distributed-lock-impl", "org.apache.kylin.common.lock.LocalLockFactory");
        nodeRegistryService.scheduleCheck();
        List<NodeRegistry.NodeInstance> newNodeInstances = manager.getNodeRegistry()
                .getNodeInstances(ServerModeEnum.QUERY);
        assertTrue(newNodeInstances.isEmpty());
    }

    @Test
    public void testPreDestroy() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                .projectId(StringUtils.EMPTY).readonly(false).maxRetry(1).processor(() -> {
                    NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
                    manager.createNodeRegistryIfNotExists();
                    return null;
                }).build());

        NodeRegistryManager manager = NodeRegistryManager.getInstance(getTestConfig());
        List<NodeRegistry.NodeInstance> nodeInstances = manager.getNodeRegistry().getNodeInstances(ServerModeEnum.ALL);
        assertEquals(1, nodeInstances.size());

        nodeRegistryService.preDestroy();
        assertNull(manager.getNodeRegistry());
    }
}
