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
import static org.apache.kylin.metadata.system.NodeRegistry.NODE_REGISTRY;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NodeRegistryManager {

    public static NodeRegistryManager getInstance(KylinConfig config) {
        return config.getManager(NodeRegistryManager.class);
    }

    // called by reflection
    static NodeRegistryManager newInstance(KylinConfig config) {
        return new NodeRegistryManager(config);
    }

    private final KylinConfig config;
    private final CachedCrudAssist<NodeRegistry> crud;
    private final String host;
    private final int port;
    private final ServerModeEnum serverMode;

    public NodeRegistryManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction()) {
            log.info("Initializing DiscoveryManager with KylinConfig Id: {}", System.identityHashCode(config));
        }
        this.config = config;
        crud = new CachedCrudAssist<NodeRegistry>(getStore(), MetadataType.SYSTEM, null, NodeRegistry.class) {
            @Override
            protected NodeRegistry initEntityAfterReload(NodeRegistry entity, String projectName) {
                return entity;
            }
        };
        host = AddressUtil.getLocalHostExactAddress();
        port = Integer.parseInt(config.getServerPort());
        serverMode = ServerModeEnum.of(config.getServerMode());
    }

    public NodeRegistry getNodeRegistry() {
        return crud.get(NODE_REGISTRY);
    }

    public List<NodeRegistry.NodeInstance> getNodeInstances(ServerModeEnum mode) {
        return getNodeRegistry().getNodeInstances(mode);
    }

    public NodeRegistry.NodeInstance getLocalNodeInstance() {
        return new NodeRegistry.NodeInstance(host, port, serverMode);
    }

    public void createNodeRegistryIfNotExists() {
        NodeRegistry nodeRegistry = getNodeRegistry();
        if (nodeRegistry != null) {
            renew();
        } else {
            NodeRegistry newNodeRegistry = copyForWrite(new NodeRegistry());
            newNodeRegistry.registerOrUpdate(newNodeInstance());
            crud.save(newNodeRegistry);
        }
    }

    public void renew() {
        NodeRegistry nodeRegistry = copyForWrite(getNodeRegistry());
        nodeRegistry.registerOrUpdate(newNodeInstance());
        crud.save(nodeRegistry);
    }

    public void checkAndClean() {
        NodeRegistry nodeRegistry = getNodeRegistry();
        if (nodeRegistry == null) {
            return;
        }
        nodeRegistry = copyForWrite(nodeRegistry);
        nodeRegistry.removeExpiredRegistrations();
        crud.save(nodeRegistry);
    }

    public void cleanup() {
        NodeRegistry nodeRegistry = getNodeRegistry();
        if (nodeRegistry == null) {
            return;
        }
        nodeRegistry = copyForWrite(nodeRegistry);
        nodeRegistry.deregister(newNodeInstance());
        if (nodeRegistry.isEmpty()) {
            crud.delete(nodeRegistry);
        } else {
            crud.save(nodeRegistry);
        }
    }

    public NodeRegistry copyForWrite(NodeRegistry nodeRegistry) {
        Preconditions.checkNotNull(nodeRegistry);
        return crud.copyForWrite(nodeRegistry);
    }

    ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(config);
    }

    private NodeRegistry.NodeInstance newNodeInstance() {
        return new NodeRegistry.NodeInstance(host, port, serverMode);
    }
}
