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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Getter
@JsonAutoDetect(getterVisibility = NONE, isGetterVisibility = NONE, setterVisibility = NONE, fieldVisibility = NONE)
@Slf4j
public class NodeRegistry extends RootPersistentEntity {

    public static final String NODE_REGISTRY = "node_registry";

    @Getter
    @JsonAutoDetect(getterVisibility = NONE, isGetterVisibility = NONE, setterVisibility = NONE, fieldVisibility = NONE)
    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class NodeInstance {

        public NodeInstance() {
            // Non-args constructor for jackson deserializing
        }

        public NodeInstance(String host, int port, String modeStr) {
            this(host, port, ServerModeEnum.valueOf(modeStr));
        }

        public NodeInstance(String host, int port, ServerModeEnum mode) {
            this.host = host;
            this.port = port;
            this.serverMode = mode;
            this.lastHeartbeatTS = System.currentTimeMillis();
        }

        @JsonProperty("host")
        private String host;

        @JsonProperty("port")
        private int port;

        @JsonProperty("server_mode")
        private ServerModeEnum serverMode;

        @JsonProperty("last_heartbeat_ts")
        @EqualsAndHashCode.Exclude
        private long lastHeartbeatTS;

        public boolean hostAndPortEquals(NodeInstance that) {
            return Objects.equals(host, that.host) && port == that.port;
        }
    }

    @JsonProperty("name")
    private final String name = NODE_REGISTRY;

    @JsonProperty("registry")
    private final Map<ServerModeEnum, List<NodeInstance>> registry = new HashMap<>();

    @JsonProperty("last_cleanup_ts")
    private long lastCleanupTS;

    public List<NodeInstance> getNodeInstances(ServerModeEnum mode) {
        return registry.getOrDefault(mode, new ArrayList<>());
    }

    public void registerOrUpdate(NodeInstance instance) {
        Preconditions.checkNotNull(instance);
        List<NodeInstance> nodeInstances = registry.computeIfAbsent(instance.getServerMode(), k -> new ArrayList<>());
        if (nodeInstances.isEmpty()) {
            nodeInstances.add(instance);
        } else {
            int indexFound = findIndexByHostAndPort(nodeInstances, instance);
            if (indexFound > -1) {
                nodeInstances.set(indexFound, instance);
            } else {
                nodeInstances.add(instance);
            }
        }
    }

    public void deregister(NodeInstance instance) {
        Preconditions.checkNotNull(instance);
        List<NodeInstance> nodeInstances = registry.get(instance.getServerMode());
        if (nodeInstances != null && !nodeInstances.isEmpty()) {
            int indexFound = findIndexByHostAndPort(nodeInstances, instance);
            if (indexFound > -1) {
                nodeInstances.remove(indexFound);
            }
        }
    }

    public void removeExpiredRegistrations() {
        long now = System.currentTimeMillis();
        long expireThreshold = KylinConfig.getInstanceFromEnv().getNodeRegistryJdbcExpireThreshold();
        long timeoutTS = now - expireThreshold;
        for (Map.Entry<ServerModeEnum, List<NodeInstance>> entry : registry.entrySet()) {
            List<NodeInstance> nodeInstances = entry.getValue();
            if (nodeInstances != null && !nodeInstances.isEmpty()) {
                nodeInstances.removeIf(node -> node.getLastHeartbeatTS() < timeoutTS);
            }
        }
        lastCleanupTS = now;
    }

    public boolean isEmpty() {
        if (registry.isEmpty()) {
            return true;
        }
        for (List<NodeInstance> nodeInstances : registry.values()) {
            if (!nodeInstances.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String resourceName() {
        return NODE_REGISTRY;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.SYSTEM;
    }

    private static int findIndexByHostAndPort(List<NodeInstance> nodeInstances, NodeInstance instance) {
        for (int i = 0; i < nodeInstances.size(); i++) {
            NodeInstance ins = nodeInstances.get(i);
            if (ins.hostAndPortEquals(instance)) {
                return i;
            }
        }
        return -1;
    }
}
