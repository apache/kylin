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

package io.kyligence.kap.secondstorage.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;

@DataDefinition
public class NodeGroup extends RootPersistentEntity implements Serializable,
        IManagerAware<NodeGroup> {
    @JsonProperty("nodeNames")
    private List<String> nodeNames = new ArrayList<>();
    @JsonProperty("lock_types")
    private List<String> lockTypes = new ArrayList<>();
    private transient Manager<NodeGroup> manager;

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void setManager(Manager<NodeGroup> manager) {
        this.manager = manager;
    }

    @Override
    public void verify() {
        // node name can't duplicate
        if (nodeNames != null) {
            Preconditions.checkArgument(nodeNames.stream().distinct().count() == nodeNames.size());
        }
        if (lockTypes != null) {
            Preconditions.checkArgument(lockTypes.stream().distinct().count() == lockTypes.size());
        }
    }

    public List<String> getNodeNames() {
        return CollectionUtils.isEmpty(nodeNames) ? Collections.emptyList() : Collections.unmodifiableList(nodeNames);
    }

    public NodeGroup setNodeNames(List<String> nodeNames) {
        this.checkIsNotCachedAndShared();
        this.nodeNames = nodeNames;
        return this;
    }

    public List<String> getLockTypes() {
        return CollectionUtils.isEmpty(lockTypes) ? Collections.emptyList() : Collections.unmodifiableList(lockTypes);
    }

    public NodeGroup setLockTypes(List<String> lockTypes) {
        this.lockTypes = lockTypes;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NodeGroup nodeGroup = (NodeGroup) o;

        return nodeNames != null ? nodeNames.equals(nodeGroup.nodeNames) : nodeGroup.nodeNames == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (nodeNames != null ? nodeNames.hashCode() : 0);
        return result;
    }

    public NodeGroup update(Consumer<NodeGroup> updater) {
        Preconditions.checkArgument(manager != null);
        return manager.update(uuid, updater);
    }

    public static final class Builder {
        private List<String> nodeNames;
        private List<String> lockTypes;

        public Builder setNodeNames(List<String> nodeNames) {
            this.nodeNames = nodeNames;
            return this;
        }

        public Builder setLockTypes(List<String> lockTypes) {
            this.lockTypes = lockTypes;
            return this;
        }

        public NodeGroup build() {
            NodeGroup nodeGroup = new NodeGroup();
            nodeGroup.nodeNames = nodeNames;
            nodeGroup.lockTypes = lockTypes;
            return nodeGroup;
        }
    }
}
