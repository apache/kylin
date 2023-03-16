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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@DataDefinition
public class TablePartition implements Serializable {

    @JsonProperty("shard_nodes")
    private final List<String> shardNodes = Lists.newArrayList();
    @JsonProperty("size_in_node")
    private Map<String, Long> sizeInNode;
    @JsonProperty("segment_id")
    private String segmentId;
    @JsonProperty("id")
    private String id;
    @JsonProperty("node_file_map")
    private Map<String, List<SegmentFileStatus>> nodeFileMap;
    @JsonProperty("secondary_index_columns")
    private Set<Integer> secondaryIndexColumns = Sets.newHashSet();

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Long> getSizeInNode() {
        return sizeInNode== null ? Collections.emptyMap() : sizeInNode;
    }

    public List<String> getShardNodes() {
        return Collections.unmodifiableList(shardNodes);
    }

    public static final class Builder {
        private List<String> shardNodes;
        private String segmentId;
        private String id;
        private Map<String, Long> sizeInNode;
        private Map<String, List<SegmentFileStatus>> nodeFileMap;
        private Set<Integer> secondaryIndexColumns = Sets.newHashSet();

        public Builder setSegmentId(String segmentId) {
            this.segmentId = segmentId;
            return this;
        }
        public Builder setShardNodes(List<String> shardNodes) {
            this.shardNodes = shardNodes;
            return this;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setNodeFileMap(Map<String, List<SegmentFileStatus>> nodeFileMap) {
            this.nodeFileMap = nodeFileMap;
            return this;
        }

        public Builder setSizeInNode(Map<String, Long> sizeInNode) {
            this.sizeInNode = sizeInNode;
            return this;
        }

        public Builder setSecondaryIndexColumns(Set<Integer> secondaryIndexColumns) {
            this.secondaryIndexColumns = secondaryIndexColumns;
            return this;
        }

        public TablePartition build() {
            TablePartition partition = new TablePartition();
            partition.shardNodes.addAll(shardNodes);
            partition.segmentId = segmentId;
            partition.id = id;
            partition.nodeFileMap = nodeFileMap;
            partition.sizeInNode = sizeInNode;
            partition.secondaryIndexColumns = secondaryIndexColumns;
            return partition;
        }
    }
    public String getSegmentId() {
        return segmentId;
    }

    public String getId() {
        return id;
    }

    public Map<String, List<SegmentFileStatus>> getNodeFileMap() {
        return nodeFileMap == null ? Collections.emptyMap() : nodeFileMap;
    }

    public void removeSecondaryIndex(Set<Integer> cols) {
        this.secondaryIndexColumns.removeAll(cols);
    }

    public Set<Integer> getSecondaryIndexColumns() {
        return this.secondaryIndexColumns;
    }

    public void addSecondaryIndex(Set<Integer> cols) {
        this.secondaryIndexColumns.addAll(cols);
    }
}
