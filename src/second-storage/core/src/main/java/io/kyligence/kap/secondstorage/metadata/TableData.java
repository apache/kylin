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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.kylin.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;
import scala.collection.JavaConverters;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@DataDefinition
public class TableData implements Serializable, WithLayout {


    public static final class Builder {

        private LayoutEntity layoutEntity;
        private PartitionType partitionType;
        private String database;
        private String table;

        public Builder setLayoutEntity(LayoutEntity layoutEntity) {
            this.layoutEntity = layoutEntity;
            return this;
        }

        public Builder setPartitionType(PartitionType partitionType) {
            this.partitionType = partitionType;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public TableData build() {
            TableData tableData = new TableData();
            tableData.layoutID = layoutEntity.getId();
            tableData.partitionType = this.partitionType;
            tableData.table = this.table;
            tableData.database = this.database;
            return tableData;
        }
    }
    public static Builder builder() {
        return new Builder();
    }

    @JsonBackReference
    private TableFlow tableFlow;

    @JsonProperty("layout_id")
    private long layoutID;

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("partition_type")
    private PartitionType partitionType;

    @JsonProperty("partitions")
    private final List<TablePartition> partitions = Lists.newArrayList();

    private Set<String> allSegmentIds;

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    @Override
    public long getLayoutID() {
        return layoutID;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    protected void checkIsNotCachedAndShared() {
        if (tableFlow != null)
            tableFlow.checkIsNotCachedAndShared();
    }

    // create
   public void addPartition(TablePartition partition) {
       Preconditions.checkArgument(partition != null);
       checkIsNotCachedAndShared();
       partitions.removeIf(p ->
               isSameGroup(p.getShardNodes(), partition.getShardNodes()) && p.getSegmentId().equals(partition.getSegmentId())
       );
       partitions.add(partition);
   }

    // read
    public List<TablePartition> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }

    // update & delete
    public void removePartitions(Predicate<? super TablePartition> filter) {
        checkIsNotCachedAndShared();
        partitions.removeIf(filter);
    }

    public void mergePartitions(Set<String> oldSegIds, String mergeSegId) {
        checkIsNotCachedAndShared();
        List<Set<String>> nodeGroups = new ArrayList<>();
        Map<String, Long> sizeInNode = new HashMap<>();
        Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>();
        getPartitions().stream().filter(partition -> oldSegIds.contains(partition.getSegmentId()))
                .forEach(partition -> {
                    partition.getSizeInNode().forEach((key, value) ->
                        sizeInNode.put(key, sizeInNode.getOrDefault(key, 0L) + value));

                    partition.getNodeFileMap().forEach((key, value) ->
                        nodeFileMap.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));

                    for (Set<String> nodeGroup : nodeGroups) {
                        if (isSameGroup(nodeGroup, partition.getShardNodes())) {
                            nodeGroup.addAll(partition.getShardNodes());
                            return;
                        }
                    }
                    nodeGroups.add(new HashSet<>(partition.getShardNodes()));
                });
        List<TablePartition> mergedPartitions = nodeGroups.stream().map(nodeGroup ->
                TablePartition.builder()
                    .setSizeInNode(nodeGroup.stream().collect(
                            Collectors.toMap(node -> node, node -> sizeInNode.getOrDefault(node, 0L))))
                    .setSegmentId(mergeSegId)
                    .setShardNodes(Lists.newArrayList(nodeGroup))
                    .setId(RandomUtil.randomUUIDStr())
                    .setNodeFileMap(nodeGroup.stream().collect(
                            Collectors.toMap(node -> node, node -> nodeFileMap.getOrDefault(node, Collections.emptyList()))
                    )).build()
        ).collect(Collectors.toList());
        removePartitions(tablePartition -> oldSegIds.contains(tablePartition.getSegmentId()));
        this.partitions.addAll(mergedPartitions);
    }

    // utility
    private boolean isSameGroup(Collection<String> a, Collection<String> b) {
        Set<String> aSet = Sets.newHashSet(a);
        Set<String> bSet = Sets.newHashSet(b);
        return aSet.containsAll(bSet) || bSet.containsAll(aSet);
    }

    public String getShardJDBCURLs(String project, Set<String> allSegIds) {
        if (partitions.isEmpty()) {
            return null;
        }
        List<Set<String>> aliveShardReplica = SecondStorageQueryRouteUtil.getUsedShard(partitions, project, allSegIds);
        QueryContext queryContext = QueryContext.current();
        List<String> jdbcUrls = SecondStorageNodeHelper.resolveShardToJDBC(aliveShardReplica, queryContext);

        if (!CollectionUtils.isEmpty(queryContext.getSecondStorageUrls())) {
            QueryContext.current().setRetrySecondStorage(false);
        }
        QueryContext.current().setSecondStorageUrls(jdbcUrls);
        return ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(jdbcUrls));
    }

    public String getSchemaURL() {
        if (partitions.isEmpty()) {
            return null;
        }

        return QueryContext.current().getSecondStorageUrls().get(0);
    }

    public int getSchemaURLSize() {
        if (partitions.isEmpty()) {
            return 0;
        }

        return QueryContext.current().getSecondStorageUrls().size();
    }

    public boolean containSegments(Set<String> segmentIds) {
        if (allSegmentIds == null) {
            allSegmentIds = partitions.stream().map(TablePartition::getSegmentId).collect(Collectors.toSet());
        }
        return allSegmentIds.containsAll(segmentIds);
    }

    public Set<String> getAllSegments() {
        if (allSegmentIds == null) {
            allSegmentIds = partitions.stream().map(TablePartition::getSegmentId).collect(Collectors.toSet());
        }

        return Collections.unmodifiableSet(allSegmentIds);
    }

    public void removeNodes(List<String> nodeNames) {
        if (CollectionUtils.isEmpty(nodeNames)) {
            return;
        }

        List<TablePartition> newPartitionList = getPartitions().stream().map(partition -> {
            Map<String, Long> sizeInNode = new HashMap<>(partition.getSizeInNode());
            Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>(partition.getNodeFileMap());
            List<String> shardNodes = new ArrayList<>(partition.getShardNodes());

            nodeNames.forEach(nodeName -> {
                long size = partition.getSizeInNode().getOrDefault(nodeName, 0L);
                if (size != 0) {
                    throw new KylinException(ServerErrorCode.SECOND_STORAGE_DELETE_NODE_FAILED,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageDeleteNodeFailed(), nodeName, size));
                }

                sizeInNode.remove(nodeName);
                nodeFileMap.remove(nodeName);
            });

            shardNodes.removeAll(nodeNames);

            return new TablePartition.Builder()
                    .setId(partition.getId())
                    .setSegmentId(partition.getSegmentId())
                    .setShardNodes(shardNodes)
                    .setSizeInNode(sizeInNode)
                    .setNodeFileMap(nodeFileMap)
                    .build();
        }).collect(Collectors.toList());

        newPartitionList.forEach(this::addPartition);
    }
}
