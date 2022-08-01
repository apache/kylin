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

import com.google.common.base.Preconditions;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.NDataModel;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.SegmentFileStatus;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import lombok.val;
import org.apache.kylin.common.util.RandomUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadInfo {
    final NDataModel model;
    final NDataSegment segment;
    final String segmentId; // it is required for updating meta after load
    String oldSegmentId;
    final String[] nodeNames;
    final LayoutEntity layout;
    final List<List<SegmentFileStatus>> shardFiles;
    final TableFlow tableFlow;

    private String targetDatabase;
    private String targetTable;

    List<TableData> containsOldSegmentTableData = new ArrayList<>(10);

    @SuppressWarnings("unchecked")
    private static <T> List<T> newFixedSizeList(int size) {
        return (List<T>) Arrays.asList(new Object[size]);
    }

    private LoadInfo(NDataModel model, NDataSegment segment, LayoutEntity layout, String[] nodeNames, TableFlow tableFlow) {
        this(model, segment, null, layout, nodeNames, tableFlow);
    }

    private LoadInfo(NDataModel model, NDataSegment segment, String oldSegmentId, LayoutEntity layout,
            String[] nodeNames, TableFlow tableFlow) {
        this.model = model;
        this.segment = segment;
        final int shardNumber = nodeNames.length;
        this.nodeNames = nodeNames;
        this.segmentId = segment.getId();
        this.layout = layout;
        this.shardFiles = newFixedSizeList(shardNumber);
        this.oldSegmentId = oldSegmentId;
        for (int i = 0; i < shardNumber; ++i) {
            this.shardFiles.set(i, new ArrayList<>(100));
        }
        this.tableFlow = tableFlow;
    }

    /**
     * ClickHouse doesn't support the separation of storage and compute, so it's hard to scale horizontally.
     * It results in the long term use of a fixed number of shards. We have two cases:
     * <ul>
     *    <li>Full Load</li>
     *    <li>Incremental Load</li>
     * </ul>
     * The problem here is how to distribute files across multiple shards evenly in incremental load? Consider the
     * case where table index building always generates 10 parquet files every day, and unfortunately, we only have 3
     * shards. If we always distribute files from index 0, then shard 0 will have 10 more files than the other two after
     * ten days. i.e.
     * <ul>
     *     <li>shard 0: 40
     *     <li>shard 1: 30
     *     <li>shard 2: 40
     * </ul>
     *
     * TODO: Incremental Load
     * TODO: Use a simple way to avoid this issue -- randomly choose the start shard each time we distribute loads.
     * TODO: fault-tolerant for randomly choosing the start shard?
     *
     */

    public static LoadInfo distribute(String[] nodeNames, NDataModel model, NDataSegment segment, FileProvider provider,
            LayoutEntity layout, TableFlow tableFlow) {
        int shardNum = nodeNames.length;
        final LoadInfo info = new LoadInfo(model, segment, layout, nodeNames, tableFlow);
        val it = provider.getAllFilePaths().iterator();
        int index = 0;
        while (it.hasNext()) {
            FileStatus fileStatus = it.next();
            info.addShardFile(index, fileStatus.getPath(), fileStatus.getLen());
            index = ++index % shardNum;
        }
        return info;
    }

    public LoadInfo setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
        return this;
    }

    public LoadInfo setTargetTable(String targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public LoadInfo setOldSegmentId(String oldSegmentId) {
        this.oldSegmentId = oldSegmentId;

        this.tableFlow.getTableDataList().forEach(tableData -> {
            if (tableData.getAllSegments().contains(oldSegmentId)) {
                containsOldSegmentTableData.add(tableData);
            }
        });
        return this;
    }

    private void addShardFile(int shardIndex, String filePath, long fileLen) {
        Preconditions.checkArgument(shardIndex < shardFiles.size());
        shardFiles.get(shardIndex).add(SegmentFileStatus.builder().setLen(fileLen).setPath(filePath).build());
    }

    public LayoutEntity getLayout() {
        return layout;
    }

    public List<List<String>> getShardFiles() {
        return shardFiles.stream()
                .map(files -> files.stream().map(SegmentFileStatus::getPath).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    public String getSegmentId() {
        return segmentId;
    }

    public String[] getNodeNames() {
        return nodeNames;
    }

    // meta update
    public TablePartition createMetaInfo() {
        Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>();
        ListIterator<String> it = Arrays.asList(nodeNames).listIterator();
        while (it.hasNext()) {
            int idx = it.nextIndex();
            nodeFileMap.put(it.next(), shardFiles.get(idx));
        }
        val metric = new ClickHouseTableStorageMetric(Arrays.asList(this.nodeNames));
        metric.collect(false);

        Preconditions.checkNotNull(targetDatabase);
        Preconditions.checkNotNull(targetTable);
        String dateFormat = null;
        if (model.isIncrementBuildOnExpertMode()) {
            dateFormat = model.getPartitionDesc().getPartitionDateFormat();
        }
        Map<String, Long> sizeInNode = metric.getByPartitions(targetDatabase, targetTable, segment.getSegRange(), dateFormat);
        return TablePartition.builder().setSegmentId(segmentId).setShardNodes(Arrays.asList(nodeNames))
                .setId(RandomUtil.randomUUIDStr()).setNodeFileMap(nodeFileMap).setSizeInNode(sizeInNode).build();
    }

    public void upsertTableData(TableFlow copied, String database, String table, PartitionType partitionType) {
        copied.upsertTableData(layout, tableData -> {
            Preconditions.checkArgument(tableData.getPartitionType() == partitionType);
            if (tableData.getLayoutID() != layout.getId()) {
                return;
            }

            if (oldSegmentId != null) {
                tableData.removePartitions(tablePartition -> tablePartition.getSegmentId().equals(oldSegmentId));
            }
            tableData.addPartition(createMetaInfo());
        }, () -> TableData.builder().setPartitionType(partitionType).setLayoutEntity(getLayout()).setDatabase(database)
                .setTable(table).build());
    }
}
