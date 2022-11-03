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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.datatype.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.kyligence.kap.secondstorage.ColumnMapping;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.metadata.model.NDataModel;

@Slf4j
public class DataLoader {

    static String clickHouseType(DataType dt) {
        switch (dt.getName()) {
        case DataType.BOOLEAN:
            return "UInt8";
        case DataType.BYTE:
        case DataType.TINY_INT:
            return "Int8";
        case DataType.SHORT:
        case DataType.SMALL_INT:
            return "Int16";
        case DataType.INT:
        case DataType.INT4:
        case DataType.INTEGER:
            return "Int32";
        case DataType.LONG:
        case DataType.LONG8:
        case DataType.BIGINT:
            return "Int64";
        case DataType.FLOAT:
            return "Float32";
        case DataType.DOUBLE:
            return "Float64";
        case DataType.DECIMAL:
        case DataType.NUMERIC:
            return String.format(Locale.ROOT, "Decimal(%d,%d)", dt.getPrecision(), dt.getScale());
        case DataType.VARCHAR:
        case DataType.CHAR:
        case DataType.STRING:
            return "String";
        case DataType.DATE:
            return "Date";
        case DataType.TIMESTAMP:
        case DataType.DATETIME:
            return "DateTime";
        case DataType.TIME:
        case DataType.REAL:
        case DataType.ANY_STR:
        case DataType.BINARY:
        case DataType.ARRAY:
            throw new UnsupportedOperationException("will support");
        default:
        }
        throw new UnsupportedOperationException("");
    }

    static List<ColumnWithType> columns(Map<String, String> columnTypeMap, LayoutEntity layout, String partitionCol, boolean addPrefix) {
        List<ColumnWithType> cols = new ArrayList<>();
        layout.getOrderedDimensions()
                .forEach((k, v) -> {
                    String colType = columnTypeMap.get(getPrefixColumn(String.valueOf(k)));
                    cols.add(new ColumnWithType(
                        addPrefix ? getPrefixColumn(String.valueOf(k)) : String.valueOf(k),
                        colType == null ? clickHouseType(v.getType()) : colType,
                        // partition column must not be null
                        colType == null && v.getColumnDesc().isNullable() && !String.valueOf(k).equals(partitionCol),
                        true));
                });
        return cols;
    }

    static List<String> orderColumns(LayoutEntity layout, List<Integer> orderCols, boolean addPrefix) {
        val orderedDimensions = layout.getOrderedDimensions();
        return orderCols.stream().filter(orderedDimensions::containsKey)
                .map(column -> addPrefix ? getPrefixColumn(String.valueOf(column)) : String.valueOf(column))
                .collect(Collectors.toList());
    }

    static String getPrefixColumn(String col) {
        return ColumnMapping.kapColumnToSecondStorageColumn(col);
    }

    private final String executableId;
    private final String database;
    private final Engine tableEngine;
    private final boolean isIncremental;
    private final List<LoadInfo> loadInfoBatch;
    private final LoadContext loadContext;
    private final List<ShardLoader> shardLoaders;
    private final Map<String, List<ClickhouseLoadFileLoad>> singleFileLoaderPerNode;
    @Getter
    private final LoadContext.CompletedSegmentKeyUtil segmentKey;
    @Getter
    private final String segmentId;

    public DataLoader(String executableId,
                      String database,
                      Engine tableEngine,
                      boolean isIncremental,
                      List<LoadInfo> loadInfoBatch,
                      LoadContext loadContext) {
        this.executableId = executableId;
        this.database = database;
        this.tableEngine = tableEngine;
        this.isIncremental = isIncremental;
        this.loadInfoBatch = loadInfoBatch;
        this.loadContext = loadContext;
        int totalJdbcNum = loadInfoBatch.stream().mapToInt(item -> item.getNodeNames().length).sum();
        this.shardLoaders = new ArrayList<>(totalJdbcNum + 2);
        this.segmentId = loadInfoBatch.get(0).getSegmentId();
        this.segmentKey = new LoadContext.CompletedSegmentKeyUtil(loadInfoBatch.get(0).getLayout().getId());
        this.singleFileLoaderPerNode = new HashMap<>();
        toSingleFileLoaderPerNode();
    }

    public List<ShardLoader> getShardLoaders() {
        return shardLoaders == null ? Collections.emptyList() : shardLoaders;
    }

    public Map<String, List<ClickhouseLoadFileLoad>> getSingleFileLoaderPerNode() {
        return singleFileLoaderPerNode;
    }

    private void toSingleFileLoaderPerNode() {
        // skip segment when committed
        if (loadContext.getHistorySegments(segmentKey).contains(this.segmentId)) {
            return;
        }

        // After new shard is created, JDBC Connection is Ready.
        for (val loadInfo : loadInfoBatch) {
            String[] nodeNames = loadInfo.getNodeNames();
            Preconditions.checkArgument(nodeNames.length == loadInfo.getShardFiles().size());

            for (int idx = 0; idx < nodeNames.length; idx++) {
                final String nodeName = nodeNames[idx];
                final List<String> listParquet = loadInfo.getShardFiles().get(idx);

                val builder = ShardLoader.ShardLoadContext.builder().nodeName(nodeName)
                        .jdbcURL(SecondStorageNodeHelper.resolve(nodeName)).executableId(executableId)
                        .segmentId(loadInfo.segmentId).database(database).layout(loadInfo.getLayout())
                        .parquetFiles(listParquet).tableEngine(tableEngine).destTableName(loadInfo.getTargetTable())
                        .loadContext(loadContext).tableEntity(loadInfo.getTableEntity());
                if (isIncremental) {
                    String partitionColName = loadInfo.model.getPartitionDesc().getPartitionDateColumn();
                    val dateCol = loadInfo.model.getAllNamedColumns().stream()
                            .filter(column -> column.getAliasDotColumn().equals(partitionColName)
                                    && NDataModel.ColumnStatus.DIMENSION.equals(column.getStatus()))
                            .findFirst().orElseThrow(
                                    () -> new IllegalStateException("can't find partition column " + partitionColName));
                    Preconditions.checkState(loadInfo.getLayout().getColumns().stream()
                            .anyMatch(col -> col.getAliasDotName().equals(dateCol.getAliasDotColumn())));
                    Preconditions.checkArgument(loadInfo.segment.getSegRange().getStart() instanceof Long);
                    builder.partitionFormat(loadInfo.model.getPartitionDesc().getPartitionDateFormat())
                            .partitionColumn(Objects.toString(dateCol.getId()))
                            .targetPartitions(SecondStorageDateUtils.splitByDay(loadInfo.segment.getSegRange()));
                }

                val needDropTable = getNeedDropTable(loadInfo);
                builder.needDropTable(needDropTable);
                builder.needDropPartition(getNeedDropPartition(loadInfo, needDropTable));

                ShardLoader.ShardLoadContext context = builder.build();
                List<ClickhouseLoadFileLoad> clickhouseLoadFileLoads = singleFileLoaderPerNode.computeIfAbsent(nodeName, k -> new ArrayList<>());
                ShardLoader shardLoader = new ShardLoader(context);

                clickhouseLoadFileLoads.addAll(shardLoader.toSingleFileLoader());
                shardLoaders.add(shardLoader);
            }
        }
    }

    public Map<String, List<ClickhouseLoadPartitionDrop>> getLoadCommitDropPartitions() {
        Map<String, List<ClickhouseLoadPartitionDrop>> dropPartitions = new HashMap<>();
        for (ShardLoader shardLoader : this.getShardLoaders()) {
            ImmutableList.Builder<String> needDropPartitionTableBuilder = ImmutableList.builder();
            needDropPartitionTableBuilder.add(shardLoader.getDestTableName());
            if (shardLoader.getNeedDropPartition() != null) {
                needDropPartitionTableBuilder.addAll(shardLoader.getNeedDropPartition());
            }
            val needDropPartitionTables = needDropPartitionTableBuilder.build();

            if (shardLoader.isIncremental()) {
                List<ClickhouseLoadPartitionDrop> dropPartitionShard = dropPartitions
                        .computeIfAbsent(shardLoader.getNodeName(), k -> new ArrayList<>());
                dropPartitionShard.addAll(shardLoader.getTargetPartitions().stream().map(
                        partition -> new ClickhouseLoadPartitionDrop(needDropPartitionTables, partition, shardLoader))
                        .collect(Collectors.toList()));
            }
        }
        return dropPartitions;
    }

    public Map<String, List<ClickhouseLoadPartitionCommit>> getLoadCommitMovePartitions() throws SQLException {
        Map<String, List<ClickhouseLoadPartitionCommit>> movePartitions = new HashMap<>();
        for (ShardLoader shardLoader : this.getShardLoaders()) {
            List<ClickhouseLoadPartitionCommit> movePartitionNode = movePartitions
                    .computeIfAbsent(shardLoader.getNodeName(), k -> new ArrayList<>());
            if (shardLoader.isIncremental()) {
                movePartitionNode.addAll(shardLoader.getInsertTempTablePartition().stream()
                        .map(partition -> new ClickhouseLoadPartitionCommit(partition, shardLoader))
                        .collect(Collectors.toList()));
            } else {
                movePartitionNode.add(new ClickhouseLoadPartitionCommit(null, shardLoader));
            }
        }
        return movePartitions;
    }

    public Map<String, List<ClickhouseLoadPartitionDrop>> getLoadCommitExceptionPartitions() throws SQLException {
        Map<String, List<ClickhouseLoadPartitionDrop>> movePartitions = new HashMap<>();
        for (ShardLoader shardLoader : this.getShardLoaders()) {
            if (shardLoader.isIncremental()) {
                List<ClickhouseLoadPartitionDrop> dropPartitionShard = movePartitions
                        .computeIfAbsent(shardLoader.getNodeName(), k -> new ArrayList<>());
                dropPartitionShard.addAll(shardLoader.getInsertTempTablePartition().stream()
                        .map(partition -> new ClickhouseLoadPartitionDrop(
                                Collections.singletonList(shardLoader.getDestTableName()), partition, shardLoader))
                        .collect(Collectors.toList()));
            }
        }
        return movePartitions;
    }

    private Set<String> getNeedDropTable(LoadInfo loadInfo) {
        return loadInfo.containsOldSegmentTableData.stream().filter(tableData -> {
            val all = tableData.getAllSegments();
            return tableData.getLayoutID() != loadInfo.getLayout().getId() && all.size() == 1 && all.contains(loadInfo.oldSegmentId);
        }).map(TableData::getTable).collect(Collectors.toSet());
    }

    private Set<String> getNeedDropPartition(LoadInfo loadInfo, Set<String> needDropTable) {
        return loadInfo.containsOldSegmentTableData.stream()
                .filter(tableData -> tableData.getLayoutID() != loadInfo.getLayout().getId() && !needDropTable.contains(tableData.getTable()))
                .map(TableData::getTable)
                .collect(Collectors.toSet());
    }
}
