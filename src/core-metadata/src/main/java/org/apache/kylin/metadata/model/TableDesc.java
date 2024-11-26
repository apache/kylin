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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.metadata.table.ATable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Table Metadata from Source. All name should be uppercase.
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class TableDesc extends ATable implements Serializable, ISourceAware {

    public static final String TABLE_TYPE_VIEW = "VIEW";
    public static final long NOT_READY = -1;
    private static final String TRANSACTIONAL_TABLE_NAME_SUFFIX = "_hive_tx_intermediate";

    // returns <table, project>
    public static Pair<String, String> parseResourcePath(String path) {
        if (path.endsWith(".json"))
            path = path.substring(0, path.length() - ".json".length());

        int cut = path.lastIndexOf("/");
        if (cut >= 0)
            path = path.substring(cut + 1);

        String table;
        String prj;
        int dash = path.indexOf("--");
        if (dash >= 0) {
            table = path.substring(0, dash);
            prj = path.substring(dash + 2);
        } else {
            table = path;
            prj = null;
        }
        return Pair.newPair(table, prj);
    }

    // ============================================================================

    @JsonProperty("source_type")
    private int sourceType = ISourceAware.ID_HIVE;

    @JsonProperty("table_type")
    private String tableType;

    private Boolean hasInternal;

    private boolean hasInternalDeprecated = false;

    //Sticky table
    @Getter
    @Setter
    @JsonProperty("top")
    private boolean isTop;
    @JsonProperty("data_gen")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dataGen;

    @Getter
    @Setter
    @JsonProperty("increment_loading")
    private boolean incrementLoading;

    @Getter
    @Setter
    @JsonProperty("last_snapshot_path")
    private String lastSnapshotPath;

    @Getter
    @Setter
    @JsonProperty("last_snapshot_size")
    private long lastSnapshotSize;

    @Setter
    @JsonProperty("snapshot_last_modified")
    private long snapshotLastModified;

    @Getter
    @Setter
    @JsonProperty("query_hit_count")
    private int snapshotHitCount = 0;

    //first level partition col for this table
    @Setter
    @Getter
    @JsonProperty("partition_column")
    private String partitionColumn;

    @Getter
    @Setter
    @JsonProperty("snapshot_partitions")
    private Map<String, Long> snapshotPartitions = Maps.newHashMap();

    @Getter
    @Setter
    @JsonProperty("snapshot_partitions_info")
    private Map<String, SnapshotPartitionInfo> snapshotPartitionsInfo = Maps.newHashMap();

    @Getter
    @Setter
    @JsonProperty("snapshot_total_rows")
    private long snapshotTotalRows;

    //partition col of current built snapshot
    @Getter
    @Setter
    @JsonProperty("snapshot_partition_col")
    private String snapshotPartitionCol;

    // user select partition for this snapshot table
    @Setter
    @Getter
    @JsonProperty("selected_snapshot_partition_col")
    private String selectedSnapshotPartitionCol;

    @Setter
    @Getter
    @JsonProperty("temp_snapshot_path")
    private String tempSnapshotPath;

    @Setter
    @Getter
    @JsonProperty("snapshot_has_broken")
    private boolean snapshotHasBroken;

    private KafkaConfig kafkaConfig;

    @Setter
    @Getter
    @JsonProperty("transactional")
    private boolean isTransactional;

    private Boolean isRangePartition;

    private boolean rangePartitionDeprecated;

    @Setter
    @Getter
    @JsonProperty("partition_desc")
    private PartitionDesc partitionDesc;

    @Getter
    @Setter
    @JsonProperty("table_comment")
    private String tableComment;

    public TableDesc() {
    }

    public TableDesc(TableDesc other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.createTime = other.createTime;
        this.name = other.name;
        this.hasInternal = other.hasInternal;
        this.hasInternalDeprecated = other.hasInternalDeprecated;
        this.rangePartitionDeprecated = other.rangePartitionDeprecated;
        this.sourceType = other.sourceType;
        this.tableType = other.tableType;
        this.dataGen = other.dataGen;
        this.incrementLoading = other.incrementLoading;
        this.columns = new ColumnDesc[other.columns.length];
        for (int i = 0; i < other.columns.length; i++) {
            this.columns[i] = new ColumnDesc(other.columns[i]);
            this.columns[i].init(this);
        }
        this.isTop = other.isTop;
        this.project = other.project;
        this.database.setName(other.getDatabase());
        this.identity = other.identity;
        this.lastSnapshotPath = other.lastSnapshotPath;
        this.lastSnapshotSize = other.lastSnapshotSize;
        this.partitionColumn = other.partitionColumn;
        this.snapshotPartitions = other.snapshotPartitions;
        this.snapshotPartitionsInfo = other.snapshotPartitionsInfo;
        this.snapshotTotalRows = other.snapshotTotalRows;
        this.selectedSnapshotPartitionCol = other.selectedSnapshotPartitionCol;
        this.snapshotPartitionCol = other.snapshotPartitionCol;
        this.snapshotLastModified = other.getSnapshotLastModified();
        this.snapshotHasBroken = other.snapshotHasBroken;
        this.kafkaConfig = other.kafkaConfig;
        this.isTransactional = other.isTransactional;
        this.isRangePartition = other.isRangePartition;
        this.partitionDesc = other.partitionDesc;
        this.tableComment = other.tableComment;
        setMvcc(other.getMvcc());
    }

    @JsonGetter("range_partition")
    public boolean isRangePartition() {
        return isRangePartition == null ? rangePartitionDeprecated : isRangePartition;
    }

    @JsonSetter("range_partition")
    public void setRangePartition(boolean isRangePartition) {
        this.isRangePartition = isRangePartition;
    }

    @JsonGetter("has_internal")
    public boolean isHasInternal() {
        return hasInternal == null ? hasInternalDeprecated : hasInternal;
    }

    @JsonSetter("has_internal")
    public void setHasInternal(boolean hasInternal) {
        this.hasInternal = hasInternal;
    }

    /**
     * This setter exists for compatibility with older data versions
     * that utilize the has_Internal json field. Please use {@link TableDesc#setHasInternal} instead.
     * @deprecated since 5.2.2
     */
    @JsonSetter("has_Internal")
    @Deprecated
    public void setHasInternalDeprecated(boolean hasInternalDeprecated) {
        this.hasInternalDeprecated = hasInternalDeprecated;
    }

    /**
     * This setter exists for compatibility with older data versions
     * that utilize the rangePartition json field. Please use {@link TableDesc#setRangePartition} instead.
     * @deprecated since 5.2.2
     */
    @JsonSetter("rangePartition")
    @Deprecated
    public void setRangePartitionDeprecated(boolean rangePartitionDeprecated) {
        this.rangePartitionDeprecated = rangePartitionDeprecated;
    }

    /**
     * Streaming table can't be accessed when streaming disabled
     */
    public boolean isAccessible(boolean turnOnStreaming) {
        return turnOnStreaming || getSourceType() != ID_STREAMING;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.TABLE_INFO;
    }

    public TableDesc appendColumns(ColumnDesc[] computedColumns, boolean makeCopy) {
        if (ArrayUtils.isEmpty(computedColumns)) {
            return this;
        }

        TableDesc ret = makeCopy ? new TableDesc(this) : this;
        ColumnDesc[] existingColumns = ret.columns;
        List<ColumnDesc> newColumns = Lists.newArrayList();

        for (ColumnDesc computedColumn : computedColumns) {

            //check name conflict
            boolean isFreshCC = true;
            for (ColumnDesc existingColumn : existingColumns) {
                if (!StringUtils.equalsIgnoreCase(existingColumn.getName(), computedColumn.getName())) {
                    continue;
                }
                // if we're adding a computed column twice, it should be allowed without producing duplicates
                if (!existingColumn.isComputedColumn()) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "There is already a column named %s on table %s, please change your computed column name",
                            computedColumn.getName(), this.getIdentity()));
                } else {
                    isFreshCC = false;
                }
            }

            if (isFreshCC) {
                newColumns.add(computedColumn);
            }
        }

        List<ColumnDesc> expandedColumns = Lists.newArrayList(existingColumns);
        for (ColumnDesc newColumnDesc : newColumns) {
            newColumnDesc.init(ret);
            expandedColumns.add(newColumnDesc);
        }
        ret.columns = expandedColumns.toArray(new ColumnDesc[0]);
        return ret;
    }

    public ColumnDesc findColumnByName(String name) {
        //ignore the db name and table name if exists
        int lastIndexOfDot = name.lastIndexOf(".");
        if (lastIndexOfDot >= 0) {
            name = name.substring(lastIndexOfDot + 1);
        }

        for (ColumnDesc c : columns) {
            // return first matched column
            if (name.equalsIgnoreCase(c.getOriginalName())) {
                return c;
            }
        }
        return null;
    }

    public String getBackTickIdentity() {
        return getBackTickCaseSensitiveIdentity("");
    }

    public String getDoubleQuoteIdentity() {
        return getDoubleQuoteCaseSensitiveIdentity("");
    }

    public String getBackTickTransactionalTableIdentity(String suffix) {
        return getBackTickCaseSensitiveIdentity(TRANSACTIONAL_TABLE_NAME_SUFFIX.toUpperCase(Locale.ROOT) + suffix);
    }

    public String getBackTickTransactionalTableIdentity(String temporaryWritableDB, String suffix) {
        String fullSuffix = TRANSACTIONAL_TABLE_NAME_SUFFIX.toUpperCase(Locale.ROOT) + suffix;
        if (StringUtils.isNotBlank(temporaryWritableDB)) {
            return String.format(Locale.ROOT, "`%s`.`%s`", temporaryWritableDB.toUpperCase(Locale.ROOT),
                    getCaseSensitiveName() + fullSuffix);
        }
        return getBackTickTransactionalTableIdentity(suffix);
    }

    private String getBackTickCaseSensitiveIdentity(String suffix) {
        return "null".equals(this.getCaseSensitiveDatabase())
                ? String.format(Locale.ROOT, "`%s`", this.getCaseSensitiveName())
                : String.format(Locale.ROOT, "`%s`.`%s`", this.getCaseSensitiveDatabase(),
                        this.getCaseSensitiveName() + suffix);
    }

    private String getDoubleQuoteCaseSensitiveIdentity(String suffix) {
        return "null".equals(this.getCaseSensitiveDatabase())
                ? String.format(Locale.ROOT, "\"%s\"", this.getCaseSensitiveName())
                : String.format(Locale.ROOT, "\"%s\".\"%s\"", this.getCaseSensitiveDatabase(),
                        this.getCaseSensitiveName() + suffix);
    }

    public boolean isView() {
        return StringUtils.containsIgnoreCase(tableType, TABLE_TYPE_VIEW);
    }

    public int getMaxColumnIndex() {
        if (columns == null) {
            return -1;
        }

        int max = -1;

        for (ColumnDesc col : columns) {
            int idx = col.getZeroBasedIndex();
            max = Math.max(max, idx);
        }
        return max;
    }

    public int getColumnCount() {
        return getMaxColumnIndex() + 1;
    }

    public String getDataGen() {
        return dataGen;
    }

    public long getSnapshotLastModified() {
        if (snapshotLastModified == 0) {
            return lastModified;
        }
        return snapshotLastModified;
    }

    @Override
    public void init(String project) {
        super.init(project);
        if (sourceType == ISourceAware.ID_STREAMING) {
            kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getKafkaConfig(this.getIdentity());
        }
    }

    @Override
    public int hashCode() {
        return getTableAlias().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TableDesc tableDesc = (TableDesc) o;

        if (sourceType != tableDesc.sourceType)
            return false;
        if (!Objects.equals(name, tableDesc.name))
            return false;
        if (!Arrays.equals(columns, tableDesc.columns))
            return false;

        return getIdentity().equals(tableDesc.getIdentity());

    }

    public String getTransactionalTableIdentity() {
        return (getIdentity() + TRANSACTIONAL_TABLE_NAME_SUFFIX).toUpperCase(Locale.ROOT);
    }

    public String getTransactionalTableIdentity(String temporaryWritableDB) {
        if (StringUtils.isNotBlank(temporaryWritableDB)) {
            return (String.format(Locale.ROOT, "%s.%s", temporaryWritableDB, getCaseSensitiveName())
                    + TRANSACTIONAL_TABLE_NAME_SUFFIX).toUpperCase(Locale.ROOT);
        }
        return getTransactionalTableIdentity();
    }

    public String getTransactionalTableName() {
        return (getName() + TRANSACTIONAL_TABLE_NAME_SUFFIX).toUpperCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return "TableDesc{" + "name='" + name + '\'' + ", columns=" + Arrays.toString(columns) + ", sourceType="
                + sourceType + ", tableType='" + tableType + '\'' + ", database=" + database + ", identity='"
                + getIdentity() + '\'' + '}';
    }

    /** create a mockup table for unit test */
    public static TableDesc mockup(String tableName) {
        TableDesc mockup = new TableDesc();
        mockup.setName(tableName);
        return mockup;
    }

    @Override
    public KylinConfig getConfig() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(this.project);
        return projectInstance.getConfig();
    }

    @Override
    public int getSourceType() {
        return sourceType;
    }

    public void setSourceType(int sourceType) {
        this.sourceType = sourceType;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public void deleteSnapshot(boolean makeBroken) {
        this.lastSnapshotPath = null;
        snapshotPartitionCol = null;
        snapshotPartitions = Maps.newHashMap();
        snapshotPartitionsInfo = Maps.newHashMap();
        selectedSnapshotPartitionCol = null;
        lastSnapshotSize = 0;
        snapshotHasBroken = makeBroken;
    }

    public void copySnapshotFrom(TableDesc originTable) {
        setLastSnapshotPath(originTable.getLastSnapshotPath());
        setLastSnapshotSize(originTable.getLastSnapshotSize());
        setSnapshotPartitions(originTable.getSnapshotPartitions());
        setSnapshotPartitionCol(originTable.getSnapshotPartitionCol());
        setSelectedSnapshotPartitionCol(originTable.getSelectedSnapshotPartitionCol());
        setSnapshotLastModified(originTable.getSnapshotLastModified());
        setSnapshotHasBroken(originTable.isSnapshotHasBroken());
    }

    public void resetSnapshotPartitions(Set<String> snapshotPartitions) {
        this.snapshotPartitions = snapshotPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), item -> NOT_READY));
        this.snapshotPartitionsInfo.clear();

    }

    public void putPartitionSize(String partition, long size) {
        snapshotPartitions.put(partition, size);
    }

    public void putPartitionRow(String partition, long row) {
        SnapshotPartitionInfo snapshotPartitionInfo = snapshotPartitionsInfo.get(partition);
        if (snapshotPartitionInfo != null) {
            snapshotPartitionInfo.setTotalRows(row);
        } else {
            snapshotPartitionInfo = new SnapshotPartitionInfo();
            snapshotPartitionInfo.setTotalRows(row);
            snapshotPartitionsInfo.put(partition, snapshotPartitionInfo);
        }

    }

    public long getPartitionRow(String partition) {
        SnapshotPartitionInfo snapshotPartitionInfo = snapshotPartitionsInfo.get(partition);
        if (snapshotPartitionInfo != null) {
            return snapshotPartitionInfo.getTotalRows();
        }
        return 0;
    }

    public void addSnapshotPartitions(Set<String> snapshotPartitions) {
        snapshotPartitions.forEach(part -> this.snapshotPartitions.put(part, NOT_READY));
    }

    public Set<String> getNotReadyPartitions() {
        Set<String> notReadyPartitions = Sets.newHashSet();
        snapshotPartitions.forEach((item, ready) -> {
            if (ready == NOT_READY) {
                notReadyPartitions.add(item);
            }
        });
        return notReadyPartitions;
    }

    public Set<String> getReadyPartitions() {
        Set<String> readyPartitions = Sets.newHashSet();
        snapshotPartitions.forEach((item, ready) -> {
            if (ready != NOT_READY) {
                readyPartitions.add(item);
            }
        });
        return readyPartitions;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SnapshotPartitionInfo implements Serializable {

        @JsonProperty("total_rows")
        private long totalRows;

    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public boolean isKafkaTable() {
        return getSourceType() == ISourceAware.ID_STREAMING && getKafkaConfig() != null;
    }

    public String getTableAlias() {
        if (kafkaConfig != null && kafkaConfig.hasBatchTable()) {
            return kafkaConfig.getBatchTable();
        }
        return getIdentity();
    }

    public boolean isLogicalView() {
        return KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB().equalsIgnoreCase(this.getDatabase());
    }
}
