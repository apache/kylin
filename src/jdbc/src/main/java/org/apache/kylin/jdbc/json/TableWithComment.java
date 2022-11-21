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

package org.apache.kylin.jdbc.json;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableWithComment {
    private static final long serialVersionUID = 1L;

    @JsonProperty("uuid")
    protected String uuid;

    @JsonProperty("last_modified")
    protected long lastModified;

    @JsonProperty("create_time")
    protected long createTime;

    @JsonProperty("version")
    protected String version;

    @JsonProperty("mvcc")
    private long mvcc;

    @JsonProperty("name")
    private String name;

    @JsonProperty("columns")
    private List<ColumnWithComment> columns;

    @JsonProperty("source_type")
    private int sourceType;

    @JsonProperty("kafka_bootstrap_servers")
    private String kafkaBootstrapServers;

    @JsonProperty("subscribe")
    private String subscribe;

    @JsonProperty("starting_offsets")
    private String startingOffsets;

    @JsonProperty("table_type")
    private String tableType;

    @JsonProperty("top")
    private boolean isTop;

    @JsonProperty("data_gen")
    private String dataGen;

    @JsonProperty("increment_loading")
    private boolean incrementLoading;

    @JsonProperty("last_snapshot_path")
    private String lastSnapshotPath;

    @JsonProperty("last_snapshot_size")
    private long lastSnapshotSize;

    @JsonProperty("snapshot_last_modified")
    private long snapshotLastModified;

    @JsonProperty("query_hit_count")
    private int snapshotHitCount;

    //first level partition col for this table
    @JsonProperty("partition_column")
    private String partitionColumn;

    @JsonProperty("snapshot_partitions")
    private Map<String, Long> snapshotPartitions;

    //partition col of current built snapshot
    @JsonProperty("snapshot_partition_col")
    private String snapshotPartitionCol;

    // user select partition for this snapshot table
    @JsonProperty("selected_snapshot_partition_col")
    private String selectedSnapshotPartitionCol;

    @JsonProperty("temp_snapshot_path")
    private String tempSnapshotPath;

    @Setter
    @Getter
    protected String project;

    @Setter
    @Getter
    private String identity;
    private String database;
    private boolean isBorrowedFromGlobal;

    @JsonProperty("exd")
    private Map<String, String> descExd;

    @JsonProperty("root_fact")
    private boolean rootFact;

    @JsonProperty("lookup")
    private boolean lookup;

    @JsonProperty("primary_key")
    private Set<String> primaryKey;

    @JsonProperty("foreign_key")
    private Set<String> foreignKey;

    @JsonProperty("partitioned_column")
    private String partitionedColumn;

    @JsonProperty("partitioned_column_format")
    private String partitionedColumnFormat;

    @JsonProperty("storage_size")
    private long storageSize;

    @JsonProperty("total_records")
    private long totalRecords;

    @JsonProperty("sampling_rows")
    private List<String[]> samplingRows;

    @JsonProperty("last_build_job_id")
    private String jodID;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getMvcc() {
        return mvcc;
    }

    public void setMvcc(long mvcc) {
        this.mvcc = mvcc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ColumnWithComment> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnWithComment> columns) {
        this.columns = columns;
    }

    public int getSourceType() {
        return sourceType;
    }

    public void setSourceType(int sourceType) {
        this.sourceType = sourceType;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getSubscribe() {
        return subscribe;
    }

    public void setSubscribe(String subscribe) {
        this.subscribe = subscribe;
    }

    public String getStartingOffsets() {
        return startingOffsets;
    }

    public void setStartingOffsets(String startingOffsets) {
        this.startingOffsets = startingOffsets;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public boolean isTop() {
        return isTop;
    }

    public void setTop(boolean top) {
        isTop = top;
    }

    public String getDataGen() {
        return dataGen;
    }

    public void setDataGen(String dataGen) {
        this.dataGen = dataGen;
    }

    public boolean isIncrementLoading() {
        return incrementLoading;
    }

    public void setIncrementLoading(boolean incrementLoading) {
        this.incrementLoading = incrementLoading;
    }

    public String getLastSnapshotPath() {
        return lastSnapshotPath;
    }

    public void setLastSnapshotPath(String lastSnapshotPath) {
        this.lastSnapshotPath = lastSnapshotPath;
    }

    public long getLastSnapshotSize() {
        return lastSnapshotSize;
    }

    public void setLastSnapshotSize(long lastSnapshotSize) {
        this.lastSnapshotSize = lastSnapshotSize;
    }

    public long getSnapshotLastModified() {
        return snapshotLastModified;
    }

    public void setSnapshotLastModified(long snapshotLastModified) {
        this.snapshotLastModified = snapshotLastModified;
    }

    public int getSnapshotHitCount() {
        return snapshotHitCount;
    }

    public void setSnapshotHitCount(int snapshotHitCount) {
        this.snapshotHitCount = snapshotHitCount;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public Map<String, Long> getSnapshotPartitions() {
        return snapshotPartitions;
    }

    public void setSnapshotPartitions(Map<String, Long> snapshotPartitions) {
        this.snapshotPartitions = snapshotPartitions;
    }

    public String getSnapshotPartitionCol() {
        return snapshotPartitionCol;
    }

    public void setSnapshotPartitionCol(String snapshotPartitionCol) {
        this.snapshotPartitionCol = snapshotPartitionCol;
    }

    public String getSelectedSnapshotPartitionCol() {
        return selectedSnapshotPartitionCol;
    }

    public void setSelectedSnapshotPartitionCol(String selectedSnapshotPartitionCol) {
        this.selectedSnapshotPartitionCol = selectedSnapshotPartitionCol;
    }

    public String getTempSnapshotPath() {
        return tempSnapshotPath;
    }

    public void setTempSnapshotPath(String tempSnapshotPath) {
        this.tempSnapshotPath = tempSnapshotPath;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public boolean isBorrowedFromGlobal() {
        return isBorrowedFromGlobal;
    }

    public void setBorrowedFromGlobal(boolean borrowedFromGlobal) {
        isBorrowedFromGlobal = borrowedFromGlobal;
    }

    public Map<String, String> getDescExd() {
        return descExd;
    }

    public void setDescExd(Map<String, String> descExd) {
        this.descExd = descExd;
    }

    public boolean isRootFact() {
        return rootFact;
    }

    public void setRootFact(boolean rootFact) {
        this.rootFact = rootFact;
    }

    public boolean isLookup() {
        return lookup;
    }

    public void setLookup(boolean lookup) {
        this.lookup = lookup;
    }

    public Set<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Set<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Set<String> getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(Set<String> foreignKey) {
        this.foreignKey = foreignKey;
    }

    public String getPartitionedColumn() {
        return partitionedColumn;
    }

    public void setPartitionedColumn(String partitionedColumn) {
        this.partitionedColumn = partitionedColumn;
    }

    public String getPartitionedColumnFormat() {
        return partitionedColumnFormat;
    }

    public void setPartitionedColumnFormat(String partitionedColumnFormat) {
        this.partitionedColumnFormat = partitionedColumnFormat;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public List<String[]> getSamplingRows() {
        return samplingRows;
    }

    public void setSamplingRows(List<String[]> samplingRows) {
        this.samplingRows = samplingRows;
    }

    public String getJodID() {
        return jodID;
    }

    public void setJodID(String jodID) {
        this.jodID = jodID;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnWithComment {

        @JsonProperty("id")
        private String id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("datatype")
        private String datatype;

        @JsonProperty("comment")
        private String comment;

        @JsonProperty("data_gen")
        private String dataGen;

        @JsonProperty("index")
        private String index;

        //if null, it's not a computed column
        @JsonProperty("cc_expr")
        private String computedColumnExpr;

        @JsonProperty("case_sensitive_name")
        public String caseSensitiveName;

        @JsonProperty("is_partitioned")
        public boolean isPartitioned;

        @JsonProperty("cardinality")
        private Long cardinality;

        @JsonProperty("min_value")
        private String minValue;

        @JsonProperty("max_value")
        private String maxValue;

        @JsonProperty("null_count")
        private Long nullCount;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDatatype() {
            return datatype;
        }

        public void setDatatype(String datatype) {
            this.datatype = datatype;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public String getDataGen() {
            return dataGen;
        }

        public void setDataGen(String dataGen) {
            this.dataGen = dataGen;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getComputedColumnExpr() {
            return computedColumnExpr;
        }

        public void setComputedColumnExpr(String computedColumnExpr) {
            this.computedColumnExpr = computedColumnExpr;
        }

        public String getCaseSensitiveName() {
            return caseSensitiveName;
        }

        public void setCaseSensitiveName(String caseSensitiveName) {
            this.caseSensitiveName = caseSensitiveName;
        }

        public boolean isPartitioned() {
            return isPartitioned;
        }

        public void setPartitioned(boolean partitioned) {
            isPartitioned = partitioned;
        }

        public Long getCardinality() {
            return cardinality;
        }

        public void setCardinality(Long cardinality) {
            this.cardinality = cardinality;
        }

        public String getMinValue() {
            return minValue;
        }

        public void setMinValue(String minValue) {
            this.minValue = minValue;
        }

        public String getMaxValue() {
            return maxValue;
        }

        public void setMaxValue(String maxValue) {
            this.maxValue = maxValue;
        }

        public Long getNullCount() {
            return nullCount;
        }

        public void setNullCount(Long nullCount) {
            this.nullCount = nullCount;
        }
    }
}

