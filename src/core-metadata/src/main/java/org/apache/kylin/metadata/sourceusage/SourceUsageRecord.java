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
package org.apache.kylin.metadata.sourceusage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class SourceUsageRecord extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(SourceUsageRecord.class);

    public static Logger getLogger() {
        return logger;
    }

    @JsonProperty("capacity_notification")
    private boolean capacityNotification = true;

    @JsonProperty("check_time")
    private long checkTime;

    @JsonProperty("current_capacity")
    private long currentCapacity;

    @JsonProperty("license_capacity")
    private long licenseCapacity;

    @JsonProperty("capacity_details")
    private transient ProjectCapacityDetail[] capacityDetails;

    @JsonProperty("capacity_status")
    private CapacityStatus capacityStatus = CapacityStatus.OK;

    @JsonProperty("res_path")
    private String resPath;

    @Override
    public String getResourcePath() {
        return resPath;
    }

    @Override
    public String resourceName() {
        if (resPath != null) {
            return resPath.substring(MetadataType.HISTORY_SOURCE_USAGE.name().length() + 1);
        }
        return null;
    }

    public SourceUsageRecord() {
        capacityDetails = new ProjectCapacityDetail[0];
    }

    public void appendProject(ProjectCapacityDetail project) {
        List<ProjectCapacityDetail> allProject = Lists.newArrayList(this.capacityDetails);
        allProject.add(project);
        this.capacityDetails = allProject.toArray(new ProjectCapacityDetail[0]);
    }

    public ProjectCapacityDetail getProjectCapacity(String name) {
        for (ProjectCapacityDetail c : this.capacityDetails) {
            if (name.equals(c.getName())) {
                return c;
            }
        }
        return null;
    }

    @Getter
    @Setter
    public static class ProjectCapacityDetail {
        @JsonProperty("name")
        private String name;

        @JsonProperty("capacity")
        private long capacity;

        @JsonProperty("license_capacity")
        private long licenseCapacity;

        @JsonProperty("capacity_ratio")
        private double capacityRatio;

        @JsonProperty("status")
        private CapacityStatus status = CapacityStatus.OK;

        @JsonProperty("tables")
        private TableCapacityDetail[] tables;

        public ProjectCapacityDetail() {

        }

        public ProjectCapacityDetail(ProjectCapacityDetail projectCapacity) {
            this.name = projectCapacity.name;
            this.capacity = projectCapacity.capacity;
            this.status = projectCapacity.status;
            this.capacityRatio = projectCapacity.capacityRatio;
            this.tables = new TableCapacityDetail[projectCapacity.tables.length];
            for (int i = 0; i < projectCapacity.tables.length; i++) {
                this.tables[i] = new TableCapacityDetail(projectCapacity.tables[i]);
            }
        }

        public ProjectCapacityDetail(String name) {
            this.name = name;
            this.tables = new TableCapacityDetail[0];
        }

        public void appendTable(TableCapacityDetail table) {
            List<TableCapacityDetail> allTables = Lists.newArrayList(this.tables);
            allTables.add(table);
            this.tables = allTables.toArray(new TableCapacityDetail[0]);
        }

        public TableCapacityDetail getTableByName(String name) {
            for (TableCapacityDetail c : this.tables) {
                // return first matched column
                if (name.equalsIgnoreCase(c.getName())) {
                    return c;
                }
            }
            return null;
        }

        public void updateTable(TableCapacityDetail table) {
            TableCapacityDetail existing = getTableByName(table.name);
            if (existing == null) {
                appendTable(table);
            }
        }

        public boolean isOverCapacity() {
            return status == CapacityStatus.OVERCAPACITY;
        }

    }

    @Getter
    @Setter
    public static class TableCapacityDetail {
        @JsonProperty("name")
        private String name;

        @JsonProperty("status")
        private CapacityStatus status = CapacityStatus.OK;

        @JsonProperty("capacity")
        private long capacity;

        @JsonProperty("capacity_ratio")
        private double capacityRatio;

        @JsonProperty("table_type")
        private TableKind tableKind;

        @JsonProperty("columns")
        private ColumnCapacityDetail[] columns;

        public TableCapacityDetail() {

        }

        public TableCapacityDetail(TableCapacityDetail other) {
            this.name = other.name;
            this.status = other.status;
            this.tableKind = other.tableKind;
            this.capacity = other.capacity;
            this.capacityRatio = other.capacityRatio;
            this.columns = new ColumnCapacityDetail[other.columns.length];
            for (int i = 0; i < other.columns.length; i++) {
                this.columns[i] = new ColumnCapacityDetail(other.columns[i]);
            }
        }

        public TableCapacityDetail(String name) {
            this.name = name;
            columns = new ColumnCapacityDetail[0];
        }

        public void appendColumn(ColumnCapacityDetail column) {
            List<ColumnCapacityDetail> allColumns = Lists.newArrayList(this.columns);
            allColumns.add(column);
            this.columns = allColumns.toArray(new ColumnCapacityDetail[0]);
        }

        public ColumnCapacityDetail getColumnByName(String name) {
            for (ColumnCapacityDetail c : this.columns) {
                // return first matched column
                if (name.equalsIgnoreCase(c.getName())) {
                    return c;
                }
            }
            return null;
        }

        public void updateColumn(ColumnCapacityDetail column) {
            ColumnCapacityDetail existing = getColumnByName(column.name);
            if (existing == null) {
                appendColumn(column);
            }
        }

    }

    @Getter
    @Setter
    public static class ColumnCapacityDetail {
        @JsonProperty("name")
        private String name;

        @JsonProperty("max_source_bytes")
        private long maxSourceBytes = 0L;

        @JsonProperty("source_bytes_map")
        private Map<String, Long> sourceBytesMap = new HashMap<>();

        public ColumnCapacityDetail() {

        }

        public ColumnCapacityDetail(ColumnCapacityDetail other) {
            this.name = other.name;
            this.maxSourceBytes = other.maxSourceBytes;
            this.sourceBytesMap = other.sourceBytesMap;
        }

        public ColumnCapacityDetail(String name) {
            this.name = name;
        }

        public long getDataflowSourceBytes(String dataflow) {
            return this.sourceBytesMap.get(dataflow);
        }

        public void setDataflowSourceBytes(String dataflow, long sourceBytes) {
            this.sourceBytesMap.put(dataflow, sourceBytes);
            this.maxSourceBytes = Long.max(maxSourceBytes, sourceBytes);
        }

    }

    public enum TableKind {
        FACT, WITHSNAP, WITHOUTSNAP
    }

    public enum CapacityStatus {

        // FIX-ME SVL: the design could be better
        //
        // - OVERCAPACITY is not among the others. OK/Tentative/Error can all lead to OVERCAPACITY.
        //
        // - Note the logic of merging status is repeated several times in SourceUsageManager.
        //   This enum can be comparable such that OK < Tentative < Error, then Collection.max() can merge status gracefully.
        //   Ref -- https://docs.oracle.com/javase/6/docs/api/java/lang/Enum.html
        //   Ref -- https://www.tutorialspoint.com/java/lang/enum_compareto.htm

        OK, TENTATIVE, ERROR, OVERCAPACITY
    }
}
