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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TableExtDesc extends RootPersistentEntity {

    @JsonProperty("table_name")
    private String tableName;
    @JsonProperty("last_build_job_id")
    private String jodID;

    @JsonProperty("columns_stats")
    private List<ColumnStats> columnStats = new ArrayList<>();

    @JsonProperty("sample_rows")
    private List<String[]> sampleRows = new ArrayList<>();

    @JsonProperty("last_modified_time")
    private String lastModifiedTime;
    @JsonProperty("total_rows")
    private String totalRows;
    @JsonProperty("data_source_properties")
    private Map<String, String> dataSourceProps = new HashMap<>();

    public TableExtDesc() {
    }

    public String getResourcePath() {
        return concatResourcePath(getName());
    }

    public static String concatResourcePath(String tableIdentity) {
        return ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + tableIdentity + ".json";
    }

    public String getName() {
        return this.tableName;
    }

    public String getJodID() {
        return this.jodID;
    }

    public void addDataSourceProp(String key, String value) {
        this.dataSourceProps.put(key, value);
    }

    public Map<String, String> getDataSourceProp() {
        return this.dataSourceProps;
    }

    public void setSampleRows(List<String[]> sampleRows) {
        this.sampleRows = sampleRows;
    }

    public List<String[]> getSampleRows() {
        return this.sampleRows;
    }

    public String getCardinality() {

        StringBuffer cardinality = new StringBuffer();
        for (ColumnStats stat : this.columnStats) {
            cardinality.append(stat.getCardinality());
            cardinality.append(",");
        }
        return cardinality.toString();
    }

    public void setCardinality(String cardinality) {
        if (null == cardinality)
            return;

        String[] cardi = cardinality.split(",");

        if (0 == this.columnStats.size()) {
            for (int i = 0; i < cardi.length; i++) {
                ColumnStats columnStat = new ColumnStats();
                columnStat.setCardinality(Long.parseLong(cardi[i]));
                this.columnStats.add(columnStat);
            }
        } else if (this.columnStats.size() == cardi.length) {
            for (int i = 0; i < cardi.length; i++) {
                this.columnStats.get(i).setCardinality(Long.parseLong(cardi[i]));
            }
        } else {
            throw new IllegalArgumentException("The given cardinality columns don't match tables " + tableName);

        }
    }

    public List<ColumnStats> getColumnStats() {
        return this.columnStats;
    }

    public void setColumnStats(List<ColumnStats> columnStats) {
        this.columnStats = null;
        this.columnStats = columnStats;
    }

    public void setTotalRows(String totalRows) {
        this.totalRows = totalRows;
    }

    public String getTotalRows() {
        return this.totalRows;
    }

    public void setName(String name) {
        this.tableName = name;
    }

    public void setJodID(String jobID) {
        this.jodID = jobID;
    }

    public void init() {
        if (this.tableName != null)
            this.tableName = this.tableName.toUpperCase();
    }

    public void setLastModifiedTime(String lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public String getLastModifiedTime() {
        return this.lastModifiedTime;
    }

    public boolean isPartitioned() {
        return this.dataSourceProps.get("partition_column") == null ? false : !this.dataSourceProps.get("partition_column").isEmpty();
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "TableExtDesc{" + "name='" + (null == tableName ? "NULL" : tableName) + '\'' + ", columns_samples=" + (null == columnStats ? "null" : Arrays.toString(columnStats.toArray()));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnStats implements Comparable<ColumnStats>, Serializable {

        @JsonBackReference
        private TableExtDesc tableExtDesc;

        @JsonProperty("max_value")
        private String maxValue;

        @JsonProperty("min_value")
        private String minValue;

        @JsonProperty("max_length_value")
        private String maxLengthValue;

        @JsonProperty("min_length_value")
        private String minLengthValue;

        @JsonProperty("null_count")
        private long nullCount;

        @JsonProperty("cardinality")
        private long cardinality;

        @Override
        public int compareTo(ColumnStats o) {
            return 0;
        }

        public ColumnStats() {
        }

        public void setMaxValue(String maxValue) {
            this.maxValue = maxValue;
        }

        public String getMaxValue() {
            return this.maxValue;
        }

        public void setMinValue(String minValue) {
            this.minValue = minValue;
        }

        public String getMinValue() {
            return this.minValue;
        }

        public void setMaxLengthValue(String maxLengthValue) {
            this.maxLengthValue = maxLengthValue;
        }

        public String getMaxLengthValue() {
            return this.maxLengthValue;
        }

        public void setMinLengthValue(String minLengthValue) {
            this.minLengthValue = minLengthValue;
        }

        public String getMinLengthValue() {
            return this.minLengthValue;
        }

        public void setCardinality(long cardinality) {
            this.cardinality = cardinality;
        }

        public long getCardinality() {
            return this.cardinality;
        }

        public void setColumnSamples(String max, String min, String maxLenValue, String minLenValue) {
            this.maxValue = max;
            this.minValue = min;
            this.maxLengthValue = maxLenValue;
            this.minLengthValue = minLenValue;
        }

        public long getNullCount() {
            return nullCount;
        }

        public void setNullCount(long nullCount) {
            this.nullCount = nullCount;
        }
    }
}
