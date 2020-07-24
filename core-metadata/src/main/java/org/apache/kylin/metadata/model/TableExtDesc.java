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
import java.util.Locale;
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
    private String tableIdentity;
    @JsonProperty("last_build_job_id")
    private String jodID;
    @JsonProperty("frequency")
    private int frequency;

    // ============================================================================
    @JsonProperty("columns_stats")
    private List<ColumnStats> columnStats = new ArrayList<>();
    @JsonProperty("sample_rows")
    private List<String[]> sampleRows = new ArrayList<>();
    @JsonProperty("last_modified_time")
    private long lastModifiedTime;
    @JsonProperty("total_rows")
    private long totalRows;
    @JsonProperty("mapper_rows")
    private List<Long> mapRecords = new ArrayList<>();
    @JsonProperty("data_source_properties")
    private Map<String, String> dataSourceProps = new HashMap<>();
    private String project;
    public TableExtDesc() {
    }

    public static String concatRawResourcePath(String nameOnPath) {
        return ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + nameOnPath + ".json";
    }

    public static String concatResourcePath(String tableIdentity, String prj) {
        return concatRawResourcePath(TableDesc.makeResourceName(tableIdentity, prj));
    }
    @Override
    public String resourceName() {
        return TableDesc.makeResourceName(getIdentity(), getProject());
    }

    public String getResourcePath() {
        return concatResourcePath(getIdentity(), getProject());
    }

    public String getProject() {
        return project;
    }

    public int getFrequency() {
        return this.frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public String getIdentity() {
        return this.tableIdentity;
    }

    public void setIdentity(String name) {
        this.tableIdentity = name;
    }

    public String getJodID() {
        return this.jodID;
    }

    public void setJodID(String jobID) {
        this.jodID = jobID;
    }

    public void addDataSourceProp(String key, String value) {
        this.dataSourceProps.put(key, value);
    }

    public Map<String, String> getDataSourceProp() {
        return this.dataSourceProps;
    }

    public List<String[]> getSampleRows() {
        return this.sampleRows;
    }

    public void setSampleRows(List<String[]> sampleRows) {
        this.sampleRows = sampleRows;
    }

    public List<Long> getMapRecords() {
        return this.mapRecords;
    }

    public void setMapRecords(List<Long> mapRecords) {
        this.mapRecords = mapRecords;
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

        if (this.columnStats.isEmpty()) {
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
            throw new IllegalArgumentException("The given cardinality columns don't match tables " + tableIdentity);

        }
    }

    public void resetCardinality() {
        int columnSize = this.columnStats.size();
        this.columnStats.clear();
        for (int i = 0; i < columnSize; i++) {
            this.columnStats.add(new ColumnStats());
        }
    }

    public List<ColumnStats> getColumnStats() {
        return this.columnStats;
    }

    public void setColumnStats(List<ColumnStats> columnStats) {
        this.columnStats = null;
        this.columnStats = columnStats;
    }

    public long getTotalRows() {
        return this.totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    public void init(String project) {
        this.project = project;

        if (this.tableIdentity != null)
            this.tableIdentity = this.tableIdentity.toUpperCase(Locale.ROOT);
    }

    public long getLastModifiedTime() {
        return this.lastModifiedTime;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public boolean isPartitioned() {
        return this.dataSourceProps.get("partition_column") == null ? false
                : !this.dataSourceProps.get("partition_column").isEmpty();
    }

    @Override
    public int hashCode() {
        return getIdentity().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TableExtDesc tableExtDesc = (TableExtDesc) o;

        return getResourcePath().equals(tableExtDesc.getResourcePath());
    }

    @Override
    public String toString() {
        return "TableExtDesc{" + "name='" + (null == tableIdentity ? "NULL" : tableIdentity) + '\''
                + ", columns_samples=" + (null == columnStats ? "null" : Arrays.toString(columnStats.toArray()));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnStats implements Comparable<ColumnStats>, Serializable {

        @JsonBackReference
        private TableExtDesc tableExtDesc;

        @JsonProperty("column_name")
        private String columnName;

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

        @JsonProperty("exceed_precision_count")
        private long exceedPrecisionCount;

        @JsonProperty("exceed_precision_max_length_value")
        private String exceedPrecisionMaxLengthValue;

        @JsonProperty("cardinality")
        private long cardinality;

        @JsonProperty("data_skew_samples")
        private Map<String, Long> dataSkewSamples = new HashMap<>();

        public ColumnStats() {
        }

        @Override
        public int compareTo(ColumnStats o) {
            return 0;
        }

        public String getExceedPrecisionMaxLengthValue() {
            return this.exceedPrecisionMaxLengthValue;
        }

        public void setExceedPrecisionMaxLengthValue(String value) {
            this.exceedPrecisionMaxLengthValue = value;
        }

        public long getExceedPrecisionCount() {
            return this.exceedPrecisionCount;
        }

        public void setExceedPrecisionCount(long exceedPrecisionCount) {
            this.exceedPrecisionCount = exceedPrecisionCount;
        }

        public String getColumnName() {
            return this.columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getMaxValue() {
            return this.maxValue;
        }

        public void setMaxValue(String maxValue) {
            this.maxValue = maxValue;
        }

        public String getMinValue() {
            return this.minValue;
        }

        public void setMinValue(String minValue) {
            this.minValue = minValue;
        }

        public String getMaxLengthValue() {
            return this.maxLengthValue;
        }

        public void setMaxLengthValue(String maxLengthValue) {
            this.maxLengthValue = maxLengthValue;
        }

        public String getMinLengthValue() {
            return this.minLengthValue;
        }

        public void setMinLengthValue(String minLengthValue) {
            this.minLengthValue = minLengthValue;
        }

        public long getCardinality() {
            return this.cardinality;
        }

        public void setCardinality(long cardinality) {
            this.cardinality = cardinality;
        }

        public Map<String, Long> getDataSkewSamples() {
            return this.dataSkewSamples;
        }

        public void setDataSkewSamples(Map<String, Long> dataSkewSamples) {
            this.dataSkewSamples = dataSkewSamples;
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
