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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.base.Strings;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Slf4j
public class TableExtDesc extends RootPersistentEntity implements Serializable {

    public static final String S3_ROLE_PROPERTY_KEY = "s3_role";
    public static final String LOCATION_PROPERTY_KEY = "location";
    public static final String S3_ENDPOINT_KEY = "s3_endpoint";
    public static final String SEPARATOR = "/";

    public static String concatRawResourcePath(String nameOnPath) {
        return ResourceStore.TABLE_EXD_RESOURCE_ROOT + SEPARATOR + nameOnPath + ".json";
    }

    // ============================================================================

    @Getter
    @Setter
    @JsonProperty("table_name")
    private String identity;

    @Getter
    @Setter
    @JsonProperty("last_build_job_id")
    private String jodID;

    @Setter
    @JsonProperty("columns_stats")
    private List<ColumnStats> columnStats = new ArrayList<>(); // should not expose getter

    @Getter
    @Setter
    @JsonProperty("sample_rows")
    private List<String[]> sampleRows = new ArrayList<>();

    @Getter
    @Setter
    @JsonProperty("total_rows")
    private long totalRows;

    @Getter
    @JsonProperty("data_source_properties")
    private Map<String, String> dataSourceProps = new HashMap<>();

    @Getter
    private String project;

    @Getter
    @Setter
    @JsonProperty("row_count_status")
    private TableExtDesc.RowCountStatus rowCountStatus;

    @Getter
    @Setter
    @JsonProperty("original_size")
    private long originalSize = -1;

    @Getter
    @Setter
    @JsonProperty("query_hit_count")
    private int snapshotHitCount = 0;

    @Getter
    @Setter
    @JsonProperty("excluded")
    private boolean excluded;

    @Getter
    @Setter
    @JsonProperty("excluded_columns")
    private Set<String> excludedColumns = Sets.newLinkedHashSet();

    public TableExtDesc() {
    }

    public TableExtDesc(TableExtDesc other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.identity = other.identity;
        this.jodID = other.jodID;
        this.columnStats = other.columnStats;
        this.sampleRows = other.sampleRows;
        this.totalRows = other.totalRows;
        this.dataSourceProps = other.dataSourceProps;
        this.project = other.project;
        this.originalSize = other.originalSize;
        this.snapshotHitCount = other.snapshotHitCount;
        this.excluded = other.excluded;
        this.excludedColumns = other.excludedColumns;
        this.rowCountStatus = other.rowCountStatus; // no need any more, will be deleted later.
    }

    public boolean testExcluded(ColumnDesc column) {
        return excluded || excludedColumns.contains(column.getName());
    }

    @Override
    public String resourceName() {
        return getIdentity();
    }

    @Override
    public String getResourcePath() {
        return SEPARATOR + getProject() + ResourceStore.TABLE_EXD_RESOURCE_ROOT + SEPARATOR + getIdentity()
                + MetadataConstants.FILE_SURFIX;
    }

    public void addDataSourceProp(String key, String value) {
        this.dataSourceProps.put(key, value);
    }

    public String getCardinality() {

        StringBuilder cardinality = new StringBuilder();
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
            for (String aCardi : cardi) {
                ColumnStats columnStat = new ColumnStats();
                columnStat.setCardinality(Long.parseLong(aCardi));
                this.columnStats.add(columnStat);
            }
        } else if (this.columnStats.size() == cardi.length) {
            for (int i = 0; i < cardi.length; i++) {
                this.columnStats.get(i).setCardinality(Long.parseLong(cardi[i]));
            }
        } else {
            throw new IllegalArgumentException("The given cardinality columns don't match tables " + identity);
        }
    }

    public enum RowCountStatus {
        OK("ok"), TENTATIVE("tentative");

        private final String status;

        RowCountStatus(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    /**
     * Get all column stats info of a table. Owing to the side effect of schema change,
     * it may get an error result when making use of this method to get stats info of
     * a specified column indirectly. Instead, you can use {@link #getColumnStatsByName(java.lang.String)}
     * directly to get column stats info.
     */
    public List<ColumnStats> getAllColumnStats() {
        return columnStats;
    }

    /**
     * Get stats info of specified column by column name.
     */
    public ColumnStats getColumnStatsByName(String colName) {
        Map<String, ColumnStats> columnStatsMap = Maps.newHashMap();
        for (ColumnStats col : columnStats) {
            columnStatsMap.putIfAbsent(col.getColumnName(), col);
        }

        return columnStatsMap.getOrDefault(colName, null);
    }

    public boolean isExcludedCol(String colName) {
        return excluded || getExcludedColumns().contains(colName);
    }

    public int countExcludedColSize() {
        if (!isExcluded()) {
            return getExcludedColumns().size();
        }
        return getTableDesc().getColumns().length;
    }

    private TableDesc getTableDesc() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(getIdentity());
    }

    public void init(String project) {
        this.project = project;
        if (this.identity != null)
            this.identity = this.identity.toUpperCase(Locale.ROOT);
    }

    public S3RoleCredentialInfo getS3RoleCredentialInfo() {
        String location = this.dataSourceProps.get(LOCATION_PROPERTY_KEY);
        String s3Role = this.dataSourceProps.get(S3_ROLE_PROPERTY_KEY);
        String s3Endpoint = this.dataSourceProps.get(S3_ENDPOINT_KEY);
        if (Strings.isNullOrEmpty(location)) {
            return null;
        }
        String bucket = null;
        try {
            bucket = new URI(location).getAuthority();
        } catch (Exception e) {
            log.warn("invalid s3 location {}", location, e);
        }
        if (Strings.isNullOrEmpty(bucket)) {
            return null;
        }
        return new S3RoleCredentialInfo(bucket, s3Role, s3Endpoint);

    }

    @Override
    public int hashCode() {
        return getIdentity().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        return "TableExtDesc{" + "name='" + (null == identity ? "NULL" : identity) + '\'' + ", columns_samples="
                + (null == columnStats ? "null" : Arrays.toString(columnStats.toArray()));
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    public static class S3RoleCredentialInfo {
        private String bucket;
        private String role;
        private String endpoint;

        public S3RoleCredentialInfo(String bucket, String role, String endpoint) {
            this.bucket = bucket;
            this.role = role;
            this.endpoint = endpoint;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnStats implements Comparable<ColumnStats>, Serializable {

        @JsonBackReference
        private TableExtDesc tableExtDesc;

        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("max_numeral")
        private double maxNumeral = Double.NaN;

        @JsonProperty("min_numeral")
        private double minNumeral = Double.NaN;

        @JsonProperty("max_value")
        private String maxValue;

        @JsonProperty("min_value")
        private String minValue;

        @JsonProperty("max_length")
        private Integer maxLength;

        @JsonProperty("min_length")
        private Integer minLength;

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

        @JsonIgnore
        private transient Map<String, HLLCounter> rangeHLLC = new HashMap<>();

        @JsonIgnore
        private transient HLLCounter totalHLLC;

        @JsonIgnore
        private transient long totalCardinality;

        @Override
        public int compareTo(ColumnStats o) {
            return 0;
        }

        public void init() {
            if (rangeHLLC.isEmpty()) {
                return;
            }

            final Iterator<HLLCounter> hllcIterator = rangeHLLC.values().iterator();

            totalHLLC = new HLLCounter(hllcIterator.next());
            while (hllcIterator.hasNext()) {
                totalHLLC.merge(hllcIterator.next());
            }

            totalCardinality = totalHLLC.getCountEstimate();

            cardinality = totalCardinality;
        }

        public void updateBasicStats(double maxNumeral, double minNumeral, int maxLength, int minLength,
                String maxLengthValue, String minLengthValue) {
            if (Double.isNaN(this.maxNumeral) || maxNumeral > this.maxNumeral) {
                this.maxNumeral = maxNumeral;
            }

            if (Double.isNaN(this.minNumeral) || minNumeral < this.minNumeral) {
                this.minNumeral = minNumeral;
            }

            if (this.maxLength == null || maxLength > this.maxLength) {
                this.maxLength = maxLength;
                this.maxLengthValue = maxLengthValue;
            }

            if (this.minLength == null || minLength < this.minLength) {
                this.minLength = minLength;
                this.minLengthValue = minLengthValue;
            }
        }

        @JsonIgnore
        public long getTotalCardinality() {
            return totalCardinality;
        }

        public void addNullCount(long incre) {
            this.nullCount += incre;
        }

        public void setColumnSamples(String max, String min, String maxLenValue, String minLenValue) {
            this.maxValue = max;
            this.minValue = min;
            this.maxLengthValue = maxLenValue;
            this.minLengthValue = minLenValue;
        }

        public static TableExtDesc.ColumnStats getColumnStats(NTableMetadataManager tableMetadataManager,
                TblColRef colRef) {
            TableExtDesc.ColumnStats ret = null;

            TableExtDesc tableExtDesc = tableMetadataManager.getTableExtIfExists(colRef.getTableRef().getTableDesc());
            if (tableExtDesc != null) {
                ret = tableExtDesc.getColumnStatsByName(colRef.getColumnDesc().getName());
            }
            return ret;
        }
    }
}
