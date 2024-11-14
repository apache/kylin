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

package org.apache.kylin.metadata.table;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalTableDesc extends ATable implements Serializable {

    private static final String BUCKET_COLUMN = "bucketCol";
    private static final String BUCKET_NUM = "bucketNum";
    private static final String PRELOADED_CACHE = "preloadedCache";
    private static final String PRIMARY_KEY = "primaryKey";
    private static final String SORT_BY_KEY = "sortByKey";
    private static final String SORT_BY_PARTITION_BEFORE_SAVE = "sortByPartition";
    public static final int INIT_SIZE = 0;

    @Getter
    public enum StorageType {
        PARQUET("parquet"), //parquet
        GLUTEN("clickhouse"), //gluten mergetree, default
        DELTALAKE("delta"), //future
        ICEBERG("iceberg"); //future

        private final String format;

        StorageType(String format) {
            this.format = format;
        }
    }

    @JsonProperty("tbl_properties")
    private Map<String, String> tblProperties;

    @JsonProperty("storage_type")
    private StorageType storageType;

    @JsonProperty("storage_size")
    private long storageSize = INIT_SIZE;

    @JsonProperty("row_count")
    private long rowCount = 0;

    @JsonProperty("hit_count")
    private long hitCount = 0;

    @JsonProperty("location")
    private String location;

    @JsonProperty("table_partition")
    private InternalTablePartition tablePartition;

    public InternalTableDesc(InternalTableDesc other) {
        this.project = other.project;
        this.database.setName(other.getDatabase());
        this.tblProperties = other.tblProperties;
        this.storageType = other.storageType;
        this.location = other.location;
        this.tablePartition = other.tablePartition;
        this.storageSize = other.storageSize;
        setMvcc(other.getMvcc());
    }

    public InternalTableDesc(TableDesc originTable) {
        this.project = originTable.getProject();
        this.database.setName(originTable.getDatabase());
        this.name = originTable.getName();
        this.columns = originTable.getColumns();
        this.uuid = RandomUtil.randomUUIDStr();
        this.lastModified = 0L;
    }

    /**
     * - remove empty value
     * - fix order_by/bucket/ relevant cols to sensitive Columns
     */
    public void optimizeTblProperties() {
        Map<String, String> properties = this.getTblProperties();
        Map<String, String> optimizedProperties = Maps.newHashMap();
        properties.forEach((k, v) -> {
            if (!StringUtils.isEmpty(v)) {
                optimizedProperties.put(k, v);
            }
        });
        this.setTblProperties(optimizedProperties);
    }

    // only support clickhouse
    // parquet(dev only)
    @Override
    public void init(String project) {
        super.init(project);
    }

    public String getDoubleQuoteInternalIdentity() {
        return String.format(Locale.ROOT, "\"INTERNAL_CATALOG\".\"%s\".\"%s\".\"%s\"", project, getDatabase(),
                getName());
    }

    public void setStorageType(String storageType) {
        if (storageType == null) {
            this.storageType = StorageType.GLUTEN;
        } else {
            String storageTypeUpper = storageType.toUpperCase(Locale.ROOT);
            this.storageType = StorageType.valueOf(storageTypeUpper);
        }
    }

    public String getBucketColumn() {
        if (null == tblProperties.get(BUCKET_COLUMN)) {
            return null;
        } else {
            return tblProperties.get(BUCKET_COLUMN).trim();
        }
    }

    public int getBucketNumber() {
        if (null == tblProperties.get(BUCKET_NUM)) {
            return 0;
        } else {
            return Integer.parseInt(tblProperties.get(BUCKET_NUM).trim());
        }
    }

    public List<String> getPrimaryKey() {
        return Arrays.stream(StringUtils.split(tblProperties.get(PRIMARY_KEY), ",")).map(String::trim)
                .collect(Collectors.toList());
    }

    public List<String> getSortByKey() {
        return Arrays.stream(StringUtils.split(tblProperties.get(SORT_BY_KEY), ",")).map(String::trim)
                .collect(Collectors.toList());
    }

    public boolean isPreloadedCacheEnable() {
        return Boolean.parseBoolean(tblProperties.getOrDefault(PRELOADED_CACHE, KylinConfig.FALSE));
    }

    public boolean isSortByPartitionEnabled() {
        if (tblProperties.containsKey(SORT_BY_PARTITION_BEFORE_SAVE)) {
            return Boolean.parseBoolean(tblProperties.get(SORT_BY_PARTITION_BEFORE_SAVE));
        } else {
            return NProjectManager.getProjectConfig(project).isInternalTableSortByPartitionEnabled();
        }
    }

    public String generateInternalTableLocation() {
        String workingDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        return workingDir + project + "/Internal/" + getDatabase() + "/" + getName();
    }

    public StorageType getStorageType() {
        return this.storageType;
    }

    public String[] getPartitionColumns() {
        return tablePartition == null ? null : tablePartition.getPartitionColumns();
    }

    public String getDatePartitionFormat() {
        return tablePartition == null ? null : tablePartition.getDatePartitionFormat();
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.INTERNAL_TABLE;
    }

    public TableDesc getTableDesc() {
        return NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getTableDesc(this.getIdentity());
    }

}
