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

package org.apache.kylin.metadata.cube.model;

import static org.apache.kylin.common.persistence.ResourceStore.DATA_LOADING_RANGE_RESOURCE_ROOT;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.apache.kylin.metadata.model.SegmentRange;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@SuppressWarnings("serial")
@Setter
@Getter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataLoadingRange extends RootPersistentEntity {

    @JsonProperty("table_name")
    private String tableName;
    @JsonProperty("column_name")
    private String columnName;
    @JsonProperty("partition_date_format")
    private String partitionDateFormat;

    @JsonProperty("pushdown_range_limited")
    private boolean pushdownRangeLimited;

    @JsonProperty("segment_config")
    private SegmentConfig segmentConfig = new SegmentConfig();

    @JsonProperty("covered_range")
    private SegmentRange coveredRange;

    @Setter(AccessLevel.NONE)
    private String project;

    public NDataLoadingRange(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public void initAfterReload(KylinConfig config, String p) {
        this.project = p;
    }

    @Override
    public String resourceName() {
        return tableName;
    }

    @Override
    public String getResourcePath() {
        return new StringBuilder().append("/").append(project).append(DATA_LOADING_RANGE_RESOURCE_ROOT).append("/")
                .append(resourceName()).append(MetadataConstants.FILE_SURFIX).toString();
    }
}
