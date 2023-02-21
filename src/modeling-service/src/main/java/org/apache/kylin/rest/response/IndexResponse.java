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
package org.apache.kylin.rest.response;

import java.util.List;

import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.Source;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class IndexResponse {
    private Long id;

    private String project;

    private String model;

    private String name;

    private String owner;

    private IndexEntity.Status status;

    @JsonProperty("col_order")
    private List<ColOrderPair> colOrder;

    @JsonProperty("shard_by_columns")
    private List<String> shardByColumns;

    @JsonProperty("sort_by_columns")
    private List<String> sortByColumns;

    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_NDATA_STORAGE;

    @JsonProperty("data_size")
    private long dataSize;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("last_modified")
    private long lastModified;

    @JsonProperty("related_tables")
    private List<String> relatedTables;

    @JsonIgnore
    private boolean isManual;

    @JsonIgnore
    private boolean isBase;

    // just for baseindex
    @JsonProperty("need_update")
    private boolean needUpdate;

    @JsonProperty("index_range")
    private IndexEntity.Range indexRange;

    @JsonProperty("source")
    public IndexEntity.Source getSource() {
        if (isBase()) {
            return IndexEntity.isTableIndex(getId()) ? Source.BASE_TABLE_INDEX : Source.BASE_AGG_INDEX;
        }
        if (getId() < IndexEntity.TABLE_INDEX_START_ID) {
            if (isManual()) {
                return IndexEntity.Source.CUSTOM_AGG_INDEX;
            } else {
                return IndexEntity.Source.RECOMMENDED_AGG_INDEX;
            }
        } else {
            if (isManual()) {
                return IndexEntity.Source.CUSTOM_TABLE_INDEX;
            } else {
                return IndexEntity.Source.RECOMMENDED_TABLE_INDEX;
            }
        }

    }

    @JsonProperty("abnormal_type")
    private NDataLayout.AbnormalType abnormalType;

    @Data
    @AllArgsConstructor
    public static class ColOrderPair {
        private String key;
        private String value;
        private Long cardinality;

        public ColOrderPair(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public void changeTableAlias(String oldAlias, String newAlias) {
            String[] split = this.key.split("\\.");
            if (split.length == 2) {
                String table = split[0];
                String column = split[1];
                if (table.equalsIgnoreCase(oldAlias)) {
                    this.key = newAlias + "." + column;
                }
            }
        }
    }
}
