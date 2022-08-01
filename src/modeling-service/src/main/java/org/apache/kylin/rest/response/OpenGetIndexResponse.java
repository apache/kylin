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

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.cube.model.IndexEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class OpenGetIndexResponse implements Serializable {

    @JsonProperty("project")
    private String project;
    @JsonProperty("uuid")
    private String modelId;
    @JsonProperty("model_name")
    private String modelAlias;
    @JsonProperty("total_size")
    private int totalSize;
    @JsonProperty("offset")
    private int offset;
    @JsonProperty("limit")
    private int limit;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("indexes")
    private List<IndexDetail> indexDetailList;
    @JsonProperty("absent_batch_index_ids")
    private List<Long> absentBatchIndexIds;

    @Getter
    @Setter
    @NoArgsConstructor
    public static class IndexDetail {

        @JsonProperty("id")
        private long id;
        @JsonProperty("status")
        private IndexEntity.Status status;
        @JsonProperty("source")
        private IndexEntity.Source source;
        @JsonProperty("col_order")
        private List<IndexResponse.ColOrderPair> colOrder;
        @JsonProperty("shard_by_columns")
        private List<String> shardByColumns;
        @JsonProperty("sort_by_columns")
        private List<String> sortByColumns;
        @JsonProperty("data_size")
        private long dataSize;
        @JsonProperty("usage")
        private long usage;
        @JsonProperty("last_modified")
        private long lastModified;
        @JsonProperty("storage_type")
        private int storageType;
        @JsonProperty("related_tables")
        private List<String> relatedTables;

        public static IndexDetail newIndexDetail(IndexResponse indexResponse) {
            IndexDetail detail = new IndexDetail();
            detail.setId(indexResponse.getId());
            detail.setStatus(indexResponse.getStatus());
            detail.setSource(indexResponse.getSource());
            detail.setColOrder(indexResponse.getColOrder());
            detail.setShardByColumns(indexResponse.getShardByColumns());
            detail.setSortByColumns(indexResponse.getSortByColumns());
            detail.setDataSize(indexResponse.getDataSize());
            detail.setUsage(indexResponse.getUsage());
            detail.setLastModified(indexResponse.getLastModified());
            detail.setStorageType(indexResponse.getStorageType());
            detail.setRelatedTables(indexResponse.getRelatedTables());
            return detail;
        }
    }

}
