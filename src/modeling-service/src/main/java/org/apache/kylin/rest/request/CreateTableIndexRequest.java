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
package org.apache.kylin.rest.request;

import static org.apache.kylin.metadata.cube.model.IndexEntity.Range.HYBRID;

import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CreateTableIndexRequest implements ProjectInsensitiveRequest {

    private Long id;

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("col_order")
    private List<String> colOrder;

    @JsonProperty("layout_override_indexes")
    @Builder.Default
    private Map<String, String> layoutOverrideIndexes = Maps.newHashMap();

    @JsonProperty("shard_by_columns")
    private List<String> shardByColumns;

    @JsonProperty("sort_by_columns")
    private List<String> sortByColumns;

    @JsonProperty("storage_type")
    @Builder.Default
    private int storageType = IStorageAware.ID_NDATA_STORAGE;

    @Builder.Default
    @JsonProperty("load_data")
    private boolean isLoadData = true;

    @JsonProperty("index_range")
    private IndexEntity.Range indexRange = HYBRID;
}
