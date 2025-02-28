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

import java.util.List;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity.Source;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class CreateBaseIndexRequest implements ProjectInsensitiveRequest {

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("source_types")
    private Set<Source> sourceTypes = Sets.newHashSet();

    @JsonProperty("base_agg_index_property")
    private LayoutProperty baseAggIndexProperty;

    @JsonProperty("base_table_index_property")
    private LayoutProperty baseTableIndexProperty;

    public boolean needHandleBaseAggIndex() {
        return sourceTypes.isEmpty() || sourceTypes.contains(Source.BASE_AGG_INDEX);
    }

    public boolean needHandleBaseTableIndex() {
        return sourceTypes.isEmpty() || sourceTypes.contains(Source.BASE_TABLE_INDEX);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LayoutProperty {

        @JsonProperty("col_order")
        private List<String> colOrder;

        @JsonProperty("shard_by_columns")
        private List<String> shardByColumns;

        @JsonProperty("sort_by_columns")
        private List<String> sortByColumns;
    }

}
