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

import org.apache.kylin.metadata.cube.model.IndexEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class IndexGraphResponse {
    private String project;

    private String model;

    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;

    @JsonProperty("auto_agg_indexes")
    private IndexList autoAggIndexes;

    @JsonProperty("auto_table_indexes")
    private IndexList autoTableIndexes;

    @JsonProperty("manual_agg_indexes")
    private IndexList manualAggIndexes;

    @JsonProperty("manual_table_indexes")
    protected IndexList manualTableIndexes;

    @JsonProperty("is_full_loaded")
    public boolean isFullLoaded() {
        return endTime == Long.MAX_VALUE;
    }

    @Data
    public static class IndexList {

        private List<Index> indexes;

        @JsonProperty("total_size")
        private long totalSize;
    }

    @Data
    public static class Index {

        private long id;

        @JsonProperty("data_size")
        private long dataSize;

        private long usage;

        private IndexEntity.Status status;

        @JsonProperty("last_modified_time")
        private long lastModifiedTime;
    }

    @JsonProperty("empty_indexes")
    private long emptyIndexes;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    @JsonProperty("segment_to_complement_count")
    private long segmentToComplementCount;
}
