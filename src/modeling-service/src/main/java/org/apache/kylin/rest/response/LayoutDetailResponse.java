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
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class LayoutDetailResponse {

    @JsonProperty("project")
    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("layout_id")
    private String layoutId;

    @JsonProperty("layout_detail_range_list")
    private Set<LayoutDetailRange> layoutDetailRangeSet;

    @JsonProperty("partition_columns")
    private List<String> partitionColumns;

    @JsonProperty("zorder_by_columns")
    private List<String> zorderByColumns;

    @JsonProperty("max_compaction_file_size_in_bytes")
    private long maxCompactionFileSizeInBytes;

    @JsonProperty("min_compaction_file_size_in_bytes")
    private long minCompactionFileSizeInBytes;

    @JsonProperty("compaction_after_update")
    private boolean compactionAfterUpdate;

    @JsonProperty("location")
    private String location;

    @JsonProperty("num_of_files")
    private int numOfFiles;

    @JsonProperty("size_in_bytes")
    private long sizeInBytes;

    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonProperty("table_version")
    private long tableVersion;

    @JsonProperty("create_time")
    private long createTime;

    @Data
    public static class LayoutDetailRange {

        @JsonProperty("start_ts")
        private long start;

        @JsonProperty("end_ts")
        private long end;
    }
}
