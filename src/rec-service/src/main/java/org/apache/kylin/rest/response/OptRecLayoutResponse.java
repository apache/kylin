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

import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class OptRecLayoutResponse {
    @JsonProperty("item_id")
    private int id;
    @JsonProperty("is_agg")
    private boolean isAgg;
    @JsonProperty("is_add")
    private boolean isAdd;
    @JsonProperty("type")
    private RawRecItem.IndexRecType type;
    @JsonProperty("create_time")
    private long createTime;
    @JsonProperty("last_modified")
    private long lastModified;
    @JsonProperty("usage")
    private int usage;
    @JsonProperty("index_id")
    private long indexId;
    @JsonProperty("data_size")
    private long dataSize;
    @JsonProperty("memo_info")
    private Map<String, String> memoInfo = Maps.newHashMap();
    @JsonProperty("rec_detail_response")
    private OptRecDetailResponse recDetailResponse;
}
