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

package org.apache.kylin.metadata.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class QueryHistoryInfoResponse implements Serializable {
    @JsonProperty("exactly_match")
    private boolean exactlyMatch;
    @JsonProperty("scan_segment_num")
    private int scanSegmentNum;
    @JsonProperty("state")
    private QueryHistoryInfo.HistoryState state;
    @JsonProperty("execution_error")
    private boolean executionError;
    @JsonProperty("error_msg")
    private String errorMsg;
    @JsonProperty("query_snapshots")
    private List<List<String>> querySnapshots = new ArrayList<>();
    @JsonProperty("realization_metrics")
    protected List<QueryMetrics.RealizationMetrics> realizationMetrics = new ArrayList<>();
    @JsonProperty("traces")
    private List<QueryHistoryInfo.QueryTraceSpan> traces = new ArrayList<>();
    @JsonProperty("cache_type")
    private String cacheType;
    @JsonProperty("query_msg")
    private String queryMsg;
}
