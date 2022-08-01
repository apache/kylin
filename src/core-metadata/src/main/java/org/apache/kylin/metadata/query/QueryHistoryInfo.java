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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Setter
@Getter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class QueryHistoryInfo implements Serializable {
    @JsonProperty("exactly_match")
    private boolean exactlyMatch;
    @JsonProperty("scan_segment_num")
    private int scanSegmentNum;
    @JsonProperty("state")
    private HistoryState state;
    @JsonProperty("execution_error")
    private boolean executionError;
    @JsonProperty("error_msg")
    private String errorMsg;
    @JsonProperty("query_snapshots")
    private List<List<String>> querySnapshots = new ArrayList<>();
    @JsonProperty("realization_metrics")
    protected List<QueryMetrics.RealizationMetrics> realizationMetrics = new ArrayList<>();
    @JsonProperty("traces")
    private List<QueryTraceSpan> traces = new ArrayList<>();
    @JsonProperty("cache_type")
    private String cacheType;
    @JsonProperty("query_msg")
    private String queryMsg;

    public QueryHistoryInfo(boolean exactlyMatch, Integer scanSegmentNum, boolean executionError) {
        this.exactlyMatch = exactlyMatch;
        this.scanSegmentNum = Objects.isNull(scanSegmentNum) ? 0 : scanSegmentNum;
        this.executionError = executionError;
        this.state = HistoryState.PENDING;
    }

    public enum HistoryState {
        PENDING(0, "pending"), SUCCESS(1, "success"), FAILED(2, "failed");

        private int stateCode;
        private String name;

        HistoryState(int stateCode, String name) {
            this.stateCode = stateCode;
            this.name = name;
        }
    }

    @Data
    @Setter
    @Getter
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class QueryTraceSpan {

        @JsonProperty("name")
        String name;

        @JsonProperty("group")
        String group;

        @JsonProperty("duration")
        long duration;

        public QueryTraceSpan(String name, String group, long duration) {
            this.name = name;
            this.duration = duration;
            this.group = group;
        }

        public QueryTraceSpan() {
        }
    }
}
