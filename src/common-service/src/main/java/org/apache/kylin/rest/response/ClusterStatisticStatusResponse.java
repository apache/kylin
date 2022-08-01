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

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class ClusterStatisticStatusResponse {
    @JsonProperty("start")
    private long start;
    @JsonProperty("end")
    private long end;
    @JsonProperty("interval")
    private long interval;

    @JsonProperty("job")
    private List<NodeStatisticStatusResponse> job;
    @JsonProperty("query")
    private List<NodeStatisticStatusResponse> query;

    @Data
    public static class NodeStatisticStatusResponse {
        @JsonProperty("instance")
        private String instance;
        @JsonProperty("details")
        private List<NodeTimeState> details;
        @JsonProperty("unavailable_time")
        private long unavailableTime;
        @JsonProperty("unavailable_count")
        private int unavailableCount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodeTimeState {
        @JsonProperty("time")
        private long time;
        @JsonProperty("status")
        private ClusterStatusResponse.NodeState state;
    }
}
