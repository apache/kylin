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
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class HealthResponse {
    @JsonProperty("spark_status")
    private RestartSparkStatusResponse restartSparkStatus;
    @JsonProperty("slow_queries_status")
    private List<CanceledSlowQueryStatusResponse> canceledSlowQueryStatus;

    @Getter
    @Setter
    @AllArgsConstructor
    public static class RestartSparkStatusResponse {
        @JsonProperty("restart_failure_times")
        private int startSparkFailureTimes;
        @JsonProperty("last_restart_failure_time")
        private long lastStartSparkFailureTime;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class CanceledSlowQueryStatusResponse {
        @JsonProperty("query_id")
        private String queryId;
        @JsonProperty("canceled_times")
        private int canceledTimes;
        @JsonProperty("last_canceled_time")
        private long lastCanceledTime;
        @JsonProperty("duration_time")
        private float queryDurationTime;
    }
}
