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

package org.apache.kylin.streaming.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class StreamingJobStatsRequest extends StreamingRequestHeader {

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("project")
    private String project;

    @JsonProperty("batch_row_num")
    private Long batchRowNum;

    @JsonProperty("window_size")
    private Long windowSize;

    @JsonProperty("rows_per_second")
    private Double rowsPerSecond;

    @JsonProperty("trigger_start_time")
    private Long triggerStartTime;

    /**
     * progress.durationMs
     */
    @JsonProperty("processing_time")
    private Long processingTime;

    @JsonProperty("min_data_latency")
    private Long minDataLatency;

    @JsonProperty("max_data_latency")
    private Long maxDataLatency;

    public StreamingJobStatsRequest() {

    }

    public StreamingJobStatsRequest(String jobId, String project, Long batchRowNum, Double rowsPerSecond,
            Long durationMs, Long triggerStartTime, Long minDataLatency, Long maxDataLatency) {
        this.jobId = jobId;
        this.project = project;
        this.batchRowNum = batchRowNum;
        this.rowsPerSecond = rowsPerSecond;
        this.processingTime = durationMs;
        this.triggerStartTime = triggerStartTime;
        this.minDataLatency = minDataLatency;
        this.maxDataLatency = maxDataLatency;
    }
}
