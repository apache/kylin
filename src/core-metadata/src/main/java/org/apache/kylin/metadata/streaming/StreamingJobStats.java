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

package org.apache.kylin.metadata.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StreamingJobStats {

    // table names
    public static final String STREAMING_JOB_STATS_SUFFIX = "streaming_job_stats";

    @JsonProperty("id")
    private Long id;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("project_name")
    private String projectName;

    @JsonProperty("batch_row_num")
    private Long batchRowNum;

    @JsonProperty("rows_per_second")
    private Double rowsPerSecond;

    @JsonProperty("processing_time")
    private Long processingTime;

    @JsonProperty("min_data_latency")
    private Long minDataLatency;

    @JsonProperty("max_data_latency")
    private Long maxDataLatency;

    @JsonProperty("create_time")
    private Long createTime;

    public StreamingJobStats() {

    }

    public StreamingJobStats(String jobId, String projectName, Long batchRowNum, Double rowsPerSecond, Long durationMs,
            Long minDataLatency, Long maxDataLatency, Long createTime) {
        this.jobId = jobId;
        this.projectName = projectName;
        this.batchRowNum = batchRowNum;
        this.rowsPerSecond = rowsPerSecond;
        this.processingTime = durationMs;
        this.minDataLatency = minDataLatency;
        this.maxDataLatency = maxDataLatency;
        this.createTime = createTime;
    }

}
