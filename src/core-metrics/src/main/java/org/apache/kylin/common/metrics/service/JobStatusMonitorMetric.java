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
package org.apache.kylin.common.metrics.service;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kylin.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobStatusMonitorMetric extends MonitorMetric {
    public static final String JOB_STATUS_MONITOR_METRIC_TABLE = "tb_job_status";

    @JsonProperty("finished_jobs")
    @Column(name = "finished_jobs")
    private Long finishedJobs = 0L;

    @JsonProperty("pending_jobs")
    @Column(name = "pending_jobs")
    private Long pendingJobs = 0L;

    @JsonProperty("error_jobs")
    @Column(name = "error_jobs")
    private Long errorJobs = 0L;

    @JsonProperty("running_jobs")
    @Column(name = "running_jobs")
    private Long runningJobs = 0L;

    @Override
    public Map<String, Object> getFields() {
        Map<String, Object> fields = super.getFields();
        fields.put("finished_jobs", this.getFinishedJobs());
        fields.put("pending_jobs", this.getPendingJobs());
        fields.put("error_jobs", this.getErrorJobs());
        fields.put("running_jobs", this.getRunningJobs());
        return fields;
    }

    @Override
    public String getTable() {
        return JOB_STATUS_MONITOR_METRIC_TABLE;
    }
}
