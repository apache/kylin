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
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import lombok.Data;

@Data
public class JobInfoResponse {
    @JsonProperty("jobs")
    private List<JobInfo> jobs;

    public static JobInfoResponse of(List<String> jobIds, String jobName) {
        Preconditions.checkNotNull(jobIds);
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(
                jobIds.stream().map(id -> new JobInfoResponse.JobInfo(jobName, id)).collect(Collectors.toList()));
        return jobInfoResponse;
    }

    @Data
    public static class JobInfo {
        @JsonProperty("job_name")
        private String jobName;

        @JsonProperty("job_id")
        private String jobId;

        public JobInfo() {
        }

        public JobInfo(String jobName, String jobId) {
            this.jobName = jobName;
            this.jobId = jobId;
        }
    }
}
