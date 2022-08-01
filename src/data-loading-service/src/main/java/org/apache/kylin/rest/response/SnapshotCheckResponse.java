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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class SnapshotCheckResponse {

    @JsonProperty("affected_jobs")
    List<JobInfo> affectedJobs;

    public SnapshotCheckResponse() {
        affectedJobs = new ArrayList<>();
    }

    public void addAffectedJobs(String id, String database, String table) {
        affectedJobs.add(new JobInfo(id, database, table));
    }

    private static class JobInfo {
        @JsonProperty("database")
        String database;
        @JsonProperty("table")
        String table;
        @JsonProperty("job_id")
        String jobId;

        public JobInfo(String jobId, String database, String table) {
            this.database = database;
            this.table = table;
            this.jobId = jobId;
        }
    }
}
