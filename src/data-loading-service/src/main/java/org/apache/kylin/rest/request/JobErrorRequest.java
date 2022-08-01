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

package org.apache.kylin.rest.request;

import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class JobErrorRequest implements ProjectInsensitiveRequest {
    private String project;
    @JsonProperty("job_id")
    private String jobId;
    @JsonProperty("failed_step_id")
    private String failedStepId;
    @JsonProperty("failed_segment_id")
    private String failedSegmentId;
    @JsonProperty("failed_stack")
    private String failedStack;
    @JsonProperty("failed_reason")
    private String failedReason;
}
