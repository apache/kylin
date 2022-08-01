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

import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class StreamingJobResponse extends StreamingJobMeta {

    @JsonProperty("model_broken")
    private boolean isModelBroken;
    @JsonProperty("data_latency")
    private Long dataLatency;
    @JsonProperty("last_status_duration")
    private Long lastStatusDuration;
    @JsonProperty("model_indexes")
    private Integer modelIndexes;
    @JsonProperty("launching_error")
    private boolean launchingError = false;
    @JsonProperty("partition_desc")
    private PartitionDesc partitionDesc;

    public StreamingJobResponse(StreamingJobMeta jobMeta) {
        setUuid(jobMeta.getUuid());
        setModelId(jobMeta.getModelId());
        setModelName(jobMeta.getModelName());
        setJobType(jobMeta.getJobType());
        setCurrentStatus(jobMeta.getCurrentStatus());
        setFactTableName(jobMeta.getFactTableName());
        setProject(jobMeta.getProject());
        setBroken(jobMeta.isBroken());
        setModelBroken(jobMeta.isBroken());
        setSkipListener(jobMeta.isSkipListener());
        setParams(jobMeta.getParams());
        setYarnAppId(jobMeta.getYarnAppId());
        setYarnAppUrl(jobMeta.getYarnAppUrl());
        setOwner(jobMeta.getOwner());
        setCreateTime(jobMeta.getCreateTime());
        setLastModified(jobMeta.getLastModified());
        setLastUpdateTime(jobMeta.getLastUpdateTime());
    }
}
