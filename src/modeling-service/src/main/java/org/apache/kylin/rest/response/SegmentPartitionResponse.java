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

import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnumToDisplay;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SegmentPartitionResponse {
    @JsonProperty("id")
    private long partitionId;

    @JsonProperty("values")
    private String[] values;

    @JsonProperty("status")
    private PartitionStatusEnumToDisplay status;

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

    @JsonProperty("source_count")
    private long sourceCount;

    @JsonProperty("bytes_size")
    private long bytesSize;

    public SegmentPartitionResponse(long id, String[] values, PartitionStatusEnum status, //
            long lastModifiedTime, long sourceCount, long bytesSize) {
        this.partitionId = id;
        this.values = values;
        this.lastModifiedTime = lastModifiedTime;
        this.sourceCount = sourceCount;
        this.bytesSize = bytesSize;
        setPartitionStatusToDisplay(status);
    }

    private void setPartitionStatusToDisplay(PartitionStatusEnum status) {
        if (PartitionStatusEnum.NEW == status) {
            this.status = PartitionStatusEnumToDisplay.LOADING;
        }

        if (PartitionStatusEnum.REFRESH == status) {
            this.status = PartitionStatusEnumToDisplay.REFRESHING;
        }

        if (PartitionStatusEnum.READY == status) {
            this.status = PartitionStatusEnumToDisplay.ONLINE;
        }
    }
}
