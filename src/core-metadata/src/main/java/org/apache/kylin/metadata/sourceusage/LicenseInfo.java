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
package org.apache.kylin.metadata.sourceusage;

import java.io.Serializable;

import org.apache.kylin.metadata.sourceusage.SourceUsageRecord.CapacityStatus;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class LicenseInfo implements Serializable {
    @JsonProperty("current_node")
    private int currentNode = 0;

    @JsonProperty("node")
    private int node = 0;

    @JsonProperty("node_status")
    private CapacityStatus nodeStatus = CapacityStatus.OK;

    @JsonProperty("current_capacity")
    private long currentCapacity = 0L;

    @JsonProperty("capacity")
    private long capacity = 0L;

    @JsonProperty("capacity_status")
    private CapacityStatus capacityStatus = CapacityStatus.OK;

    @JsonProperty("time")
    private long time;

    @JsonProperty("first_error_time")
    private long firstErrorTime;
}
