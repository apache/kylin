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
package org.apache.kylin.common.persistence.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SegmentRawResourceWrap {
    @JsonProperty("id")
    private String id;
    @JsonProperty("model_uuid")
    private String modelUuid;

    @JsonProperty("uuid")
    private String uuid;

    @JsonProperty("project")
    private String project;

    @JsonProperty("reserved_filed_1")
    private String reservedFiled1;

    @JsonProperty("reserved_filed_2")
    private byte[] reservedFiled2;

    @JsonProperty("reserved_filed_3")
    private byte[] reservedFiled3;
}
