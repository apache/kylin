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

import org.apache.kylin.common.util.ArgsTypeJsonDeserializer;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Data;

@Data
public class BuildIndexRequest implements ProjectInsensitiveRequest {

    @JsonProperty("project")
    private String project;

    @JsonDeserialize(using = ArgsTypeJsonDeserializer.IntegerJsonDeserializer.class)
    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    @JsonProperty("yarn_queue")
    private String yarnQueue;

    @JsonProperty("tag")
    private Object tag;
}
