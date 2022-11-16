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

import java.util.List;

import org.apache.kylin.common.util.ArgsTypeJsonDeserializer;


import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EpochRequest implements ProjectInsensitiveRequest {

    @JsonDeserialize(using = ArgsTypeJsonDeserializer.ListJsonDeserializer.class)
    @JsonProperty("projects")
    private List<String> projects;

    @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
    @JsonProperty("force")
    private Boolean force;

    @JsonProperty("client")
    private boolean client = false;
}
