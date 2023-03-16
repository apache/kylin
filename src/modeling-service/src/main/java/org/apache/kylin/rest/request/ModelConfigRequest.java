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

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.RetentionRange;
import org.apache.kylin.metadata.model.VolatileRange;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.Data;

@Data
public class ModelConfigRequest implements ProjectInsensitiveRequest {

    @JsonProperty("project")
    private String project;
    @JsonProperty("auto_merge_enabled")
    private Boolean autoMergeEnabled;
    @JsonProperty("auto_merge_time_ranges")
    private List<AutoMergeTimeEnum> autoMergeTimeRanges;
    @JsonProperty("volatile_range")
    private VolatileRange volatileRange;
    @JsonProperty("retention_range")
    private RetentionRange retentionRange;
    @JsonProperty("override_props")
    LinkedHashMap<String, String> overrideProps = Maps.newLinkedHashMap();
}
