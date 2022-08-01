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

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class FavoriteRuleUpdateRequest implements Serializable, ProjectInsensitiveRequest {

    private String project;

    @Deprecated
    @JsonProperty("freq_enable")
    private boolean freqEnable;
    @Deprecated
    @JsonProperty("freq_value")
    private String freqValue;

    @JsonProperty("count_enable")
    private boolean countEnable;
    @JsonProperty("count_value")
    private String countValue;
    @JsonProperty("duration_enable")
    private boolean durationEnable;
    @JsonProperty("min_duration")
    private String minDuration;
    @JsonProperty("max_duration")
    private String maxDuration;
    @JsonProperty("submitter_enable")
    private boolean submitterEnable;
    private List<String> users;
    @JsonProperty("user_groups")
    private List<String> userGroups;
    @JsonProperty("recommendation_enable")
    private boolean recommendationEnable;
    @JsonProperty("recommendations_value")
    private String recommendationsValue;
    @JsonProperty("excluded_tables_enable")
    private boolean excludeTablesEnable;
    @JsonProperty("excluded_tables")
    private String excludedTables;
    @JsonProperty("min_hit_count")
    private String minHitCount;
    @JsonProperty("effective_days")
    private String effectiveDays;
    @JsonProperty("update_frequency")
    private String updateFrequency;
}
