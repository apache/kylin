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

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProjectStatisticsResponse {
    @JsonProperty("database_size")
    private int databaseSize;
    @JsonProperty("table_size")
    private int tableSize;
    @JsonProperty("last_week_query_count")
    private long lastWeekQueryCount;
    @JsonProperty("unhandled_query_count")
    private long unhandledQueryCount;
    @JsonProperty("additional_rec_pattern_count")
    private int additionalRecPatternCount;
    @JsonProperty("removal_rec_pattern_count")
    private int removalRecPatternCount;
    @JsonProperty("rec_pattern_count")
    private int recPatternCount;
    @JsonProperty("effective_rule_size")
    private int effectiveRuleSize;
    @JsonProperty("approved_rec_count")
    private int approvedRecCount;
    @JsonProperty("approved_additional_rec_count")
    private int approvedAdditionalRecCount;
    @JsonProperty("approved_removal_rec_count")
    private int approvedRemovalRecCount;
    @JsonProperty("model_size")
    private int modelSize;
    @JsonProperty("acceptable_rec_size")
    private int acceptableRecSize;
    @JsonProperty("is_refreshed")
    private boolean refreshed;
    @JsonProperty("max_rec_show_size")
    private int maxRecShowSize;
}
