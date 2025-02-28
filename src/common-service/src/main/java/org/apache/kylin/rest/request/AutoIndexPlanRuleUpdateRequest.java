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

import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class AutoIndexPlanRuleUpdateRequest implements Serializable, ProjectInsensitiveRequest {

    // ---------------------- core confs ----------------------
    @JsonProperty("project")
    private String project;
    @JsonProperty("model")
    private String model;
    @JsonProperty("index_planner_enable")
    private boolean indexPlannerEnable;
    @JsonProperty("index_planner_max_index_count")
    private Integer indexPlannerMaxIndexCount = 20;
    @JsonProperty("index_planner_max_change_count")
    private Integer indexPlannerMaxChangeCount = 100;
    @JsonProperty("index_planner_level")
    private String indexPlannerLevel = FavoriteRule.IndexPlannerLevelType.BALANCED.name();

    // ---------------------- Deprecated It will be deleted later ----------------------
    @JsonProperty("auto_index_plan_option")
    private int autoIndexPlanOption = 0;
    @JsonProperty("auto_index_plan_auto_change_index_enable")
    private boolean autoIndexPlanAutoChangeIndexEnable = true;
    // ABSOLUTE, RELATIVE
    @JsonProperty("auto_index_plan_auto_complete_mode")
    private String autoIndexPlanAutoCompleteMode = FavoriteRule.AutoCompleteModeType.ABSOLUTE.name();
    // LIKE '2023-01-01'
    @JsonProperty("auto_index_plan_absolute_begin_date")
    private String autoIndexPlanAbsoluteBeginDate = "1990-01-01";
    // YEAR, MONTH, DAY
    @JsonProperty("auto_index_plan_relative_time_unit")
    private String autoIndexPlanRelativeTimeUnit = FavoriteRule.DateUnitType.DAY.name();
    @JsonProperty("auto_index_plan_relative_time_interval")
    private Integer autoIndexPlanRelativeTimeInterval = 12;
    @JsonProperty("auto_index_plan_segment_job_enable")
    private boolean autoIndexPlanSegmentJobEnable = false;

}
