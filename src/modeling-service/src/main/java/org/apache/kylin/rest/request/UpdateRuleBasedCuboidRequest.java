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

import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;
import org.springframework.beans.BeanUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UpdateRuleBasedCuboidRequest implements ProjectInsensitiveRequest {

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("aggregation_groups")
    private List<NAggregationGroup> aggregationGroups;

    @JsonProperty("global_dim_cap")
    private Integer globalDimCap;

    @Builder.Default
    @JsonProperty("scheduler_version")
    private int schedulerVersion = 1;

    @Builder.Default
    @JsonProperty("load_data")
    private boolean isLoadData = true;

    @Builder.Default
    @JsonProperty("restore_deleted_index")
    private boolean restoreDeletedIndex = false;

    public RuleBasedIndex convertToRuleBasedIndex() {
        val newRuleBasedCuboid = new RuleBasedIndex();
        BeanUtils.copyProperties(this, newRuleBasedCuboid);
        newRuleBasedCuboid.adjustMeasures();
        newRuleBasedCuboid.adjustDimensions();
        newRuleBasedCuboid.setGlobalDimCap(globalDimCap);

        return newRuleBasedCuboid;
    }

    public static UpdateRuleBasedCuboidRequest convertToRequest(String project, String modelId, boolean isLoadData,
            RuleBasedIndex ruleBasedIndex) {
        UpdateRuleBasedCuboidRequest updateRuleBasedCuboidRequest = new UpdateRuleBasedCuboidRequest();
        BeanUtils.copyProperties(ruleBasedIndex, updateRuleBasedCuboidRequest);
        updateRuleBasedCuboidRequest.setLoadData(isLoadData);
        updateRuleBasedCuboidRequest.setProject(project);
        updateRuleBasedCuboidRequest.setModelId(modelId);
        return updateRuleBasedCuboidRequest;
    }
}
