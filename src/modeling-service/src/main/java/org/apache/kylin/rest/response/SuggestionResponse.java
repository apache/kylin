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

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class SuggestionResponse {
    @JsonProperty("reused_models")
    List<ModelRecResponse> reusedModels;
    @JsonProperty("new_models")
    List<ModelRecResponse> newModels;
    @JsonProperty("optimal_models")
    List<ModelRecResponse> optimalModels;

    public SuggestionResponse(List<ModelRecResponse> reusedModels, List<ModelRecResponse> newModels) {
        this.reusedModels = reusedModels;
        this.newModels = newModels;
        this.optimalModels = Lists.newArrayList();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    public static class ModelRecResponse extends NDataModel {
        @JsonProperty("rec_items")
        private List<LayoutRecDetailResponse> indexes = Lists.newArrayList();
        @JsonProperty("index_plan")
        private IndexPlan indexPlan;

        @Override
        @JsonGetter("computed_columns")
        public List<ComputedColumnDesc> getComputedColumnDescs() {
            return this.computedColumnDescs;
        }

        public ModelRecResponse(NDataModel dataModel) {
            super(dataModel);
        }
    }
}
