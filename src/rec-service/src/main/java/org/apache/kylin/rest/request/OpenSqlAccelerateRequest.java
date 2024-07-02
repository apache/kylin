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

import org.apache.kylin.common.util.ArgsTypeJsonDeserializer;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OpenSqlAccelerateRequest implements Serializable, ProjectInsensitiveRequest {

    @JsonProperty("project")
    private String project;
    @JsonProperty("sqls")
    private List<String> sqls;
    @JsonProperty("force")
    private Boolean force2CreateNewModel;
    @JsonProperty("accept_recommendation_strategy")
    private String acceptRecommendationStrategy;
    @JsonProperty("accept_recommendation")
    @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
    private boolean acceptRecommendation = false;
    @JsonProperty("with_segment")
    @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
    private boolean withEmptySegment = true;
    @JsonProperty("with_model_online")
    @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
    private boolean withModelOnline = false;
    @JsonProperty("save_new_model")
    private boolean saveNewModel = true;
    @JsonProperty("with_base_index")
    private boolean withBaseIndex = false;
    @JsonProperty("with_optimal_models")
    private boolean withOptimalModel = false;
    @JsonProperty("need_build")
    private boolean needBuild = false;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("discard_table_index")
    @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
    private boolean discardTableIndex = false;

    public OpenSqlAccelerateRequest(String project, List<String> sqls, Boolean force2CreateNewModel) {
        this.project = project;
        this.sqls = sqls;
        this.force2CreateNewModel = force2CreateNewModel;
    }

}
