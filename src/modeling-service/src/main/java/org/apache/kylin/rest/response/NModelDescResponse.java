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

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class NModelDescResponse implements Serializable {
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("last_modified")
    private long lastModified;
    @JsonProperty("create_time")
    private long createTime;
    @JsonProperty("version")
    private String version;
    @JsonProperty("name")
    private String name;
    @JsonProperty("project")
    private String project;
    @JsonProperty("description")
    private String description;
    @JsonProperty("dimensions")
    private List<Dimension> dimensions;
    @JsonProperty("measures")
    private List<NDataModel.Measure> measures;
    @JsonProperty("aggregation_groups")
    private List<AggGroupResponse> aggregationGroups;
    @JsonProperty("computed_columns")
    private List<ComputedColumnDesc> computedColumnDescs;
    @JsonProperty("fact_table")
    private String factTable;
    @JsonProperty("join_tables")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<JoinTableDesc> joinTables;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Dimension implements Serializable {

        @JsonUnwrapped
        private NDataModel.NamedColumn namedColumn;

        @JsonProperty("derived")
        private boolean derived = false;
    }
}
