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

import org.apache.kylin.metadata.model.ComputedColumnDesc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Data;

@Data
public class InvalidIndexesResponse {
    @JsonProperty("computed_columns")
    private List<ComputedColumnDesc> ccList = Lists.newArrayList();
    @JsonProperty("dimensions")
    private List<String> dimensions = Lists.newArrayList();
    @JsonProperty("measures")
    private List<String> measures = Lists.newArrayList();
    @JsonProperty("indexes")
    private List<Long> indexes = Lists.newArrayList();
    @JsonProperty("agg_index_count")
    private int invalidAggIndexCount;
    @JsonProperty("table_index_count")
    private int invalidTableIndexCount;
    @JsonProperty("anti_flatten_lookups")
    private List<String> antiFlattenLookups;
}
