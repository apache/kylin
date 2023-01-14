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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class TableExclusionRequest implements Serializable {
    @JsonProperty("project")
    private String project;
    @JsonProperty("canceled_tables")
    private List<String> canceledTables = Lists.newArrayList();
    @JsonProperty("excluded_tables")
    private List<ExcludedTable> excludedTables = Lists.newArrayList();

    @Data
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class ExcludedTable implements Serializable {
        @JsonProperty("table")
        private String table;
        @JsonProperty("excluded")
        private boolean excluded;
        @JsonProperty("removed_columns")
        private List<String> removedColumns = Lists.newArrayList();
        @JsonProperty("added_columns")
        private List<String> addedColumns = Lists.newArrayList();
    }
}
