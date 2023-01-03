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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kylin.common.util.Pair;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TableDescRequest {

    @JsonProperty(required = true)
    private String project;

    private String table;

    private String database;

    @JsonProperty(value = "ext")
    private boolean withExt;

    @JsonProperty(value = "is_fuzzy", defaultValue = "false")
    private boolean isFuzzy;

    @JsonProperty(value = "page_offset", defaultValue = "0")
    private Integer offset;

    @JsonProperty(value = "page_size", defaultValue = "10")
    private Integer limit;

    @JsonProperty(value = "source_type", defaultValue = "9")
    private List<Integer> sourceType;

    @JsonProperty(value = "with_excluded", defaultValue = "true")
    private boolean withExcluded;

    public TableDescRequest(String project, boolean withExt, String table, String database, boolean isFuzzy, List<Integer> sourceType) {
        this.project = project;
        this.withExt = withExt;
        this.table = table;
        this.database = database;
        this.isFuzzy = isFuzzy;
        this.sourceType = sourceType;
    }

    public TableDescRequest(String project, String table, Integer offset, Integer limit, boolean withExcluded, List<Integer> sourceType) {
        this.project = project;
        this.table = table;
        this.offset = offset;
        this.limit = limit;
        this.withExcluded = withExcluded;
        this.sourceType = sourceType;
    }

    public TableDescRequest(String project, String table, String database, boolean withExt, boolean isFuzzy,
                            Pair<Integer, Integer> offsetAndLimit, List<Integer> sourceType) {
        this.project = project;
        this.table = table;
        this.database = database;
        this.withExt = withExt;
        this.isFuzzy = isFuzzy;
        this.offset = offsetAndLimit.getFirst();
        this.limit = offsetAndLimit.getSecond();
        this.sourceType = sourceType;
    }
}
