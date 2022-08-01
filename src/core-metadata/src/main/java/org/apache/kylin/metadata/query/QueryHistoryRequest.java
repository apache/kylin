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

package org.apache.kylin.metadata.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class QueryHistoryRequest {

    private String project;

    private String startTimeFrom;
    private String startTimeTo;
    private String latencyFrom;
    private String latencyTo;
    private String sql;
    private String server;
    // Userd to filter results with submitter name
    private List<String> filterSubmitter;
    // Userd to filter results with model name
    private String filterModelName;
    // Userd to filter results with models
    private List<String> filterModelIds;
    private List<String> queryStatus;

    List<String> realizations;

    public QueryHistoryRequest(String project, String startTimeFrom, String startTimeTo) {
        this.project = project;
        this.startTimeFrom = startTimeFrom;
        this.startTimeTo = startTimeTo;
    }

    @JsonIgnore
    private boolean isAdmin;
    @JsonIgnore
    private String username;
    private boolean isSubmitterExactlyMatch;
}
