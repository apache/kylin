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

package org.apache.kylin.job.rest;

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableState;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JobMapperFilter {

    // schedule states
    private List<ExecutableState> statuses;

    // jobName is jobType
    private List<String> jobNames;

    private long queryStartTime;

    // for sql condition: in (...)
    private List<String> subjects;

    // for sql condition: in (...)
    private List<String> modelIds;

    // for fuzzy match
    private String jobId;

    // for sql condition: in (...)
    private List<String> jobIds;

    private String project;

    private String orderByFiled;

    private String orderType;

    @Builder.Default
    private int offset = -1;

    @Builder.Default
    private int limit = -1;

    // placeholder for mybatis ${}
    private String jobInfoTable;

    private List<Long> timeRange;

    public void setStatuses(List<ExecutableState> stateList) {
        statuses = stateList;
    }
    
    public void setStatuses(ExecutableState... states) {
        statuses = Lists.newArrayList(states);
    }
}
