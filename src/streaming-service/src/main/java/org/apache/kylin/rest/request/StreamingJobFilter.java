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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class StreamingJobFilter {
    private String modelName;

    private List<String> modelNames;

    private List<String> jobTypes;

    private List<String> statuses;

    private String project;

    private String sortBy;

    private boolean reverse;

    private List<String> jobIds;

    public StreamingJobFilter(String modelName, List<String> modelNames, List<String> jobTypes, List<String> statuses,
            String project, String sortBy, boolean reverse) {
        this.modelName = modelName;
        this.modelNames = modelNames;
        this.jobTypes = jobTypes;
        this.statuses = statuses;
        this.project = project;
        this.sortBy = sortBy;
        this.reverse = reverse;
    }
}
