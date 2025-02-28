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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

import java.util.List;

import org.apache.kylin.rest.model.GlutenCacheExecuteResult;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = NONE, getterVisibility = NONE, isGetterVisibility = NONE, setterVisibility = NONE)
public class GlutenCacheResponse {
    @JsonProperty("result")
    private Boolean result;
    @JsonProperty("execute_results")
    private List<GlutenCacheExecuteResult> executeResults;
    @JsonProperty("exception_message")
    private String exceptionMessage;

    public GlutenCacheResponse(List<GlutenCacheExecuteResult> executeResults, String exceptionMessage) {
        this.result = false;
        this.executeResults = executeResults;
        this.exceptionMessage = exceptionMessage;
    }
}
