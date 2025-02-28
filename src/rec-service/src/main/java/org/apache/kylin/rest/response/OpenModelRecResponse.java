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

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OpenModelRecResponse implements Serializable {
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("alias")
    private String alias;
    @JsonProperty("version")
    protected String version;
    @JsonProperty("rec_items")
    private List<LayoutRecDetailResponse> indexes;

    public static OpenModelRecResponse convert(SuggestionResponse.ModelRecResponse response) {
        OpenModelRecResponse recommendationsResponse = new OpenModelRecResponse();
        recommendationsResponse.setUuid(response.getUuid());
        recommendationsResponse.setAlias(response.getAlias());
        recommendationsResponse.setVersion(response.getVersion());
        recommendationsResponse.setIndexes(response.getIndexes());
        return recommendationsResponse;
    }
}
