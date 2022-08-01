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

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Setter
@Getter
public class IndexStatResponse {

    @JsonProperty("max_data_size")
    private long maxDataSize;

    @JsonProperty("max_usage")
    private long maxUsage;

    @JsonProperty("need_create_base_table_index")
    private boolean needCreateBaseTableIndex;

    @JsonProperty("need_create_base_agg_index")
    private boolean needCreateBaseAggIndex;

    public static IndexStatResponse from(List<IndexResponse> results) {
        IndexStatResponse response = new IndexStatResponse();
        long maxUsage = 0;
        long maxDataSize = 0;

        for (IndexResponse index : results) {
            maxDataSize = Math.max(maxDataSize, index.getDataSize());
            maxUsage = Math.max(maxUsage, index.getUsage());
        }
        response.setMaxDataSize(maxDataSize);
        response.setMaxUsage(maxUsage);
        return response;
    }
}
