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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Setter
@Getter
public class SnapshotSourceTableStatsResponse {
    @JsonProperty("need_refresh")
    private Boolean needRefresh = Boolean.FALSE;
    @JsonProperty("need_refresh_partitions_value")
    private Set<String> needRefreshPartitionsValue = Sets.newHashSet();

    public SnapshotSourceTableStatsResponse(Boolean needRefresh) {
        this.needRefresh = needRefresh;
    }

    public SnapshotSourceTableStatsResponse(Boolean needRefresh, Set<String> needRefreshPartitionsValue) {
        this.needRefresh = needRefresh;
        this.needRefreshPartitionsValue = needRefreshPartitionsValue;
    }
}
