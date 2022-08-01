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
package org.apache.kylin.metadata.favorite;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class QueryHistoryTimeOffset extends RootPersistentEntity {
    @JsonProperty("auto_mark_time_offset")
    private long autoMarkTimeOffset;
    @JsonProperty("favorite_query_update_time_offset")
    private long favoriteQueryUpdateTimeOffset;

    public QueryHistoryTimeOffset(long autoMarkTimeOffset, long favoriteQueryUpdateTimeOffset) {
        this.autoMarkTimeOffset = autoMarkTimeOffset;
        this.favoriteQueryUpdateTimeOffset = favoriteQueryUpdateTimeOffset;
    }
}
