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

import java.util.Collection;
import java.util.List;

import org.apache.kylin.rest.util.PagingUtil;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FusionRuleDataResult<T extends Collection> {

    private T value;

    @JsonProperty("total_size")
    private int totalSize = 0;

    private int offset = 0;

    private int limit = 0;

    @JsonProperty("index_update_enabled")
    private boolean indexUpdateEnabled = true;

    public static <T extends Collection> FusionRuleDataResult<T> get(T data, T allData, int offset, int limit,
            boolean enabled) {
        if (null == allData) {
            return new FusionRuleDataResult<>(data, 0, offset, limit, enabled);
        }

        return new FusionRuleDataResult<>(data, allData.size(), offset, limit, enabled);
    }

    public static <E> FusionRuleDataResult<List<E>> get(List<E> data, int offset, int limit, boolean enabled) {
        return get(PagingUtil.cutPage(data, offset, limit), data, offset, limit, enabled);
    }
}
