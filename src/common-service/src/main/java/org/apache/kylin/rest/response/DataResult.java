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
public class DataResult<T extends Collection> {

    private T value;

    @JsonProperty("total_size")
    private int totalSize = 0;

    private int offset = 0;

    private int limit = 0;

    public DataResult(T data, int totalSize) {
        this.value = data;
        this.totalSize = totalSize;
    }

    public static <T extends Collection> DataResult<T> get(T data, int totalSize) {
        return new DataResult<>(data, totalSize);
    }

    public static <T extends Collection> DataResult<T> get(T data, T allData) {
        return get(data, allData, 0, 0);
    }

    public static <T extends Collection> DataResult<T> get(T data, T allData, int offset, int limit) {
        if (null == allData) {
            return new DataResult<>(data, 0, offset, limit);
        }

        return new DataResult<>(data, allData.size(), offset, limit);
    }

    public static <E> DataResult<List<E>> get(List<E> data, int offset, int limit) {
        return get(PagingUtil.cutPage(data, offset, limit), data, offset, limit);
    }
}
