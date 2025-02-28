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

package org.apache.kylin.job.dao;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class JobStatisticsBasic extends RootPersistentEntity {
    @JsonProperty("count")
    private int count;
    @JsonProperty("total_duration")
    private long totalDuration;
    @JsonProperty("total_byte_size")
    private long totalByteSize;

    public void update(long duration, long byteSize, int deltaCount) {
        this.count += deltaCount;
        this.totalDuration += duration;
        this.totalByteSize += byteSize;
    }

    public JobStatisticsBasic(int count, long totalDuration, long totalByteSize) {
        this.count = count;
        this.totalDuration = totalDuration;
        this.totalByteSize = totalByteSize;
    }

    public JobStatisticsBasic(long totalDuration, long totalByteSize) {
        this(1, totalDuration, totalByteSize);
    }
}
