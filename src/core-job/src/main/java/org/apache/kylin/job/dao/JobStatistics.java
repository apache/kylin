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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class JobStatistics extends JobStatisticsBasic {

    @JsonProperty("date")
    private long date;
    @JsonProperty("model_stats")
    private Map<String, JobStatisticsBasic> jobStatisticsByModels = Maps.newHashMap();

    @Override
    public String resourceName() {
        return String.valueOf(date);
    }

    public JobStatistics(long date, String model, long duration, long byteSize) {
        this.date = date;
        setCount(1);
        setTotalDuration(duration);
        setTotalByteSize(byteSize);
        jobStatisticsByModels.put(model, new JobStatisticsBasic(duration, byteSize));
    }

    public JobStatistics(int count, long totalDuration, long totalByteSize) {
        setCount(count);
        setTotalDuration(totalDuration);
        setTotalByteSize(totalByteSize);
    }

    public JobStatistics(long date, long totalDuration, long totalByteSize) {
        this.date = date;
        setCount(1);
        setTotalDuration(totalDuration);
        setTotalByteSize(totalByteSize);
    }

    public void update(String model, long duration, long byteSize, int deltaCount) {
        super.update(duration, byteSize, deltaCount);
        JobStatisticsBasic jobStatisticsByModel = jobStatisticsByModels.get(model);
        if (jobStatisticsByModel == null) {
            jobStatisticsByModel = new JobStatisticsBasic(duration, byteSize);
        } else {
            jobStatisticsByModel.update(duration, byteSize, deltaCount);
        }

        jobStatisticsByModels.put(model, jobStatisticsByModel);
    }
}
