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

package org.apache.kylin.metadata.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
public class QueryStatistics {
    @JsonProperty("engine_type")
    private String engineType;

    @JsonProperty("count")
    private long count;

    @JsonProperty("ratio")
    private double ratio;

    @JsonProperty("mean")
    private double meanDuration;

    @JsonProperty("model")
    private String model;

    @JsonProperty("time")
    private Instant time;

    @JsonProperty("month")
    private String month;

    public QueryStatistics(String engineType) {
        this.engineType = engineType;
    }

    public void apply(final QueryStatistics other) {
        this.count = other.count;
        this.ratio = other.ratio;
        this.meanDuration = other.meanDuration;
    }

    public void updateRatio(double amount) {
        if (amount > 0d) {
            // Keep two decimals
            this.ratio = ((double) Math.round(((double) count) / amount * 100d)) / 100d;
        }
    }
}
