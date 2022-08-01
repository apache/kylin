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

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class QueryStatistics {
    @JsonProperty("engine_type")
    @Column(name = "engine_type", tag = true)
    private String engineType;

    @JsonProperty("count")
    @Column(name = "count")
    private long count;

    @JsonProperty("ratio")
    private double ratio;

    @JsonProperty("mean")
    @Column(name = "mean")
    private double meanDuration;

    @JsonProperty("model")
    @Column(name = "model", tag = true)
    private String model;

    @JsonProperty("time")
    @Column(name = "time")
    private Instant time;

    @JsonProperty("month")
    @Column(name = "month", tag = true)
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
