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

package org.apache.kylin.job.metrics;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kylin.shaded.influxdb.org.influxdb.annotation.Column;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
public class JobMetricsStatistics {

    @JsonProperty("count")
    @Column(name = "count")
    private int count;

    @JsonProperty("duration")
    @Column(name = "duration")
    private long duration;

    @JsonProperty("model_size")
    @Column(name = "model_size")
    private long modelSize;

    @JsonProperty("time")
    @Column(name = "time")
    private Instant time;

    @JsonProperty("model")
    @Column(name = "model", tag = true)
    private String model;
}
