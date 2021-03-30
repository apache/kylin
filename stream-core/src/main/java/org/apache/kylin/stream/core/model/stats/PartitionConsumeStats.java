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

package org.apache.kylin.stream.core.model.stats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class PartitionConsumeStats {
    @JsonProperty("avg_rate")
    private double avgRate;
    @JsonProperty("one_min_rate")
    private double oneMinRate;
    @JsonProperty("five_min_rate")
    private double fiveMinRate;
    @JsonProperty("fifteen_min_rate")
    private double fifteenMinRate;
    @JsonProperty("total_consume")
    private long totalConsume;
    @JsonProperty("consume_lag")
    private long consumeLag;

    public double getAvgRate() {
        return avgRate;
    }

    public void setAvgRate(double avgRate) {
        this.avgRate = avgRate;
    }

    public double getOneMinRate() {
        return oneMinRate;
    }

    public void setOneMinRate(double oneMinRate) {
        this.oneMinRate = oneMinRate;
    }

    public double getFiveMinRate() {
        return fiveMinRate;
    }

    public void setFiveMinRate(double fiveMinRate) {
        this.fiveMinRate = fiveMinRate;
    }

    public double getFifteenMinRate() {
        return fifteenMinRate;
    }

    public void setFifteenMinRate(double fifteenMinRate) {
        this.fifteenMinRate = fifteenMinRate;
    }

    public long getTotalConsume() {
        return totalConsume;
    }

    public void setTotalConsume(long totalConsume) {
        this.totalConsume = totalConsume;
    }

    public long getConsumeLag() {
        return consumeLag;
    }

    public void setConsumeLag(long consumeLag) {
        this.consumeLag = consumeLag;
    }
}
