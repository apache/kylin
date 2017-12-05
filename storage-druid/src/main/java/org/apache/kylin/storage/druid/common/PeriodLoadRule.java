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

package org.apache.kylin.storage.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

import java.util.Map;
import java.util.Objects;

public class PeriodLoadRule extends Rule {
    private final Period period;
    private final Map<String, Integer> tieredReplicants;

    @JsonCreator
    public PeriodLoadRule(
            @JsonProperty("period") Period period,
            @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants) {
        this.period = period;
        this.tieredReplicants = tieredReplicants;
        validateTieredReplicants(tieredReplicants);
    }

    @Override
    @JsonProperty
    public String getType() {
        return "loadByPeriod";
    }

    @JsonProperty
    public Period getPeriod() {
        return period;
    }

    @JsonProperty
    public Map<String, Integer> getTieredReplicants() {
        return tieredReplicants;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeriodLoadRule)) return false;
        PeriodLoadRule that = (PeriodLoadRule) o;
        return Objects.equals(period, that.period) &&
                Objects.equals(tieredReplicants, that.tieredReplicants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(period, tieredReplicants);
    }
}
