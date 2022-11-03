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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LayoutMetric {
    private FrequencyMap frequencyMap;
    private LatencyMap latencyMap;

    public LayoutMetric(FrequencyMap frequencyMap, LatencyMap latencyMap) {
        this.frequencyMap = frequencyMap;
        this.latencyMap = latencyMap;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LatencyMap implements Serializable {
        /**
         *
         */
        @JsonIgnore
        private NavigableMap<Long, Long> totalLatencyMapPerDay = new TreeMap<>();

        @JsonAnySetter
        public void add(String key, long value) {
            totalLatencyMapPerDay.put(Long.parseLong(key), value);
        }

        public void incLatency(long queryTime, long value) {
            long totalLatency = totalLatencyMapPerDay.getOrDefault(getDateInMillis(queryTime), 0L);
            totalLatencyMapPerDay.put(getDateInMillis(queryTime), totalLatency + value);
        }

        @JsonAnyGetter
        public Map<Long, Long> getMap() {
            return totalLatencyMapPerDay;
        }

        public LatencyMap merge(LatencyMap other) {
            other.getTotalLatencyMapPerDay().forEach((k, v) -> this.totalLatencyMapPerDay.merge(k, v, Long::sum));
            return this;
        }

        @JsonIgnore
        public double getLatencyByDate(long queryTime) {
            return totalLatencyMapPerDay.get(getDateInMillis(queryTime)) == null ? 0
                    : totalLatencyMapPerDay.get(getDateInMillis(queryTime));
        }

        private long getDateInMillis(final long queryTime) {
            return TimeUtil.getDayStart(queryTime);
        }

    }
}
