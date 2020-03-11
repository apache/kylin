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

import java.util.List;
import java.util.Map;

import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.storage.columnar.ColumnarStoreCacheStats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ReceiverStats {
    @JsonProperty("assignments")
    private Map<String, List<Partition>> assignments;

    @JsonProperty("is_lead")
    private boolean isLead;

    @JsonProperty("cube_stats")
    private Map<String, ReceiverCubeStats> cubeStatsMap = Maps.newHashMap();

    @JsonProperty("cache_stats")
    private ColumnarStoreCacheStats cacheStats;

    public void addCubeStats(String cubeName, ReceiverCubeStats cubeStats) {
        cubeStatsMap.put(cubeName, cubeStats);
    }

    public Map<String, List<Partition>> getAssignments() {
        return assignments;
    }

    public void setAssignments(Map<String, List<Partition>> assignments) {
        this.assignments = assignments;
    }

    public boolean isLead() {
        return isLead;
    }

    public void setLead(boolean lead) {
        isLead = lead;
    }

    public Map<String, ReceiverCubeStats> getCubeStatsMap() {
        return cubeStatsMap;
    }

    public void setCubeStatsMap(Map<String, ReceiverCubeStats> cubeStatsMap) {
        this.cubeStatsMap = cubeStatsMap;
    }

    public ColumnarStoreCacheStats getCacheStats() {
        return cacheStats;
    }

    public void setCacheStats(ColumnarStoreCacheStats cacheStats) {
        this.cacheStats = cacheStats;
    }
}
