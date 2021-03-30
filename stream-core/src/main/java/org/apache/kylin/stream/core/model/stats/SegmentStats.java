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
public class SegmentStats {
    @JsonProperty("segment_state")
    private String segmentState;

    @JsonProperty("segment_create_time")
    private long segmentCreateTime;

    @JsonProperty("segment_last_update_time")
    private long segmentLastUpdateTime;

    @JsonProperty("latest_event_time")
    private long latestEventTime;

    @JsonProperty("latest_event_latency")
    private long latestEventLatency;

    @JsonProperty("store_stats")
    private SegmentStoreStats storeStats;

    public String getSegmentState() {
        return segmentState;
    }

    public void setSegmentState(String segmentState) {
        this.segmentState = segmentState;
    }

    public SegmentStoreStats getStoreStats() {
        return storeStats;
    }

    public void setStoreStats(SegmentStoreStats storeStats) {
        this.storeStats = storeStats;
    }

    public long getSegmentCreateTime() {
        return segmentCreateTime;
    }

    public void setSegmentCreateTime(long segmentCreateTime) {
        this.segmentCreateTime = segmentCreateTime;
    }

    public long getSegmentLastUpdateTime() {
        return segmentLastUpdateTime;
    }

    public void setSegmentLastUpdateTime(long segmentLastUpdateTime) {
        this.segmentLastUpdateTime = segmentLastUpdateTime;
    }

    public long getLatestEventTime() {
        return latestEventTime;
    }

    public void setLatestEventTime(long latestEventTime) {
        this.latestEventTime = latestEventTime;
    }

    public long getLatestEventLatency() {
        return latestEventLatency;
    }

    public void setLatestEventLatency(long latestEventLatency) {
        this.latestEventLatency = latestEventLatency;
    }
}
