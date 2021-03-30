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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ReceiverCubeStats {
    @JsonProperty("segment_stats")
    private Map<String, SegmentStats> segmentStatsMap = Maps.newHashMap();

    //Consumer stats will not survive after streaming receiver restart
    @JsonProperty("consumer_stats")
    private ConsumerStats consumerStats;

    //Total ingest count will survive after streaming receiver restart
    @JsonProperty("total_ingest")
    private long totalIngest;

    @JsonProperty("latest_event_time")
    private long latestEventTime;

    @JsonProperty("latest_event_ingest_time")
    private long latestEventIngestTime;

    @JsonProperty("long_latency_info")
    private LongLatencyInfo longLatencyInfo;

    public ConsumerStats getConsumerStats() {
        return consumerStats;
    }

    public void setConsumerStats(ConsumerStats consumerStats) {
        this.consumerStats = consumerStats;
    }

    public long getTotalIngest() {
        return totalIngest;
    }

    public void setTotalIngest(long totalIngest) {
        this.totalIngest = totalIngest;
    }

    public void addSegmentStats(String segmentName, SegmentStats segmentStats) {
        segmentStatsMap.put(segmentName, segmentStats);
    }

    public Map<String, SegmentStats> getSegmentStatsMap() {
        return segmentStatsMap;
    }

    public void setSegmentStatsMap(Map<String, SegmentStats> segmentStatsMap) {
        this.segmentStatsMap = segmentStatsMap;
    }

    public long getLatestEventTime() {
        return latestEventTime;
    }

    public void setLatestEventTime(long latestEventTime) {
        this.latestEventTime = latestEventTime;
    }

    public long getLatestEventIngestTime() {
        return latestEventIngestTime;
    }

    public void setLatestEventIngestTime(long latestEventIngestTime) {
        this.latestEventIngestTime = latestEventIngestTime;
    }

    public LongLatencyInfo getLongLatencyInfo() {
        return longLatencyInfo;
    }

    public void setLongLatencyInfo(LongLatencyInfo longLatencyInfo) {
        this.longLatencyInfo = longLatencyInfo;
    }
}
