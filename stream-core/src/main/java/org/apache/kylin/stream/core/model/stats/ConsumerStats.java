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
public class ConsumerStats {
    @JsonProperty("total_incoming_events")
    private long totalIncomingEvents;

    @JsonProperty("total_exception_events")
    private long totalExceptionEvents;

    @JsonProperty("partition_consume_stats")
    private Map<Integer, PartitionConsumeStats> partitionConsumeStatsMap = Maps.newHashMap();

    @JsonProperty("paused")
    private boolean paused;

    @JsonProperty("stopped")
    private boolean stopped;

    @JsonProperty("consume_offset_info")
    private String consumeOffsetInfo;

    @JsonProperty("consume_lag")
    private long consumeLag;

    public Map<Integer, PartitionConsumeStats> getPartitionConsumeStatsMap() {
        return partitionConsumeStatsMap;
    }

    public void setPartitionConsumeStatsMap(Map<Integer, PartitionConsumeStats> partitionConsumeStatsMap) {
        this.partitionConsumeStatsMap = partitionConsumeStatsMap;
    }

    public String getConsumeOffsetInfo() {
        return consumeOffsetInfo;
    }

    public void setConsumeOffsetInfo(String consumeOffsetInfo) {
        this.consumeOffsetInfo = consumeOffsetInfo;
    }

    public long getTotalIncomingEvents() {
        return totalIncomingEvents;
    }

    public void setTotalIncomingEvents(long totalIncomingEvents) {
        this.totalIncomingEvents = totalIncomingEvents;
    }

    public long getTotalExceptionEvents() {
        return totalExceptionEvents;
    }

    public void setTotalExceptionEvents(long totalExceptionEvents) {
        this.totalExceptionEvents = totalExceptionEvents;
    }

    public boolean isPaused() {
        return paused;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    public boolean isStopped() {
        return stopped;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public long getConsumeLag() {
        return consumeLag;
    }

    public void setConsumeLag(long consumeLag) {
        this.consumeLag = consumeLag;
    }
}
