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
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class LongLatencyInfo {
    @JsonProperty("long_latency_segment_events")
    private Map<String, Integer> longLatencyEventCnts = new TreeMap<>();
    @JsonProperty("total_long_latency_events")
    private int totalLongLatencyEventCnt;

    public Map<String, Integer> getLongLatencyEventCnts() {
        return longLatencyEventCnts;
    }

    public void setLongLatencyEventCnts(Map<String, Integer> longLatencyEventCnts) {
        this.longLatencyEventCnts = longLatencyEventCnts;
    }

    public int getTotalLongLatencyEventCnt() {
        return totalLongLatencyEventCnt;
    }

    public void setTotalLongLatencyEventCnt(int totalLongLatencyEventCnt) {
        this.totalLongLatencyEventCnt = totalLongLatencyEventCnt;
    }

    public void incLongLatencyEvent(String segmentName) {
        Integer llEventCnt = longLatencyEventCnts.get(segmentName);
        if (llEventCnt == null) {
            llEventCnt = 1;
        } else {
            llEventCnt = llEventCnt + 1;
        }
        longLatencyEventCnts.put(segmentName, llEventCnt);
        totalLongLatencyEventCnt++;
    }

    public LongLatencyInfo truncate(int maxSegments) {
        int segmentNum = longLatencyEventCnts.size();
        if (segmentNum <= maxSegments) {
            return this;
        } else {
            SortedMap<String, Integer> sortMap = new TreeMap<>(longLatencyEventCnts);
            int shouldRemoved = maxSegments - segmentNum;
            for (String segmentName : sortMap.keySet()) {
                if (shouldRemoved == 0) {
                    break;
                }
                longLatencyEventCnts.remove(segmentName);
                shouldRemoved--;
            }
        }
        return this;
    }

    @Override
    public String toString() {
        return "LongLatencyInfo{" + "longLatencyEventCnts=" + longLatencyEventCnts + ", totalLongLatencyEventCnt="
                + totalLongLatencyEventCnt + '}';
    }
}
