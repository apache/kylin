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

package org.apache.kylin.stream.core.storage;

import java.util.Map;

import org.apache.kylin.stream.core.model.stats.LongLatencyInfo;

public class CheckPoint {
    private String sourceConsumePosition;
    private Map<Long, String> persistedIndexes;
    private LongLatencyInfo longLatencyInfo;
    // Map<SegmentStartTime, String>
    private Map<Long, String> segmentSourceStartPosition;
    private long checkPointTime;
    private long totalCount;
    private long checkPointCount;

    public String getSourceConsumePosition() {
        return sourceConsumePosition;
    }

    public void setSourceConsumePosition(String sourceConsumePosition) {
        this.sourceConsumePosition = sourceConsumePosition;
    }

    public Map<Long, String> getPersistedIndexes() {
        return persistedIndexes;
    }

    public void setPersistedIndexes(Map<Long, String> persistedIndexes) {
        this.persistedIndexes = persistedIndexes;
    }

    public long getCheckPointTime() {
        return checkPointTime;
    }

    public void setCheckPointTime(long checkPointTime) {
        this.checkPointTime = checkPointTime;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getCheckPointCount() {
        return checkPointCount;
    }

    public void setCheckPointCount(long checkPointCount) {
        this.checkPointCount = checkPointCount;
    }

    public LongLatencyInfo getLongLatencyInfo() {
        return longLatencyInfo;
    }

    public void setLongLatencyInfo(LongLatencyInfo longLatencyInfo) {
        this.longLatencyInfo = longLatencyInfo;
    }

    public Map<Long, String> getSegmentSourceStartPosition() {
        return segmentSourceStartPosition;
    }

    public void setSegmentSourceStartPosition(Map<Long, String> segmentSourceStartPosition) {
        this.segmentSourceStartPosition = segmentSourceStartPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CheckPoint that = (CheckPoint) o;

        if (checkPointTime != that.checkPointTime) return false;
        if (totalCount != that.totalCount) return false;
        if (checkPointCount != that.checkPointCount) return false;
        if (sourceConsumePosition != null ? !sourceConsumePosition.equals(that.sourceConsumePosition) : that.sourceConsumePosition != null)
            return false;
        if (persistedIndexes != null ? !persistedIndexes.equals(that.persistedIndexes) : that.persistedIndexes != null)
            return false;
        if (longLatencyInfo != null ? !longLatencyInfo.equals(that.longLatencyInfo) : that.longLatencyInfo != null)
            return false;
        return segmentSourceStartPosition != null ? segmentSourceStartPosition.equals(that.segmentSourceStartPosition) : that.segmentSourceStartPosition == null;

    }

    @Override
    public int hashCode() {
        int result = sourceConsumePosition != null ? sourceConsumePosition.hashCode() : 0;
        result = 31 * result + (persistedIndexes != null ? persistedIndexes.hashCode() : 0);
        result = 31 * result + (longLatencyInfo != null ? longLatencyInfo.hashCode() : 0);
        result = 31 * result + (segmentSourceStartPosition != null ? segmentSourceStartPosition.hashCode() : 0);
        result = 31 * result + (int) (checkPointTime ^ (checkPointTime >>> 32));
        result = 31 * result + (int) (totalCount ^ (totalCount >>> 32));
        result = 31 * result + (int) (checkPointCount ^ (checkPointCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CheckPoint{" +
                "sourceConsumePosition='" + sourceConsumePosition + '\'' +
                ", persistedIndexes=" + persistedIndexes +
                ", longLatencyInfo=" + longLatencyInfo +
                ", segmentSourceStartPosition=" + segmentSourceStartPosition +
                ", checkPointTime=" + checkPointTime +
                ", totalCount=" + totalCount +
                ", checkPointCount=" + checkPointCount +
                '}';
    }
}
