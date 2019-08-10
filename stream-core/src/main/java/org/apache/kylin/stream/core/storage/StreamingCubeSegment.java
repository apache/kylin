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

import java.io.File;
import java.io.IOException;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.stream.core.model.StreamingMessage;

public class StreamingCubeSegment implements Comparable<StreamingCubeSegment> {
    private volatile State state;
    private IStreamingSegmentStore segmentStore;
    private long createTime;
    private long lastUpdateTime;
    private long latestEventTimeStamp = 0;
    private long latestEventLatecy = 0;
    private String cubeName;
    private CubeInstance cubeInstance;
    private String segmentName;
    private long dateRangeStart;
    private long dateRangeEnd;

    public StreamingCubeSegment(CubeInstance cubeInstance, IStreamingSegmentStore segmentStore, long segmentStart,
            long segmentEnd) {
        this.cubeInstance = cubeInstance;

        this.cubeName = cubeInstance.getName();
        this.dateRangeStart = segmentStart;
        this.dateRangeEnd = segmentEnd;
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = System.currentTimeMillis();
        this.state = State.ACTIVE;
        this.segmentStore = segmentStore;
        this.segmentName = CubeSegment.makeSegmentName(new TSRange(segmentStart, segmentEnd), null, cubeInstance.getModel());
    }

    public static StreamingCubeSegment parseSegment(CubeInstance cubeInstance, File segmentFolder,
            IStreamingSegmentStore segmentStore) {
        Pair<Long, Long> segmentStartEnd = CubeSegment.parseSegmentName(segmentFolder.getName());
        StreamingCubeSegment segment = new StreamingCubeSegment(cubeInstance, segmentStore, segmentStartEnd.getFirst(),
                segmentStartEnd.getSecond());

        State state = segmentStore.getSegmentState();
        segment.saveState(state);
        return segment;
    }

    public IStreamingSegmentStore getSegmentStore() {
        return segmentStore;
    }

    public void immutable() {
        segmentStore.persist();
        saveState(State.IMMUTABLE);
    }

    public boolean isActive() {
        return State.ACTIVE == state;
    }

    public boolean isImmutable() {
        return State.IMMUTABLE == state || State.REMOTE_PERSISTED == state;
    }

    public boolean isPersistToRemote() {
        return State.REMOTE_PERSISTED == state;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getLatestEventTimeStamp() {
        return latestEventTimeStamp;
    }

    public long getLatestEventLatecy() {
        return latestEventLatecy;
    }

    public long getDateRangeStart() {
        return dateRangeStart;
    }

    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    public boolean isLongLatencySegment() {
        return dateRangeStart == 0;
    }

    public Pair<Long, Long> getSegmentRange() {
        return new Pair<>(dateRangeStart, dateRangeEnd);
    }

    public File getDataSegmentFolder() {
        return segmentStore.getStorePath();
    }

    public CubeInstance getCubeInstance() {
        return cubeInstance;
    }

    public String getCubeName() {
        return cubeName;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public State getState() {
        return state;
    }

    public void saveState(State state) {
        this.segmentStore.setSegmentState(state);
        this.state = state;
    }

    public void addEvent(StreamingMessage event) {
        segmentStore.addEvent(event);
        latestEventTimeStamp = event.getTimestamp();
        latestEventLatecy = System.currentTimeMillis() - event.getTimestamp();
    }

    public void purge() {
        segmentStore.purge();
    }

    public void close() throws IOException {
        segmentStore.close();
    }

    @Override
    public int compareTo(StreamingCubeSegment o) {
        if (!this.getCubeName().equals(o.getCubeName())) {
            return this.getCubeName().compareTo(o.getCubeName());
        }

        return Long.compare(getDateRangeStart(), o.getDateRangeStart());
    }

    @Override
    public String toString() {
        return "StreamingCubeSegment [cubeName=" + cubeName + ", segmentName=" + segmentName + "]";
    }

    public enum State {
        ACTIVE, IMMUTABLE, REMOTE_PERSISTED
    }
}
