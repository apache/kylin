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

package org.apache.kylin.metadata.model;

import java.util.ArrayList;

public class Segments<T extends ISegment> extends ArrayList<T> {
    
    private static final long serialVersionUID = 1L;

    public static boolean sourceOffsetContains(ISegment a, ISegment b) {
        return a.getSourceOffsetStart() <= b.getSourceOffsetStart() && b.getSourceOffsetEnd() <= a.getSourceOffsetEnd();
    }
    
    public static boolean sourceOffsetOverlaps(ISegment a, ISegment b) {
        return a.getSourceOffsetStart() < b.getSourceOffsetEnd() && b.getSourceOffsetStart() < a.getSourceOffsetEnd();
    }

    public T getFirstSegment() {
        if (this == null || this.size() == 0) {
            return null;
        } else {
            return this.get(0);
        }
    }

    public long getDateRangeStart() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long startTime = Long.MAX_VALUE;
        for (T seg : readySegs) {
            startTime = Math.min(startTime, seg.getDateRangeStart());
        }

        return startTime;
    }

    public long getDateRangeEnd() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = Long.MIN_VALUE;
        for (T seg : readySegs) {
            endTime = Math.max(endTime, seg.getDateRangeEnd());
        }

        return endTime;
    }

    public T getLatestReadySegment() {
        T latest = null;
        for (int i = this.size() - 1; i >= 0; i--) {
            T seg = this.get(i);
            if (seg.getStatus() != SegmentStatusEnum.READY)
                continue;
            if (latest == null || latest.getDateRangeEnd() < seg.getDateRangeEnd()) {
                latest = seg;
            }
        }
        return latest;
    }

    public T getLatestBuiltSegment() {
        T latest = null;
        for (int i = this.size() - 1; i >= 0; i--) {
            T seg = this.get(i);
            if (seg.getLastBuildTime() > 0) {
                if (latest == null || seg.getLastBuildTime() > latest.getLastBuildTime())
                    latest = seg;
            }
        }
        return latest;
    }

    public Segments getSegments(SegmentStatusEnum status) {
        Segments<T> result = new Segments<>();

        for (T segment : this) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }
        return result;
    }

    public T getSegment(String name, SegmentStatusEnum status) {
        for (T segment : this) {
            if ((null != segment.getName() && segment.getName().equals(name)) && (status == null || segment.getStatus() == status)) {
                return segment;
            }
        }
        return null;
    }

    public Segments getBuildingSegments() {
        Segments<T> buildingSegments = new Segments();
        if (null != this) {
            for (T segment : this) {
                if (SegmentStatusEnum.NEW == segment.getStatus() || SegmentStatusEnum.READY_PENDING == segment.getStatus()) {
                    buildingSegments.add(segment);
                }
            }
        }
        return buildingSegments;
    }

    public Segments getMergingSegments(T mergedSegment) {
        Segments<T> result = new Segments();
        if (mergedSegment == null)
            return result;

        for (T seg : this) {
            if (seg.getStatus() != SegmentStatusEnum.READY && seg.getStatus() != SegmentStatusEnum.READY_PENDING)
                continue;

            if (seg == mergedSegment)
                continue;

            if (sourceOffsetContains(mergedSegment, seg)) {
                // make sure no holes
                if (result.size() > 0 && result.getLast().getSourceOffsetEnd() != seg.getSourceOffsetStart())
                    throw new IllegalStateException("Merging segments must not have holes between " + result.getLast() + " and " + seg);

                result.add(seg);
            }
        }
        return result;
    }

    private T getLast() {
        assert this.size() != 0;
        return this.get(this.size() - 1);
    }

}