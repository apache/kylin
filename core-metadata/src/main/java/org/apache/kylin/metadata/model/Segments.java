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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Segments<T extends ISegment> extends ArrayList<T> implements Serializable{

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(Segments.class);

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
        for (ISegment seg : readySegs) {
            startTime = Math.min(startTime, seg.getDateRangeStart());
        }

        return startTime;
    }

    public long getDateRangeEnd() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = Long.MIN_VALUE;
        for (ISegment seg : readySegs) {
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

    public Segments<T> getSegments(SegmentStatusEnum status) {
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

    public Segments<T> getBuildingSegments() {
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

    public Pair<Long, Long> autoMergeCubeSegments(boolean needAutoMerge, String cubeName, long[] timeRanges) throws IOException {
        if (!needAutoMerge) {
            logger.debug("Cube " + cubeName + " doesn't need auto merge");
            return null;
        }

        int buildingSize = getBuildingSegments().size();
        if (buildingSize > 0) {
            logger.debug("Cube " + cubeName + " has " + buildingSize + " building segments");
        }

        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        Segments mergingSegs = new Segments();
        if (buildingSize > 0) {

            for (ISegment building : getBuildingSegments()) {
                // exclude those under-merging segs
                for (ISegment ready : readySegs) {
                    if (ready.getSourceOffsetStart() >= building.getSourceOffsetStart() && ready.getSourceOffsetEnd() <= building.getSourceOffsetEnd()) {
                        mergingSegs.add(ready);
                    }
                }
            }
        }

        // exclude those already under merging segments
        readySegs.removeAll(mergingSegs);

        Arrays.sort(timeRanges);

        for (int i = timeRanges.length - 1; i >= 0; i--) {
            long toMergeRange = timeRanges[i];

            for (int s = 0; s < readySegs.size(); s++) {
                ISegment seg = readySegs.get(s);
                Pair<T, T> p = readySegs.getSubList(s, readySegs.size()) //
                        .findMergeOffsetsByDateRange(seg.getDateRangeStart(), seg.getDateRangeStart() + toMergeRange, toMergeRange);
                if (p != null && p.getSecond().getDateRangeEnd() - p.getFirst().getDateRangeStart() >= toMergeRange)
                    return Pair.newPair(p.getFirst().getSourceOffsetStart(), p.getSecond().getSourceOffsetEnd());
            }
        }

        return null;
    }

    public Pair<T, T> findMergeOffsetsByDateRange(long startDate, long endDate, long skipSegDateRangeCap) {
        // must be offset cube
        Segments result = new Segments();
        for (ISegment seg : this) {

            // include if date range overlaps
            if (startDate < seg.getDateRangeEnd() && seg.getDateRangeStart() < endDate) {

                // reject too big segment
                if (seg.getDateRangeEnd() - seg.getDateRangeStart() > skipSegDateRangeCap)
                    break;

                // reject holes
                if (result.size() > 0 && result.getLast().getSourceOffsetEnd() != seg.getSourceOffsetStart())
                    break;

                result.add(seg);
            }
        }

        if (result.size() <= 1)
            return null;
        else
            return (Pair<T, T>) Pair.newPair(result.getFirst(), result.getLast());
    }

    /**
     * Smartly figure out the TOBE segments once all new segments are built.
     * - Ensures no gap, no overlap
     * - Favors new segments over the old
     * - Favors big segments over the small
     */
    public Segments calculateToBeSegments(ISegment newSegment) {

        Segments tobe = (Segments) this.clone();
        if (newSegment != null && !tobe.contains(newSegment)) {
            tobe.add(newSegment);
        }
        if (tobe.size() == 0)
            return tobe;

        // sort by source offset
        Collections.sort(tobe);

        ISegment firstSeg = tobe.getFirst();
        firstSeg.validate();

        for (int i = 0, j = 1; j < tobe.size();) {
            ISegment is = (ISegment) tobe.get(i);
            ISegment js = (ISegment) tobe.get(j);
            js.validate();

            // check i is either ready or new
            if (!isNew(is) && !isReady(is)) {
                tobe.remove(i);
                continue;
            }

            // check j is either ready or new
            if (!isNew(js) && !isReady(js)) {
                tobe.remove(j);
                continue;
            }

            if (is.getSourceOffsetStart() == js.getSourceOffsetStart()) {
                // if i, j competes
                if (isReady(is) && isReady(js) || isNew(is) && isNew(js)) {
                    // if both new or ready, favor the bigger segment
                    if (is.getSourceOffsetEnd() <= js.getSourceOffsetEnd()) {
                        tobe.remove(i);
                    } else {
                        tobe.remove(j);
                    }
                    continue;
                } else {
                    // otherwise, favor the new segment
                    if (isNew(is) && is.equals(newSegment)) {
                        tobe.remove(j);
                        continue;
                    } else if (js.equals(newSegment)) {
                        tobe.remove(i);
                        continue;
                    }
                }
            }

            // if i, j in sequence
            if (is.getSourceOffsetEnd() <= js.getSourceOffsetStart()) {
                i++;
                j++;
                continue;
            }

            // js can be covered by is
            if (is.equals(newSegment)) {
                // seems j not fitting
                tobe.remove(j);
                continue;
            } else {
                i++;
                j++;
                continue;
            }

        }

        return tobe;
    }

    private boolean isReady(ISegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
    }

    private boolean isNew(ISegment seg) {
        return seg.getStatus() == SegmentStatusEnum.NEW || seg.getStatus() == SegmentStatusEnum.READY_PENDING;
    }

    private T getLast() {
        assert this.size() != 0;
        return this.get(this.size() - 1);
    }

    private T getFirst() {
        assert this.size() != 0;
        return this.get(0);
    }

    private Segments<T> getSubList(int from, int to) {
        Segments<T> result = new Segments<>();
        for (T seg : this.subList(from, to)) {
            result.add(seg);
        }
        return result;
    }

}