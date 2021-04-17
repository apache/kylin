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
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.SegmentRange.Endpoint;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class Segments<T extends ISegment> extends ArrayList<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(Segments.class);

    public static ISegmentAdvisor newSegmentAdvisor(ISegment seg) {
        try {
            Class<? extends ISegmentAdvisor> clz = ClassUtil.forName(seg.getConfig().getSegmentAdvisor(),
                    ISegmentAdvisor.class);
            return clz.getConstructor(ISegment.class).newInstance(seg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================================

    public Segments() {
        super();
    }

    public Segments(List<T> copy) {
        super(copy);
    }

    public T getFirstSegment() {
        if (this == null || this.size() == 0) {
            return null;
        } else {
            return this.get(0);
        }
    }

    public long getTSStart() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long startTime = Long.MAX_VALUE;
        for (ISegment seg : readySegs) {
            startTime = Math.min(startTime, seg.getTSRange().start.v);
        }

        return startTime;
    }

    public long getTSEnd() {
        Segments<T> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = Long.MIN_VALUE;
        for (ISegment seg : readySegs) {
            endTime = Math.max(endTime, seg.getTSRange().end.v);
        }

        return endTime;
    }

    public T getLatestReadySegment() {
        T latest = null;
        for (int i = this.size() - 1; i >= 0; i--) {
            T seg = this.get(i);
            if (seg.getStatus() != SegmentStatusEnum.READY)
                continue;
            if (latest == null || latest.getTSRange().end.v < seg.getTSRange().end.v) {
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
            if ((null != segment.getName() && segment.getName().equals(name))
                    && (status == null || segment.getStatus() == status)) {
                return segment;
            }
        }
        return null;
    }

    public Segments<T> getBuildingSegments() {
        Segments<T> buildingSegments = new Segments();
        if (null != this) {
            for (T segment : this) {
                if (SegmentStatusEnum.NEW == segment.getStatus()
                        || SegmentStatusEnum.READY_PENDING == segment.getStatus()) {
                    buildingSegments.add(segment);
                }
            }
        }
        return buildingSegments;
    }

    public Segments<T> getMergingSegments(T mergedSegment) {
        Segments<T> result = new Segments();
        if (mergedSegment == null)
            return result;

        long maxSegMergeSpan = KylinConfig.getInstanceFromEnv().getMaxSegmentMergeSpan();
        for (T seg : this) {
            if (seg.getStatus() != SegmentStatusEnum.READY && seg.getStatus() != SegmentStatusEnum.READY_PENDING)
                continue;

            if (seg == mergedSegment)
                continue;

            if (maxSegMergeSpan > 0 && seg.getTSRange().duration() > maxSegMergeSpan) {
                continue;
            }

            if (mergedSegment.getSegRange().contains(seg.getSegRange())) {
                result.add(seg);
            }
        }
        return result;
    }

    public void removeLatestSegmentByVolatileRange(Segments<T> segs, long volatileRange) {
        if(volatileRange <= 0) {
            return;
        }
        long latestSegEndTs = Long.MIN_VALUE;
        for(T seg: segs) {
            latestSegEndTs = Math.max(latestSegEndTs, seg.getTSRange().end.v);
        }
        Segments volatileSegs = new Segments();
        for(T seg: segs) {
            if (seg.getTSRange().end.v + volatileRange > latestSegEndTs) {
                logger.warn("Segment in volatile range, seg: {}, rangeStart:{}, rangeEnd {}.", seg,
                        seg.getTSRange().start.v, seg.getTSRange().end.v);
                volatileSegs.add(seg);
            }
        }
        segs.removeAll(volatileSegs);
    }

    public void removeMaxSpanSegment(Segments<T> segs, long maxSegSpan) {
        if (maxSegSpan <= 0) {
            return;
        }
        Segments maxSpanSegs = new Segments();
        for (T seg : segs) {
            if (seg.getTSRange().duration() >= maxSegSpan) {

                logger.warn("segment with max span: seg:" + seg.toString() +
                        "rangeStart:" + seg.getTSRange().start.v + ", rangeEnd" + seg.getTSRange().end.v);
                maxSpanSegs.add(seg);
            }
        }
        segs.removeAll(maxSpanSegs);
    }

    public SegmentRange autoMergeCubeSegments(boolean needAutoMerge, String cubeName, long[] timeRanges, long volatileRange) throws IOException {
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
                    if (building.getSegRange().contains(ready.getSegRange())) {
                        mergingSegs.add(ready);
                    }
                }
            }
        }
        removeLatestSegmentByVolatileRange(readySegs, volatileRange);
        removeMaxSpanSegment(readySegs, KylinConfig.getInstanceFromEnv().getMaxSegmentMergeSpan());
        // exclude those already under merging segments
        readySegs.removeAll(mergingSegs);

        Arrays.parallelSort(timeRanges);

        for (int i = timeRanges.length - 1; i >= 0; i--) {
            long toMergeRange = timeRanges[i];

            for (int s = 0; s < readySegs.size(); s++) {
                ISegment seg = readySegs.get(s);
                TSRange tsRange = new TSRange(seg.getTSRange().start.v, seg.getTSRange().start.v + toMergeRange);
                Pair<T, T> p = readySegs.getSubList(s, readySegs.size()) //
                        .findMergeOffsetsByDateRange(tsRange, toMergeRange);
                if (p != null && p.getSecond().getTSRange().end.v - p.getFirst().getTSRange().start.v >= toMergeRange)
                    return new SegmentRange(p.getFirst().getSegRange().start.v, p.getSecond().getSegRange().end.v);
            }
        }

        return null;
    }

    public Pair<T, T> findMergeOffsetsByDateRange(TSRange tsRange, long skipSegDateRangeCap) {
        // must be offset cube
        Segments result = new Segments();
        for (ISegment seg : this) {

            // include if date range overlaps
            if (tsRange.overlaps(seg.getTSRange())) {

                // reject too big segment
                if (seg.getTSRange().duration() > skipSegDateRangeCap)
                    break;

                // reject holes
                if (result.size() > 0 && !result.getLast().getSegRange().connects(seg.getSegRange()))
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

            if (is.getSegRange().start.compareTo(js.getSegRange().start) == 0) {
                // if i, j competes
                if (isReady(is) && isReady(js) || isNew(is) && isNew(js)) {
                    // if both new or ready, favor the bigger segment
                    if (is.getSegRange().end.compareTo(js.getSegRange().end) <= 0) {
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
            if (is.getSegRange().end.compareTo(js.getSegRange().start) <= 0) {
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

    /**
     * Validates:
     * - consistent isOffsetCube()
     * - for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
     * - for all new segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
     * - for all new segments, sourceOffset SHOULD fit/connect another segments
     * - dateRange does not matter any more
     */
    public void validate() {
        if (this.isEmpty())
            return;

        // make a copy, don't modify existing list
        Segments<T> all = new Segments<T>(this);
        Collections.sort(all);

        // check consistent isOffsetCube()
        boolean isOffsetCube = all.get(0).isOffsetCube();
        for (ISegment seg : all) {
            seg.validate();
            if (seg.isOffsetCube() != isOffsetCube)
                throw new IllegalStateException("Inconsistent isOffsetsOn for segment " + seg);
        }

        List<ISegment> ready = Lists.newArrayListWithCapacity(all.size());
        List<ISegment> news = Lists.newArrayListWithCapacity(all.size());
        for (ISegment seg : all) {
            if (seg.getStatus() == SegmentStatusEnum.READY)
                ready.add(seg);
            else
                news.add(seg);
        }

        // for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
        ISegment pre = null;
        for (ISegment seg : ready) {
            if (pre != null) {
                if (pre.getSegRange().overlaps(seg.getSegRange()))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
                if (pre.getSegRange().apartBefore(seg.getSegRange()))
                    logger.warn("Hole between adjacent READY segments " + pre + " and " + seg);
            }
            pre = seg;
        }

        // for all other segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
        pre = null;
        for (ISegment seg : news) {
            if (pre != null) {
                if (pre.getSegRange().overlaps(seg.getSegRange()))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
            }
            pre = seg;

            for (ISegment aReady : ready) {
                if (seg.getSegRange().overlaps(aReady.getSegRange())
                        && !seg.getSegRange().contains(aReady.getSegRange()))
                    throw new IllegalStateException("Segments overlap: " + aReady + " and " + seg);
            }
        }

        // for all other segments, sourceOffset SHOULD fit/connect other segments
        for (ISegment seg : news) {
            Pair<Boolean, Boolean> pair = all.fitInSegments(seg);
            boolean startFit = pair.getFirst();
            boolean endFit = pair.getSecond();

            if (!startFit)
                logger.warn("NEW segment start does not fit/connect with other segments: " + seg);
            if (!endFit)
                logger.warn("NEW segment end does not fit/connect with other segments: " + seg);
        }
    }

    public Pair<Boolean, Boolean> fitInSegments(ISegment newOne) {
        if (this.isEmpty()) {
          return Pair.newPair(false, false);        
        }

        ISegment first = this.get(0);
        ISegment last = this.get(this.size() - 1);
        Endpoint start = newOne.getSegRange().start;
        Endpoint end = newOne.getSegRange().end;
        boolean startFit = false;
        boolean endFit = false;
        for (ISegment sss : this) {
            if (sss == newOne)
                continue;
            startFit = startFit || (start.equals(sss.getSegRange().start) || start.equals(sss.getSegRange().end));
            endFit = endFit || (end.equals(sss.getSegRange().start) || end.equals(sss.getSegRange().end));
        }
        if (!startFit && endFit && newOne == first)
            startFit = true;
        if (!endFit && startFit && newOne == last)
            endFit = true;

        return Pair.newPair(startFit, endFit);
    }

    // given all segments in cube, checks whether specified segment is operative (not under processing)
    public boolean isOperative(ISegment seg) {
        if (seg.getStatus() != SegmentStatusEnum.READY)
            return false;

        for (ISegment other : this) {
            if (other == seg)
                continue;

            if (other.getSegRange().overlaps(seg.getSegRange()))
                return false;
        }
        return true;
    }
}
