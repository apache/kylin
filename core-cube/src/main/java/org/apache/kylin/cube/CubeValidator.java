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

package org.apache.kylin.cube;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class CubeValidator {
    private static final Logger logger = LoggerFactory.getLogger(CubeValidator.class);

    /**
     * Validates:
     * - consistent isOffsetsOn()
     * - for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
     * - for all new segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
     * - for all new segments, sourceOffset SHOULD fit/connect another segments
     * - dateRange does not matter any more
     */
    public static void validate(Collection<CubeSegment> segments) {
        if (segments == null || segments.isEmpty())
            return;

        // make a copy, don't modify given list
        List<CubeSegment> all = Lists.newArrayList(segments);
        Collections.sort(all);

        // check consistent isOffsetsOn()
        boolean isOffsetsOn = all.get(0).isSourceOffsetsOn();
        for (CubeSegment seg : all) {
            seg.validate();
            if (seg.isSourceOffsetsOn() != isOffsetsOn)
                throw new IllegalStateException("Inconsistent isOffsetsOn for segment " + seg);
        }

        List<CubeSegment> ready = Lists.newArrayListWithCapacity(all.size());
        List<CubeSegment> news = Lists.newArrayListWithCapacity(all.size());
        for (CubeSegment seg : all) {
            if (seg.getStatus() == SegmentStatusEnum.READY)
                ready.add(seg);
            else
                news.add(seg);
        }

        // for all ready segments, sourceOffset MUST have no overlaps, SHOULD have no holes
        CubeSegment pre = null;
        for (CubeSegment seg : ready) {
            if (pre != null) {
                if (pre.sourceOffsetOverlaps(seg))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
                if (pre.getSourceOffsetEnd() < seg.getSourceOffsetStart())
                    logger.warn("Hole between adjacent READY segments " + pre + " and " + seg);
            }
            pre = seg;
        }

        // for all other segments, sourceOffset MUST have no overlaps, MUST contain a ready segment if overlaps with it
        pre = null;
        for (CubeSegment seg : news) {
            if (pre != null) {
                if (pre.sourceOffsetOverlaps(seg))
                    throw new IllegalStateException("Segments overlap: " + pre + " and " + seg);
            }
            pre = seg;

            for (CubeSegment aReady : ready) {
                if (seg.sourceOffsetOverlaps(aReady) && !seg.sourceOffsetContains(aReady))
                    throw new IllegalStateException("Segments overlap: " + aReady + " and " + seg);
            }
        }

        // for all other segments, sourceOffset SHOULD fit/connect other segments
        for (CubeSegment seg : news) {
            Pair<Boolean, Boolean> pair = fitInSegments(all, seg);
            boolean startFit = pair.getFirst();
            boolean endFit = pair.getSecond();

            if (!startFit)
                logger.warn("NEW segment start does not fit/connect with other segments: " + seg);
            if (!endFit)
                logger.warn("NEW segment end does not fit/connect with other segments: " + seg);
        }
    }

    public static Pair<Boolean, Boolean> fitInSegments(List<CubeSegment> segments, CubeSegment newOne) {
        if (segments == null || segments.isEmpty())
            return null;

        CubeSegment first = segments.get(0);
        CubeSegment last = segments.get(segments.size() - 1);
        long start = newOne.getSourceOffsetStart();
        long end = newOne.getSourceOffsetEnd();
        boolean startFit = false;
        boolean endFit = false;
        for (CubeSegment sss : segments) {
            if (sss == newOne)
                continue;
            startFit = startFit || (start == sss.getSourceOffsetStart() || start == sss.getSourceOffsetEnd());
            endFit = endFit || (end == sss.getSourceOffsetStart() || end == sss.getSourceOffsetEnd());
        }
        if (!startFit && endFit && newOne == first)
            startFit = true;
        if (!endFit && startFit && newOne == last)
            endFit = true;

        return Pair.newPair(startFit, endFit);
    }

}
