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

import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.ISegmentAdvisor;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;

public class CubeSegmentAdvisor implements ISegmentAdvisor {

    protected final CubeSegment seg;

    // these are just cache of segment attributes, all changes must write through to 'seg'
    protected TSRange tsRange;
    protected SegmentRange segRange;

    public CubeSegmentAdvisor(ISegment segment) {
        this.seg = (CubeSegment) segment;
    }
    
    @Override
    public boolean isOffsetCube() {
        return seg._getSourceOffsetStart() != 0 || seg._getSourceOffsetEnd() != 0;
    }

    @Override
    public SegmentRange getSegRange() {
        if (segRange != null)
            return segRange;

        // backward compatible with pre-streaming metadata, TSRange can imply SegmentRange
        segRange = isOffsetCube() //
                ? new SegmentRange(seg._getSourceOffsetStart(), seg._getSourceOffsetEnd()) //
                : getTSRange();

        return segRange;
    }

    @Override
    public void setSegRange(SegmentRange range) {
        // backward compatible with pre-streaming metadata, TSRange can imply SegmentRange
        if (range == null) {
            seg._setSourceOffsetStart(0);
            seg._setSourceOffsetEnd(0);
        } else {
            seg._setSourceOffsetStart((Long) range.start.v);
            seg._setSourceOffsetEnd((Long) range.end.v);
        }
        clear();
    }

    @Override
    public TSRange getTSRange() {
        if (tsRange != null)
            return tsRange;
        
        tsRange = new TSRange(seg._getDateRangeStart(), seg._getDateRangeEnd());
        return tsRange;
    }

    @Override
    public void setTSRange(TSRange range) {
        if (range == null) {
            seg._setDateRangeStart(0);
            seg._setDateRangeEnd(0);
        } else {
            seg._setDateRangeStart(range.start.v);
            seg._setDateRangeEnd(range.end.v);
        }
        clear();
    }

    private void clear() {
        tsRange = null;
        segRange = null;
    }

}
