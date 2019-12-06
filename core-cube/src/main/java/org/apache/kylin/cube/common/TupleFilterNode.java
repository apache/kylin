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

package org.apache.kylin.cube.common;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

public class TupleFilterNode {
    private TupleFilter filter;

    public TupleFilterNode(TupleFilter filter) {
        this.filter = filter;
    }

    public boolean checkSeg(CubeSegment seg) {
        if (filter == null)
            return true;
        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            TblColRef col = compareTupleFilter.getColumn();
            if (col == null) {
                return true;
            }
            DimensionRangeInfo dimRangeInfo = seg.getDimensionRangeInfoMap().get(col.getIdentity());
            if (dimRangeInfo == null)
                dimRangeInfo = SegmentPruner.tryDeduceRangeFromPartitionCol(seg, col);
            if (dimRangeInfo == null)
                return true;
            String minVal = dimRangeInfo.getMin();
            String maxVal = dimRangeInfo.getMax();
            return SegmentPruner.satisfy(compareTupleFilter, minVal, maxVal);
        }

        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                for (TupleFilter filter : filter.getChildren()) {
                    if (!new TupleFilterNode(filter).checkSeg(seg))
                        return false;
                }
                return true;

            } else if (filter.getOperator() == TupleFilter.FilterOperatorEnum.OR) {
                for (TupleFilter filter : filter.getChildren()) {
                    if (new TupleFilterNode(filter).checkSeg(seg))
                        return true;
                }
                return false;
            } else if (filter.getOperator() == TupleFilter.FilterOperatorEnum.NOT) {
                for (TupleFilter filter : filter.getChildren()) {
                    return !(new TupleFilterNode(filter).checkSeg(seg));
                }
            }
        }
        return true;
    }
}
