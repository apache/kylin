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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeOrder;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentPruner {
    private static final Logger logger = LoggerFactory.getLogger(SegmentPruner.class);

    final private Set<CompareTupleFilter> mustTrueCompares;

    public SegmentPruner(TupleFilter filter) {
        this.mustTrueCompares = filter == null ? Collections.<CompareTupleFilter> emptySet()
                : filter.findMustTrueCompareFilters();
    }

    public List<CubeSegment> listSegmentsForQuery(CubeInstance cube) {
        List<CubeSegment> r = new ArrayList<>();
        for (CubeSegment seg : cube.getSegments(SegmentStatusEnum.READY)) {
            if (check(seg))
                r.add(seg);
        }
        return r;
    }
    
    public boolean check(CubeSegment seg) {
        
        if (seg.getInputRecords() == 0) {
            if (seg.getConfig().isSkippingEmptySegments()) {
                logger.debug("Prune segment {} due to 0 input record", seg);
                return false;
            } else {
                logger.debug("Insist scan of segment {} having 0 input record", seg);
            }
        }

        Map<String, DimensionRangeInfo> segDimRangInfoMap = seg.getDimensionRangeInfoMap();
        for (CompareTupleFilter comp : mustTrueCompares) {
            TblColRef col = comp.getColumn();
            
            DimensionRangeInfo dimRangeInfo = segDimRangInfoMap.get(col.getIdentity());
            if (dimRangeInfo == null)
                dimRangeInfo = tryDeduceRangeFromPartitionCol(seg, col);
            if (dimRangeInfo == null)
                continue;
            
            String minVal = dimRangeInfo.getMin();
            String maxVal = dimRangeInfo.getMax();
            
            if (!satisfy(comp, minVal, maxVal)) {
                logger.debug("Prune segment {} due to given filter", seg);
                return false;
            }
        }

        logger.debug("Pruner passed on segment {}", seg);
        return true;
    }

    private DimensionRangeInfo tryDeduceRangeFromPartitionCol(CubeSegment seg, TblColRef col) {
        DataModelDesc model = seg.getModel();
        PartitionDesc part = model.getPartitionDesc();
        
        if (!part.isPartitioned())
            return null;
        if (!col.equals(part.getPartitionDateColumnRef()))
            return null;
        
        // deduce the dim range from TSRange
        TSRange tsRange = seg.getTSRange();
        if (tsRange.start.isMin || tsRange.end.isMax)
            return null; // DimensionRangeInfo cannot express infinite
        
        String min = tsRangeToStr(tsRange.start.v, part);
        String max = tsRangeToStr(tsRange.end.v - 1, part); // note the -1, end side is exclusive
        return new DimensionRangeInfo(min, max);
    }

    private String tsRangeToStr(long ts, PartitionDesc part) {
        String value;
        DataType partitionColType = part.getPartitionDateColumnRef().getType();
        if (partitionColType.isDate()) {
            value = DateFormat.formatToDateStr(ts);
        } else if (partitionColType.isTimeFamily()) {
            value = DateFormat.formatToTimeWithoutMilliStr(ts);
        } else if (partitionColType.isStringFamily() || partitionColType.isIntegerFamily()) {//integer like 20160101
            String partitionDateFormat = part.getPartitionDateFormat();
            if (StringUtils.isEmpty(partitionDateFormat)) {
                value = "" + ts;
            } else {
                value = DateFormat.formatToDateStr(ts, partitionDateFormat);
            }
        } else {
            throw new RuntimeException("Type " + partitionColType + " is not valid partition column type");
        }
        return value;
    }

    private boolean satisfy(CompareTupleFilter comp, String minVal, String maxVal) {

        // When both min and max are null, it means all cells of the column are null.
        // In such case, return true to let query engine scan the segment, since the
        // result of null comparison is query engine specific.
        if (minVal == null && maxVal == null)
            return true;

        TblColRef col = comp.getColumn();
        DataTypeOrder order = col.getType().getOrder();
        String filterVal = toString(comp.getFirstValue());
        
        switch (comp.getOperator()) {
        case EQ:
        case IN:
            String filterMin = order.min((Set<String>) comp.getValues());
            String filterMax = order.max((Set<String>) comp.getValues());
            return order.compare(filterMin, maxVal) <= 0 && order.compare(minVal, filterMax) <= 0;
        case LT:
            return order.compare(minVal, filterVal) < 0;
        case LTE:
            return order.compare(minVal, filterVal) <= 0;
        case GT:
            return order.compare(maxVal, filterVal) > 0;
        case GTE:
            return order.compare(maxVal, filterVal) >= 0;
        case NEQ:
        case NOTIN:
        case ISNULL:
        case ISNOTNULL:
        default:
            return true;
        }
    }

    private String toString(Object v) {
        return v == null ? null : v.toString();
    }
}
