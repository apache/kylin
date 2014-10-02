/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.tuple.Tuple;
import com.kylinolap.storage.tuple.TupleIterator;
import org.apache.hadoop.hbase.client.HConnection;

import java.util.*;

/**
 * @author xjiang
 */
public class SerializedHBaseTupleIterator implements TupleIterator {

    private static final int PARTIAL_DEFAULT_LIMIT = 10000;

    private final StorageContext context;
    private final int partialResultLimit;
    private final List<CubeSegmentTupleIterator> segmentIteratorList;
    private final Iterator<CubeSegmentTupleIterator> segmentIteratorIterator;

    private TupleIterator segmentIterator;
    private int scanCount;

    public SerializedHBaseTupleIterator(HConnection conn, List<HBaseKeyRange> segmentKeyRanges,
                                        CubeInstance cube, Collection<TblColRef> dimensionColumns, TupleFilter filter,
                                        Collection<TblColRef> groupByColumns, Collection<RowValueDecoder> rowValueDecoders,
                                        StorageContext context) {

        this.context = context;
        int limit = context.getLimit();
        this.partialResultLimit = Math.max(limit, PARTIAL_DEFAULT_LIMIT);

        this.segmentIteratorList = new ArrayList<CubeSegmentTupleIterator>(segmentKeyRanges.size());
        Map<CubeSegment, List<HBaseKeyRange>> rangesMap = makeRangesMap(segmentKeyRanges);
        for (CubeSegment cubeSeg : rangesMap.keySet()) {
            List<HBaseKeyRange> keyRanges = rangesMap.get(cubeSeg);
            CubeSegmentTupleIterator segIter =
                    new CubeSegmentTupleIterator(cubeSeg, keyRanges, conn, dimensionColumns, filter,
                            groupByColumns, rowValueDecoders, context);
            this.segmentIteratorList.add(segIter);
        }

        this.segmentIteratorIterator = this.segmentIteratorList.iterator();
        if (this.segmentIteratorIterator.hasNext()) {
            this.segmentIterator = this.segmentIteratorIterator.next();
        } else {
            this.segmentIterator = CubeSegmentTupleIterator.EMPTY_TUPLE_ITERATOR;
        }
    }

    private Map<CubeSegment, List<HBaseKeyRange>> makeRangesMap(List<HBaseKeyRange> segmentKeyRanges) {
        Map<CubeSegment, List<HBaseKeyRange>> map = Maps.newHashMap();
        for (HBaseKeyRange range : segmentKeyRanges) {
            List<HBaseKeyRange> list = map.get(range.getCubeSegment());
            if (list == null) {
                list = Lists.newArrayList();
                map.put(range.getCubeSegment(), list);
            }
            list.add(range);
        }
        return map;
    }

    @Override
    public boolean hasNext() {
        // 1. check limit 
        if (context.isLimitEnable() && scanCount >= context.getLimit()) {
            return false;
        }
        // 2. check partial result
        if (context.isAcceptPartialResult() && scanCount > partialResultLimit) {
            context.setPartialResultReturned(true);
            return false;
        }
        // 3. check threshold
        if (scanCount >= context.getThreshold()) {
            throw new ScanOutOfLimitException("Scan row count exceeded threshold: " + context.getThreshold()
                    + ", please add filter condition to narrow down backend scan range, like where clause.");
        }
        // 4. check cube segments 
        return segmentIteratorIterator.hasNext() || segmentIterator.hasNext();
    }

    @Override
    public Tuple next() {
        Tuple t = null;
        while (hasNext()) {
            if (segmentIterator.hasNext()) {
                t = segmentIterator.next();
                scanCount++;
                break;
            } else {
                segmentIterator.close();
                segmentIterator = segmentIteratorIterator.next();
            }
        }
        return t;
    }

    @Override
    public void close() {
        context.setTotalScanCount(scanCount);
        segmentIterator.close();
    }
}
