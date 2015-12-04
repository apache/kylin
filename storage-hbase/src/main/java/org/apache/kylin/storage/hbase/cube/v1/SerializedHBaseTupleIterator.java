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

package org.apache.kylin.storage.hbase.cube.v1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.exception.ScanOutOfLimitException;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;
import org.apache.kylin.storage.translate.HBaseKeyRange;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author xjiang
 */
public class SerializedHBaseTupleIterator implements ITupleIterator {

    private static final int PARTIAL_DEFAULT_LIMIT = 10000;

    private final StorageContext context;
    private final int partialResultLimit;
    private final List<CubeSegmentTupleIterator> segmentIteratorList;
    private final Iterator<CubeSegmentTupleIterator> segmentIteratorIterator;

    private ITupleIterator segmentIterator;
    private int scanCount;
    private ITuple next;

    public SerializedHBaseTupleIterator(HConnection conn, List<HBaseKeyRange> segmentKeyRanges, CubeInstance cube, //
            Set<TblColRef> dimensions, TupleFilter filter, Set<TblColRef> groupBy, List<RowValueDecoder> rowValueDecoders, //
            StorageContext context, TupleInfo returnTupleInfo) {

        this.context = context;
        int limit = context.getLimit();
        this.partialResultLimit = Math.max(limit, PARTIAL_DEFAULT_LIMIT);

        this.segmentIteratorList = new ArrayList<CubeSegmentTupleIterator>(segmentKeyRanges.size());
        Map<CubeSegment, List<HBaseKeyRange>> rangesMap = makeRangesMap(segmentKeyRanges);

        for (Map.Entry<CubeSegment, List<HBaseKeyRange>> entry : rangesMap.entrySet()) {
            CubeSegmentTupleIterator it = new CubeSegmentTupleIterator(entry.getKey(), entry.getValue(), conn, dimensions, filter, groupBy, rowValueDecoders, context, returnTupleInfo);
            this.segmentIteratorList.add(it);
        }

        this.segmentIteratorIterator = this.segmentIteratorList.iterator();
        if (this.segmentIteratorIterator.hasNext()) {
            this.segmentIterator = this.segmentIteratorIterator.next();
        } else {
            this.segmentIterator = ITupleIterator.EMPTY_TUPLE_ITERATOR;
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
        if (next != null)
            return true;

        // 1. check limit
        if (context.isLimitEnabled() && scanCount >= context.getLimit() + context.getOffset()) {
            return false;
        }
        // 2. check partial result
        if (context.isAcceptPartialResult() && scanCount > partialResultLimit) {
            context.setPartialResultReturned(true);
            return false;
        }
        // 3. check threshold
        if (scanCount >= context.getThreshold()) {
            throw new ScanOutOfLimitException("Scan row count exceeded threshold: " + context.getThreshold() + ", please add filter condition to narrow down backend scan range, like where clause.");
        }
        // 4. check cube segments
        if (segmentIterator.hasNext()) {
            next = segmentIterator.next();
            scanCount++;
            return true;
        } else if (segmentIteratorIterator.hasNext()) {
            segmentIterator.close();
            segmentIterator = segmentIteratorIterator.next();
            return hasNext();
        }
        return false;
    }

    @Override
    public ITuple next() {
        if (next == null) {
            hasNext();
            if (next == null)
                throw new NoSuchElementException();
        }
        ITuple r = next;
        next = null;
        return r;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // hasNext() loop may exit because of limit, threshold, etc.
        // close all the remaining segmentIterator
        segmentIterator.close();
        while (segmentIteratorIterator.hasNext()) {
            segmentIterator = segmentIteratorIterator.next();
            segmentIterator.close();
        }
    }

}
