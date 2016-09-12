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

package org.apache.kylin.storage.gtrecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class SequentialCubeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(SequentialCubeTupleIterator.class);

    protected List<CubeSegmentScanner> scanners;
    protected List<SegmentCubeTupleIterator> segmentCubeTupleIterators;
    protected Iterator<ITuple> tupleIterator;
    protected final int storagePushDownLimit;
    protected StorageContext context;

    private int scanCount;
    private int scanCountDelta;

    public SequentialCubeTupleIterator(List<CubeSegmentScanner> scanners, Cuboid cuboid, Set<TblColRef> selectedDimensions, //
            Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context) {
        this.context = context;
        this.scanners = scanners;

        segmentCubeTupleIterators = Lists.newArrayList();
        for (CubeSegmentScanner scanner : scanners) {
            segmentCubeTupleIterators.add(new SegmentCubeTupleIterator(scanner, cuboid, selectedDimensions, selectedMetrics, returnTupleInfo, context));
        }

        this.storagePushDownLimit = context.getFinalPushDownLimit();
        if (storagePushDownLimit == Integer.MAX_VALUE) {
            //normal case
            tupleIterator = Iterators.concat(segmentCubeTupleIterators.iterator());
        } else {
            //query with limit
            Iterator<Iterator<ITuple>> transformed = Iterators.transform(segmentCubeTupleIterators.iterator(), new Function<SegmentCubeTupleIterator, Iterator<ITuple>>() {
                @Nullable
                @Override
                public Iterator<ITuple> apply(@Nullable SegmentCubeTupleIterator input) {
                    return input;
                }
            });
            tupleIterator = new SortedIteratorMergerWithLimit<ITuple>(transformed, storagePushDownLimit, segmentCubeTupleIterators.get(0).getCubeTupleConverter().getTupleDimensionComparator()).getIterator();
        }
    }

    @Override
    public boolean hasNext() {
        return tupleIterator.hasNext();
    }

    @Override
    public ITuple next() {
        scanCount++;
        if (++scanCountDelta >= 1000)
            flushScanCountDelta();

        return tupleIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // hasNext() loop may exit because of limit, threshold, etc.
        // close all the remaining segmentIterator
        flushScanCountDelta();

        for (SegmentCubeTupleIterator iterator : segmentCubeTupleIterators) {
            iterator.close();
        }
    }

    protected void close(CubeSegmentScanner scanner) {
        try {
            scanner.close();
        } catch (IOException e) {
            logger.error("Exception when close CubeScanner", e);
        }
    }

    public int getScanCount() {
        return scanCount;
    }

    private void flushScanCountDelta() {
        context.increaseTotalScanCount(scanCountDelta);
        scanCountDelta = 0;
    }

}
