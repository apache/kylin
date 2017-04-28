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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class SequentialCubeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(SequentialCubeTupleIterator.class);

    protected List<CubeSegmentScanner> scanners;
    protected List<SegmentCubeTupleIterator> segmentCubeTupleIterators;
    protected Iterator<ITuple> tupleIterator;
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

        if (context.mergeSortPartitionResults()) {
            //query with limit
            logger.info("Using SortedIteratorMergerWithLimit to merge segment results");
            Iterator<Iterator<ITuple>> transformed = (Iterator<Iterator<ITuple>>) (Iterator<?>) segmentCubeTupleIterators.iterator();
            tupleIterator = new SortedIteratorMergerWithLimit<ITuple>(transformed, context.getFinalPushDownLimit(), getTupleDimensionComparator(cuboid, returnTupleInfo)).getIterator();
        } else {
            //normal case
            logger.info("Using Iterators.concat to merge segment results");
            tupleIterator = Iterators.concat(segmentCubeTupleIterators.iterator());
        }
    }

    public Comparator<ITuple> getTupleDimensionComparator(Cuboid cuboid, TupleInfo returnTupleInfo) {
        // dimensionIndexOnTuple is for SQL with limit
        List<Integer> temp = Lists.newArrayList();
        for (TblColRef dim : cuboid.getColumns()) {
            if (returnTupleInfo.hasColumn(dim)) {
                temp.add(returnTupleInfo.getColumnIndex(dim));
            }
        }

        final int[] dimensionIndexOnTuple = new int[temp.size()];
        for (int i = 0; i < temp.size(); i++) {
            dimensionIndexOnTuple[i] = temp.get(i);
        }

        return new Comparator<ITuple>() {
            @Override
            public int compare(ITuple o1, ITuple o2) {
                Preconditions.checkNotNull(o1);
                Preconditions.checkNotNull(o2);
                for (int i = 0; i < dimensionIndexOnTuple.length; i++) {
                    int index = dimensionIndexOnTuple[i];

                    if (index == -1) {
                        //TODO: 
                        continue;
                    }

                    Comparable a = (Comparable) o1.getAllValues()[index];
                    Comparable b = (Comparable) o2.getAllValues()[index];

                    if (a == null && b == null) {
                        continue;
                    } else if (a == null) {
                        return 1;
                    } else if (b == null) {
                        return -1;
                    } else {
                        int temp = a.compareTo(b);
                        if (temp != 0) {
                            return temp;
                        } else {
                            continue;
                        }
                    }
                }

                return 0;
            }
        };
    }

    @Override
    public boolean hasNext() {
        return tupleIterator.hasNext();
    }

    @Override
    public ITuple next() {
        if (scanCount++ % 100 == 1 && System.currentTimeMillis() > context.getDeadline()) {
            throw new KylinTimeoutException("Query timeout after \"kylin.query.timeout-seconds\" seconds");
        }

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

    private void flushScanCountDelta() {
        context.increaseProcessedRowCount(scanCountDelta);
        scanCountDelta = 0;
    }

}
