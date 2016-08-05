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
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.MeasureType.IAdvMeasureFiller;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SegmentCubeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(SegmentCubeTupleIterator.class);

    protected final CubeSegmentScanner scanner;
    protected final Cuboid cuboid;
    protected final Set<TblColRef> selectedDimensions;
    protected final Set<FunctionDesc> selectedMetrics;
    protected final TupleInfo tupleInfo;
    protected final Tuple tuple;
    protected final StorageContext context;

    protected Iterator<GTRecord> gtItr;
    protected CubeTupleConverter cubeTupleConverter;
    protected Tuple next;

    private List<IAdvMeasureFiller> advMeasureFillers;
    private int advMeasureRowsRemaining;
    private int advMeasureRowIndex;

    public SegmentCubeTupleIterator(CubeSegmentScanner scanner, Cuboid cuboid, Set<TblColRef> selectedDimensions, //
            Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context) {
        this.scanner = scanner;
        this.cuboid = cuboid;
        this.selectedDimensions = selectedDimensions;
        this.selectedMetrics = selectedMetrics;
        this.tupleInfo = returnTupleInfo;
        this.tuple = new Tuple(returnTupleInfo);
        this.context = context;
        this.gtItr = getGTItr(scanner);
        this.cubeTupleConverter = new CubeTupleConverter(scanner.cubeSeg, cuboid, selectedDimensions, selectedMetrics, tupleInfo);
    }

    private Iterator<GTRecord> getGTItr(CubeSegmentScanner scanner) {
        return scanner.iterator();
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;

        // consume any left rows from advanced measure filler
        if (advMeasureRowsRemaining > 0) {
            for (IAdvMeasureFiller filler : advMeasureFillers) {
                filler.fillTuple(tuple, advMeasureRowIndex);
            }
            advMeasureRowIndex++;
            advMeasureRowsRemaining--;
            next = tuple;
            return true;
        }

        // now we have a GTRecord
        if (!gtItr.hasNext()) {
            return false;
        }
        GTRecord curRecord = gtItr.next();

        Preconditions.checkNotNull(cubeTupleConverter);

        // translate into tuple
        advMeasureFillers = cubeTupleConverter.translateResult(curRecord, tuple);

        // the simple case
        if (advMeasureFillers == null) {
            next = tuple;
            return true;
        }

        // advanced measure filling, like TopN, will produce multiple tuples out of one record
        advMeasureRowsRemaining = -1;
        for (IAdvMeasureFiller filler : advMeasureFillers) {
            if (advMeasureRowsRemaining < 0)
                advMeasureRowsRemaining = filler.getNumOfRows();
            if (advMeasureRowsRemaining != filler.getNumOfRows())
                throw new IllegalStateException();
        }
        if (advMeasureRowsRemaining < 0)
            throw new IllegalStateException();

        advMeasureRowIndex = 0;
        return hasNext();
    }

    @Override
    public ITuple next() {
        // fetch next record
        if (next == null) {
            hasNext();
            if (next == null)
                throw new NoSuchElementException();
        }

        ITuple result = next;
        next = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        close(scanner);
    }

    protected void close(CubeSegmentScanner scanner) {
        try {
            scanner.close();
        } catch (IOException e) {
            logger.error("Exception when close CubeScanner", e);
        }
    }

    public CubeTupleConverter getCubeTupleConverter() {
        return cubeTupleConverter;
    }
}
