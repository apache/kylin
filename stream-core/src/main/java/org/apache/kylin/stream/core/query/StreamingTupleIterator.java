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

package org.apache.kylin.stream.core.query;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.kylin.measure.MeasureType.IAdvMeasureFiller;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.stream.core.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingTupleIterator implements ITupleIterator {
    private static final Logger logger = LoggerFactory.getLogger(StreamingTupleIterator.class);

    protected final TupleInfo tupleInfo;
    protected final Tuple tuple;

    protected Iterator<Record> gtRecords;
    protected StreamingTupleConverter tupleConverter;
    protected Tuple next;

    private List<IAdvMeasureFiller> advMeasureFillers;
    private int advMeasureRowsRemaining;
    private int advMeasureRowIndex;

    public StreamingTupleIterator(Iterator<Record> gtRecords, StreamingTupleConverter tupleConverter,
                                  TupleInfo returnTupleInfo) {
        this.gtRecords = gtRecords;
        this.tupleConverter = tupleConverter;
        this.tupleInfo = returnTupleInfo;
        this.tuple = new Tuple(returnTupleInfo);
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

        if (!gtRecords.hasNext()) {
            return false;
        }
        // now we have a GTRecord
        Record curRecord = gtRecords.next();

        // translate into tuple
        advMeasureFillers = tupleConverter.translateResult(curRecord, tuple);

        // the simple case
        if (advMeasureFillers == null) {
            next = tuple;
            return true;
        }

        // advanced measure filling, like TopN, will produce multiple tuples out
        // of one record
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

    }
}
