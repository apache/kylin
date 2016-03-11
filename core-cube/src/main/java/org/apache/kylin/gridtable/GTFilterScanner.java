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

package org.apache.kylin.gridtable;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

public class GTFilterScanner implements IGTScanner {

    final private IGTScanner inputScanner;
    final private TupleFilter filter;
    final private IEvaluatableTuple oneTuple; // avoid instance creation

    private GTRecord next = null;

    public GTFilterScanner(IGTScanner inputScanner, GTScanRequest req) throws IOException {
        this.inputScanner = inputScanner;
        this.filter = req.getFilterPushDown();
        this.oneTuple = new IEvaluatableTuple() {
            @Override
            public Object getValue(TblColRef col) {
                return next.get(col.getColumnDesc().getZeroBasedIndex());
            }
        };

        if (TupleFilter.isEvaluableRecursively(filter) == false)
            throw new IllegalArgumentException();
    }

    @Override
    public GTInfo getInfo() {
        return inputScanner.getInfo();
    }

    @Override
    public int getScannedRowCount() {
        return inputScanner.getScannedRowCount();
    }

    @Override
    public void close() throws IOException {
        inputScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return new Iterator<GTRecord>() {

            private Iterator<GTRecord> inputIterator = inputScanner.iterator();

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;

                IFilterCodeSystem<ByteArray> filterCodeSystem = GTUtil.wrap(getInfo().codeSystem.getComparator());

                while (inputIterator.hasNext()) {
                    next = inputIterator.next();
                    if (filter != null && filter.evaluate(oneTuple, filterCodeSystem) == false) {
                        continue;
                    }
                    return true;
                }
                next = null;
                return false;
            }

            @Override
            public GTRecord next() {
                // fetch next record
                if (next == null) {
                    hasNext();
                    if (next == null)
                        throw new NoSuchElementException();
                }

                GTRecord result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }
}
