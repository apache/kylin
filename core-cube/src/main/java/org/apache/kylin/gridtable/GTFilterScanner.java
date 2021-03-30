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

import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

public class GTFilterScanner extends GTForwardingScanner {

    private TupleFilter filter;
    private IFilterCodeSystem<ByteArray> filterCodeSystem;
    private IEvaluatableTuple oneTuple; // avoid instance creation

    private GTRecord next = null;
    private long inputRowCount = 0L;

    private IGTBypassChecker checker = null;

    public GTFilterScanner(IGTScanner delegated, GTScanRequest req, IGTBypassChecker checker) {
        super(delegated);
        this.checker = checker;

        if (req != null) {
            this.filter = req.getFilterPushDown();
            this.filterCodeSystem = GTUtil.wrap(getInfo().codeSystem.getComparator());
            this.oneTuple = new IEvaluatableTuple() {
                @Override
                public Object getValue(TblColRef col) {
                    return next.get(col.getColumnDesc().getZeroBasedIndex());
                }
            };

            if (!TupleFilter.isEvaluableRecursively(filter))
                throw new IllegalArgumentException();
        }
    }

    public void setChecker(IGTBypassChecker checker) {
        this.checker = checker;
    }

    public long getInputRowCount() {
        return inputRowCount;
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return new GTFilterScannerIterator();
    }

    private class GTFilterScannerIterator implements Iterator<GTRecord> {
        private Iterator<GTRecord> inputIterator = delegated.iterator();
        private FilterResultCache resultCache = new FilterResultCache(getInfo(), filter);

        @Override
        public boolean hasNext() {
            if (next != null)
                return true;

            while (inputIterator.hasNext()) {
                next = inputIterator.next();
                inputRowCount++;
                if (!evaluate()) {
                    continue;
                }
                return true;
            }
            next = null;
            return false;
        }

        private boolean evaluate() {
            if (checker != null && checker.shouldBypass(next)) {
                return false;
            }

            if (filter == null)
                return true;

            // 'next' and 'oneTuple' are referring to the same record
            boolean[] cachedResult = resultCache.checkCache(next);
            if (cachedResult != null)
                return cachedResult[0];

            boolean result = filter.evaluate(oneTuple, filterCodeSystem);
            resultCache.setLastResult(result);
            return result;
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
    }

    // cache the last one input and result, can reuse because rowkey are ordered, and same input could come in small group
    public static class FilterResultCache {
        static final int CHECKPOINT = 10000;
        static final double HIT_RATE_THRESHOLD = 0.5;
        public static final boolean DEFAULT_OPTION = true; // enable cache by default
        private boolean enabled = DEFAULT_OPTION;
        ImmutableBitSet colsInFilter;
        int count;
        int hit;
        byte[] lastValues;
        boolean[] lastResult;

        public FilterResultCache(GTInfo info, TupleFilter filter) {
            colsInFilter = collectColumnsInFilter(filter);
            lastValues = new byte[info.getMaxColumnLength(colsInFilter)];
            lastResult = new boolean[1];
        }

        public boolean[] checkCache(GTRecord record) {
            if (!enabled)
                return null;

            count++;

            // disable the cache if the hit rate is bad
            if (count == CHECKPOINT) {
                if ((double) hit / (double) count < HIT_RATE_THRESHOLD) {
                    enabled = false;
                }
            }

            boolean match = count > 1;
            int p = 0;
            for (int i = 0; i < colsInFilter.trueBitCount(); i++) {
                int c = colsInFilter.trueBitAt(i);
                ByteArray col = record.get(c);
                if (match) {
                    match = BytesUtil.compareBytes(col.array(), col.offset(), lastValues, p, col.length()) == 0;
                }
                if (!match) {
                    System.arraycopy(col.array(), col.offset(), lastValues, p, col.length());
                }
                p += col.length();
            }

            if (match) {
                hit++;
                return lastResult;
            } else {
                return null;
            }
        }

        public void setLastResult(boolean evalResult) {
            lastResult[0] = evalResult;
        }

        private ImmutableBitSet collectColumnsInFilter(TupleFilter filter) {
            Set<TblColRef> columnsInFilter = new HashSet<>();
            TupleFilter.collectColumns(filter, columnsInFilter);
            BitSet result = new BitSet();
            for (TblColRef col : columnsInFilter)
                result.set(col.getColumnDesc().getZeroBasedIndex());
            return new ImmutableBitSet(result);
        }

    }
}
