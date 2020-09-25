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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregator;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Iterators;
import org.apache.kylin.shaded.com.google.common.collect.PeekingIterator;

/**
 * GTStreamAggregateScanner requires input records to be sorted on group fields.
 * In such cases, it's superior to hash/sort based aggregator because it can produce
 * ordered outputs on the fly and the memory consumption is very low.
 */
public class GTStreamAggregateScanner extends GTForwardingScanner {
    private final GTScanRequest req;
    private final Comparator<GTRecord> keyComparator;

    public GTStreamAggregateScanner(IGTScanner delegated, GTScanRequest scanRequest) {
        super(delegated);
        this.req = Preconditions.checkNotNull(scanRequest, "scanRequest");
        this.keyComparator = GTRecord.getComparator(scanRequest.getAggrGroupBy());
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return new StreamMergeGTRecordIterator(delegated.iterator());
    }

    public Iterator<Object[]> valuesIterator(int[] gtDimsIdx, int[] gtMetricsIdx) {
        return new StreamMergeValuesIterator(delegated.iterator(), gtDimsIdx, gtMetricsIdx);
    }

    private abstract class AbstractStreamMergeIterator<E> implements Iterator<E> {
        final PeekingIterator<GTRecord> input;
        final IGTCodeSystem codeSystem;
        final ImmutableBitSet dimensions;
        final ImmutableBitSet metrics;
        final String[] metricFuncs;
        final BufferedMeasureCodec measureCodec;

        private final GTRecord first; // reuse to avoid object creation

        AbstractStreamMergeIterator(Iterator<GTRecord> input) {
            this.input = Iterators.peekingIterator(input);
            this.codeSystem = req.getInfo().getCodeSystem();
            this.dimensions = req.getDimensions();
            this.metrics = req.getAggrMetrics();
            this.metricFuncs = req.getAggrMetricsFuncs();
            this.measureCodec = req.createMeasureCodec();

            this.first = new GTRecord(req.getInfo());
        }

        @Override
        public boolean hasNext() {
            return input.hasNext();
        }

        private boolean isSameKey(GTRecord o1, GTRecord o2) {
            return keyComparator.compare(o1, o2) == 0;
        }

        private boolean shouldMergeNext(GTRecord current) {
            return input.hasNext() && isSameKey(current, input.peek());
        }

        protected abstract E finalizeResult(GTRecord record);

        protected abstract E finalizeResult(GTRecord record, Object[] aggStates);

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // WATCH OUT! record returned by "input" scanner could be changed later,
            // so we must make a shallow copy of it.
            first.shallowCopyFrom(input.next());

            // shortcut to avoid extra deserialize/serialize cost
            if (!shouldMergeNext(first)) {
                return finalizeResult(first);
            }
            // merge records with the same key
            MeasureAggregator[] aggrs = codeSystem.newMetricsAggregators(metrics, metricFuncs);
            aggregate(aggrs, first);
            aggregate(aggrs, input.next()); // no need to copy record because it's not referred to later
            while (shouldMergeNext(first)) {
                aggregate(aggrs, input.next());
            }

            Object[] aggStates = new Object[aggrs.length];
            for (int i = 0; i < aggStates.length; i++) {
                aggStates[i] = aggrs[i].getState();
            }
            return finalizeResult(first, aggStates);
        }

        @SuppressWarnings("unchecked")
        protected void aggregate(MeasureAggregator[] aggregators, GTRecord record) {
            for (int i = 0; i < aggregators.length; i++) {
                int c = metrics.trueBitAt(i);
                Object metric = codeSystem.decodeColumnValue(c, record.cols[c].asBuffer());
                aggregators[i].aggregate(metric);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    private class StreamMergeGTRecordIterator extends AbstractStreamMergeIterator<GTRecord> {

        private GTRecord returnRecord; // avoid object creation

        StreamMergeGTRecordIterator(Iterator<GTRecord> input) {
            super(input);
            this.returnRecord = new GTRecord(req.getInfo());
        }

        @Override
        protected GTRecord finalizeResult(GTRecord record) {
            return record;
        }

        @Override
        protected GTRecord finalizeResult(GTRecord record, Object[] aggStates) {
            // 1. load dimensions
            for (int c : dimensions) {
                returnRecord.cols[c] = record.cols[c];
            }
            // 2. serialize metrics
            byte[] bytes = measureCodec.encode(aggStates).array();
            int[] sizes = measureCodec.getMeasureSizes();
            // 3. load metrics
            int offset = 0;
            for (int i = 0; i < metrics.trueBitCount(); i++) {
                int c = metrics.trueBitAt(i);
                returnRecord.cols[c].reset(bytes, offset, sizes[i]);
                offset += sizes[i];
            }
            return returnRecord;
        }
    }

    private class StreamMergeValuesIterator extends AbstractStreamMergeIterator<Object[]> {

        private int[] gtDimsIdx;
        private int[] gtMetricsIdx; // specify which metric to return and their order
        private int[] aggIdx; // specify the ith returning metric's aggStates index

        private Object[] result; // avoid object creation

        StreamMergeValuesIterator(Iterator<GTRecord> input, int[] gtDimsIdx, int[] gtMetricsIdx) {
            super(input);
            this.gtDimsIdx = gtDimsIdx;
            this.gtMetricsIdx = gtMetricsIdx;
            this.aggIdx = new int[gtMetricsIdx.length];
            for (int i = 0; i < aggIdx.length; i++) {
                int metricIdx = gtMetricsIdx[i];
                aggIdx[i] = metrics.trueBitIndexOf(metricIdx);
            }

            this.result = new Object[gtDimsIdx.length + gtMetricsIdx.length];
        }

        private void decodeAndSetDimensions(GTRecord record) {
            for (int i = 0; i < gtDimsIdx.length; i++) {
                result[i] = record.decodeValue(gtDimsIdx[i]);
            }
        }

        @Override
        protected Object[] finalizeResult(GTRecord record) {
            decodeAndSetDimensions(record);
            // decode metrics
            for (int i = 0; i < gtMetricsIdx.length; i++) {
                result[gtDimsIdx.length + i] = record.decodeValue(gtMetricsIdx[i]);
            }
            return result;
        }

        @Override
        protected Object[] finalizeResult(GTRecord record, Object[] aggStates) {
            decodeAndSetDimensions(record);
            // set metrics
            for (int i = 0; i < aggIdx.length; i++) {
                result[gtDimsIdx.length + i] = aggStates[aggIdx[i]];
            }
            return result;
        }
    }
}
