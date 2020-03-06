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
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;

public class GTAggregateTransformScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(GTAggregateTransformScanner.class);

    protected final IGTScanner inputScanner;
    private final GTScanRequest req;
    private final GTTwoLayerAggregateParam twoLayerAggParam;
    private long inputRowCount = 0L;

    public GTAggregateTransformScanner(IGTScanner inputScanner, GTScanRequest req) {
        this.inputScanner = inputScanner;
        this.req = req;
        this.twoLayerAggParam = req.getAggregateTransformParam();
    }

    @Override
    public GTInfo getInfo() {
        return inputScanner.getInfo();
    }

    @Override
    public void close() throws IOException {
        inputScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return twoLayerAggParam.satisfyPrefix(req.getDimensions())
                ? new FragmentTransformGTRecordIterator(inputScanner.iterator())
                : new NormalTransformGTRecordIterator(inputScanner.iterator());
    }

    public long getInputRowCount() {
        return inputRowCount;
    }

    /**
     * first, aggregate records fragment by fragment
     *      records in a fragment shared the same prefix
     *      each fragment will generate one aggregated record
     * then, transform aggregated record
     */
    private class FragmentTransformGTRecordIterator extends TransformGTRecordIterator {
        private final PrefixFragmentIterator fragmentIterator;

        private Iterator<GTRecord> transformedFragment = null;

        FragmentTransformGTRecordIterator(Iterator<GTRecord> input) {
            this.fragmentIterator = new PrefixFragmentIterator(input, twoLayerAggParam.vanishDimMask);
        }

        @Override
        public boolean hasNext() {
            if (transformedFragment != null && transformedFragment.hasNext()) {
                return true;
            }

            if (!fragmentIterator.hasNext()) {
                return false;
            }

            IGTScanner fragmentScanner = new IGTScanner() {
                @Override
                public GTInfo getInfo() {
                    return req.getInfo();
                }

                @Override
                public void close() throws IOException {
                }

                @Override
                public Iterator<GTRecord> iterator() {
                    return fragmentIterator.next();
                }
            };
            transformedFragment = new GTAggregateScanner(fragmentScanner, innerReq).iterator();

            return hasNext();
        }

        @Override
        public GTRecord next() {
            return transformGTRecord(transformedFragment.next());
        }
    }

    /**
     * first, aggregate all the records first
     * then transform record one by one
     */
    private class NormalTransformGTRecordIterator extends TransformGTRecordIterator {

        private final Iterator<GTRecord> aggRecordIterator;

        NormalTransformGTRecordIterator(final Iterator<GTRecord> input) {
            IGTScanner gtScanner = new IGTScanner() {
                @Override
                public GTInfo getInfo() {
                    return req.getInfo();
                }

                @Override
                public void close() throws IOException {
                }

                @Override
                public Iterator<GTRecord> iterator() {
                    return input;
                }
            };

            aggRecordIterator = new GTAggregateScanner(gtScanner, innerReq).iterator();
        }

        @Override
        public boolean hasNext() {
            return aggRecordIterator.hasNext();
        }

        @Override
        public GTRecord next() {
            return transformGTRecord(aggRecordIterator.next());
        }
    }

    private abstract class TransformGTRecordIterator implements Iterator<GTRecord> {

        private final IGTCodeSystem codeSystem;

        // transformation
        private final ImmutableBitSet tMetrics;
        private final String[] tMetricsFuncs;
        private final int[] sMetrics;

        protected final GTScanRequest innerReq;

        TransformGTRecordIterator() {
            this.codeSystem = req.getInfo().getCodeSystem();
            this.tMetrics = twoLayerAggParam.outsideLayerMetrics;
            this.tMetricsFuncs = twoLayerAggParam.outsideLayerMetricsFuncs;
            this.sMetrics = twoLayerAggParam.insideLayerMetrics;

            this.innerReq = transformGTScanRequest(req);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        /**
         * 1. add vanishDimMask to the groupby in GTScanRequest
         * 2. transform metrics
         */
        private GTScanRequest transformGTScanRequest(GTScanRequest req) {
            ImmutableBitSet aggrGroupBy = req.getAggrGroupBy();
            aggrGroupBy = aggrGroupBy.or(twoLayerAggParam.vanishDimMask);

            ImmutableBitSet metrics = req.getAggrMetrics();
            String[] metricsFuncs = req.getAggrMetricsFuncs();

            Map<Integer, String> metricsFuncsMap = Maps.newHashMap();
            for (int i = 0; i < metrics.trueBitCount(); i++) {
                int c = metrics.trueBitAt(i);
                metricsFuncsMap.put(c, metricsFuncs[i]);
            }

            String[] sMetricsFuncs = twoLayerAggParam.insideLayerMetricsFuncs;
            for (int i = 0; i < sMetrics.length; i++) {
                int c = sMetrics[i];
                metricsFuncsMap.put(c, sMetricsFuncs[i]);
            }

            metrics = metrics.or(ImmutableBitSet.valueOf(sMetrics)).andNot(tMetrics);
            metricsFuncs = new String[metrics.trueBitCount()];
            for (int i = 0; i < metrics.trueBitCount(); i++) {
                int c = metrics.trueBitAt(i);
                metricsFuncs[i] = metricsFuncsMap.get(c);
            }

            return new GTScanRequestBuilder().setAggrMetrics(metrics).setAggrMetricsFuncs(metricsFuncs)
                    .setInfo(req.getInfo()).setDimensions(req.getDimensions()).setAggrGroupBy(aggrGroupBy)
                    .setAggCacheMemThreshold(req.getAggCacheMemThreshold())
                    .setStoragePushDownLimit(req.getStoragePushDownLimit())
                    .setHavingFilterPushDown(req.getHavingFilterPushDown()).createGTScanRequest();
        }

        protected GTRecord transformGTRecord(GTRecord record) {
            MeasureAggregator[] tAggrs = codeSystem.newMetricsAggregators(tMetrics, tMetricsFuncs);
            for (int i = 0; i < tMetrics.trueBitCount(); i++) {
                int o = sMetrics[i];
                Object oMetric = codeSystem.decodeColumnValue(o, record.cols[o].asBuffer());
                Object cMetric = ((MeasureTransformation) tAggrs[i]).transformMeasure(oMetric);
                int c = tMetrics.trueBitAt(i);
                record.setValue(c, cMetric);
            }
            return record;
        }
    }

    private class PrefixFragmentIterator implements Iterator<Iterator<GTRecord>> {

        private final PeekingIterator<GTRecord> input;
        private final Comparator<GTRecord> prefixComparator;

        private GTRecord current;

        PrefixFragmentIterator(Iterator<GTRecord> input, ImmutableBitSet prefixMask) {
            this.input = Iterators.peekingIterator(input);
            this.prefixComparator = GTRecord.getComparator(prefixMask);
        }

        @Override
        public boolean hasNext() {
            return input.hasNext();
        }

        @Override
        public Iterator<GTRecord> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            current = null;

            return new Iterator<GTRecord>() {

                @Override
                public boolean hasNext() {
                    return current == null || shouldIncludeNext(current);
                }

                @Override
                public GTRecord next() {
                    if (current == null) {
                        current = new GTRecord(req.getInfo());
                    }
                    inputRowCount++;
                    current.shallowCopyFrom(input.next());
                    return current;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        private boolean isSamePrefix(GTRecord o1, GTRecord o2) {
            return prefixComparator.compare(o1, o2) == 0;
        }

        private boolean shouldIncludeNext(GTRecord current) {
            return input.hasNext() && isSamePrefix(current, input.peek());
        }
    }
}