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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryWaterLevel;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class GTAggregateScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(GTAggregateScanner.class);

    final GTInfo info;
    final ImmutableBitSet dimensions; // dimensions to return, can be more than group by
    final ImmutableBitSet groupBy;
    final ImmutableBitSet metrics;
    final String[] metricsAggrFuncs;
    final IGTScanner inputScanner;
    final AggregationCache aggrCache;
    final long spillThreshold;

    private int aggregatedRowCount = 0;
    private MemoryWaterLevel memTracker;

    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req) {
        if (!req.hasAggregation())
            throw new IllegalStateException();

        this.info = inputScanner.getInfo();
        this.dimensions = req.getColumns().andNot(req.getAggrMetrics());
        this.groupBy = req.getAggrGroupBy();
        this.metrics = req.getAggrMetrics();
        this.metricsAggrFuncs = req.getAggrMetricsFuncs();
        this.inputScanner = inputScanner;
        this.aggrCache = new AggregationCache();
        this.spillThreshold = (long) (req.getAggrCacheGB() * MemoryBudgetController.ONE_GB);
    }

    public static long estimateSizeOfAggrCache(byte[] keySample, MeasureAggregator<?>[] aggrSample, int size) {
        // Aggregation cache is basically a tree map. The tree map entry overhead is
        // - 40 according to http://java-performance.info/memory-consumption-of-java-data-types-2/
        // - 41~52 according to AggregationCacheMemSizeTest
        return (estimateSizeOf(keySample) + estimateSizeOf(aggrSample) + 64) * size;
    }

    public static long estimateSizeOf(MeasureAggregator[] aggrs) {
        // size of array, AggregationCacheMemSizeTest reports 4 for [0], 12 for [1], 12 for [2], 20 for [3] etc..
        // Memory alignment to 8 bytes
        long est = (aggrs.length + 1) / 2 * 8 + 4 + (4 /* extra */);
        for (MeasureAggregator aggr : aggrs) {
            if (aggr != null)
                est += aggr.getMemBytesEstimate();
        }
        return est;
    }

    public static long estimateSizeOf(byte[] bytes) {
        // AggregationCacheMemSizeTest reports 20 for byte[10] and 20 again for byte[16]
        // Memory alignment to 8 bytes
        return (bytes.length + 7) / 8 * 8 + 4 + (4 /* extra */);
    }

    public void trackMemoryLevel(MemoryWaterLevel tracker) {
        this.memTracker = tracker;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public int getScannedRowCount() {
        return inputScanner.getScannedRowCount();
    }

    @Override
    public void close() throws IOException {
        inputScanner.close();
        aggrCache.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        for (GTRecord r : inputScanner) {
            aggrCache.aggregate(r);
        }
        return aggrCache.iterator();
    }

    public int getNumOfSpills() {
        return aggrCache.dumps.size();
    }

    /** return the estimate memory size of aggregation cache */
    public long getEstimateSizeOfAggrCache() {
        return aggrCache.estimatedMemSize();
    }

    class AggregationCache implements Closeable {
        final List<Dump> dumps;
        final int keyLength;
        final boolean[] compareMask;
        final MeasureCodec measureCodec;

        final Comparator<byte[]> bytesComparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                int result = 0;
                // profiler shows this check is slow
                // Preconditions.checkArgument(keyLength == o1.length && keyLength == o2.length);
                for (int i = 0; i < keyLength; ++i) {
                    if (compareMask[i]) {
                        int a = (o1[i] & 0xff);
                        int b = (o2[i] & 0xff);
                        result = a - b;
                        if (result == 0) {
                            continue;
                        } else {
                            return result;
                        }
                    }
                }
                return result;
            }
        };

        SortedMap<byte[], MeasureAggregator[]> aggBufMap;

        public AggregationCache() {
            compareMask = createCompareMask();
            keyLength = compareMask.length;
            dumps = Lists.newArrayList();
            aggBufMap = createBuffMap();
            measureCodec = createMeasureCodec();
        }

        private MeasureCodec createMeasureCodec() {
            DataType[] types = new DataType[metrics.trueBitCount()];
            for (int i = 0; i < types.length; i++) {
                types[i] = info.getColumnType(metrics.trueBitAt(i));
            }
            return new MeasureCodec(types);
        }

        private boolean[] createCompareMask() {
            int keyLength = 0;
            for (int i = 0; i < dimensions.trueBitCount(); i++) {
                int c = dimensions.trueBitAt(i);
                int l = info.codeSystem.maxCodeLength(c);
                keyLength += l;
            }

            boolean[] mask = new boolean[keyLength];
            int p = 0;
            for (int i = 0; i < dimensions.trueBitCount(); i++) {
                int c = dimensions.trueBitAt(i);
                int l = info.codeSystem.maxCodeLength(c);
                boolean m = groupBy.get(c) ? true : false;
                for (int j = 0; j < l; j++) {
                    mask[p++] = m;
                }
            }
            return mask;
        }

        private SortedMap<byte[], MeasureAggregator[]> createBuffMap() {
            return Maps.newTreeMap(bytesComparator);
        }

        private byte[] createKey(GTRecord record) {
            byte[] result = new byte[keyLength];
            int offset = 0;
            for (int i = 0; i < dimensions.trueBitCount(); i++) {
                int c = dimensions.trueBitAt(i);
                final ByteArray byteArray = record.cols[c];
                final int columnLength = info.codeSystem.maxCodeLength(c);
                System.arraycopy(byteArray.array(), byteArray.offset(), result, offset, byteArray.length());
                offset += columnLength;
            }
            assert offset == result.length;
            return result;
        }

        void aggregate(GTRecord r) {
            if (++aggregatedRowCount % 1000 == 0) {
                if (memTracker != null) {
                    memTracker.markHigh();
                }
                if (spillThreshold > 0) {
                    // spill to disk when aggBufMap used too large memory
                    if (estimatedMemSize() > spillThreshold) {
                        spillBuffMap();
                    }
                }
            }

            final byte[] key = createKey(r);
            MeasureAggregator[] aggrs = aggBufMap.get(key);
            if (aggrs == null) {
                aggrs = newAggregators();
                aggBufMap.put(key, aggrs);
            }
            for (int i = 0; i < aggrs.length; i++) {
                int col = metrics.trueBitAt(i);
                Object metrics = info.codeSystem.decodeColumnValue(col, r.cols[col].asBuffer());
                aggrs[i].aggregate(metrics);
            }
        }

        private void spillBuffMap() throws RuntimeException {
            if (aggBufMap.isEmpty())
                return;

            try {
                Dump dump = new Dump(aggBufMap);
                dump.flush();
                dumps.add(dump);
                aggBufMap = createBuffMap();
            } catch (Exception e) {
                throw new RuntimeException("AggregationCache spill failed: " + e.getMessage());
            }
        }

        @Override
        public void close() throws RuntimeException {
            try {
                for (Dump dump : dumps) {
                    dump.terminate();
                }
            } catch (Exception e) {
                throw new RuntimeException("AggregationCache close failed: " + e.getMessage());
            }
        }

        private MeasureAggregator[] newAggregators() {
            return info.codeSystem.newMetricsAggregators(metrics, metricsAggrFuncs);
        }

        public long estimatedMemSize() {
            if (aggBufMap.isEmpty())
                return 0;

            byte[] sampleKey = aggBufMap.firstKey();
            MeasureAggregator<?>[] sampleValue = aggBufMap.get(sampleKey);
            return estimateSizeOfAggrCache(sampleKey, sampleValue, aggBufMap.size());
        }

        public Iterator<GTRecord> iterator() {
            // the all-in-mem case
            if (dumps.isEmpty()) {
                return new Iterator<GTRecord>() {
                    final Iterator<Entry<byte[], MeasureAggregator[]>> it = aggBufMap.entrySet().iterator();
                    final ReturningRecord returningRecord = new ReturningRecord();

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public GTRecord next() {
                        Entry<byte[], MeasureAggregator[]> entry = it.next();
                        returningRecord.load(entry.getKey(), entry.getValue());
                        return returningRecord.record;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            // the spill case
            else {
                logger.info("Last spill, current AggregationCache memory estimated size is: " + getEstimateSizeOfAggrCache());
                this.spillBuffMap();

                return new Iterator<GTRecord>() {
                    final DumpMerger merger = new DumpMerger(dumps);
                    final Iterator<Pair<byte[], MeasureAggregator[]>> it = merger.iterator();
                    final ReturningRecord returningRecord = new ReturningRecord();

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public GTRecord next() {
                        Pair<byte[], MeasureAggregator[]> entry = it.next();
                        returningRecord.load(entry.getKey(), entry.getValue());
                        return returningRecord.record;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }

        class ReturningRecord {
            final GTRecord record = new GTRecord(info);
            final ByteBuffer metricsBuf = ByteBuffer.allocate(info.getMaxColumnLength(metrics));

            void load(byte[] key, MeasureAggregator[] value) {
                int offset = 0;
                for (int i = 0; i < dimensions.trueBitCount(); i++) {
                    int c = dimensions.trueBitAt(i);
                    final int columnLength = info.codeSystem.maxCodeLength(c);
                    record.cols[c].set(key, offset, columnLength);
                    offset += columnLength;
                }
                metricsBuf.clear();
                for (int i = 0; i < value.length; i++) {
                    int col = metrics.trueBitAt(i);
                    int pos = metricsBuf.position();
                    info.codeSystem.encodeColumnValue(col, value[i].getState(), metricsBuf);
                    record.cols[col].set(metricsBuf.array(), pos, metricsBuf.position() - pos);
                }
            }
        }

        class Dump implements Iterable<Pair<byte[], byte[]>> {
            File dumpedFile;
            ObjectInputStream ois;
            SortedMap<byte[], MeasureAggregator[]> buffMap;

            public Dump(SortedMap<byte[], MeasureAggregator[]> buffMap) throws IOException {
                this.buffMap = buffMap;
            }

            @Override
            public Iterator<Pair<byte[], byte[]>> iterator() {
                try {
                    if (dumpedFile == null || !dumpedFile.exists()) {
                        throw new RuntimeException("Dumped file cannot be found at: " + (dumpedFile == null ? "<null>" : dumpedFile.getAbsolutePath()));
                    }

                    ois = new ObjectInputStream(new FileInputStream(dumpedFile));
                    final int count = ois.readInt();
                    return new Iterator<Pair<byte[], byte[]>>() {
                        int cursorIdx = 0;

                        @Override
                        public boolean hasNext() {
                            return cursorIdx < count;
                        }

                        @Override
                        public Pair<byte[], byte[]> next() {
                            try {
                                cursorIdx++;
                                byte[] key = (byte[]) ois.readObject();
                                byte[] value = (byte[]) ois.readObject();
                                return new Pair<>(key, value);
                            } catch (Exception e) {
                                throw new RuntimeException("Cannot read AggregationCache from dumped file: " + e.getMessage());
                            }
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException("Failed to read dumped file: " + e.getMessage());
                }
            }

            public void flush() throws IOException {
                if (buffMap != null) {
                    ObjectOutputStream oos = null;
                    Object[] aggrResult = null;
                    final ByteBuffer metricsBuf = ByteBuffer.allocate(info.getMaxColumnLength(metrics));

                    try {
                        dumpedFile = File.createTempFile("KYLIN_AGGR_", ".tmp");

                        logger.info("AggregationCache will dump to file: " + dumpedFile.getAbsolutePath());
                        oos = new ObjectOutputStream(new FileOutputStream(dumpedFile));
                        oos.writeInt(buffMap.size());

                        for (Entry<byte[], MeasureAggregator[]> entry : buffMap.entrySet()) {
                            metricsBuf.clear();
                            MeasureAggregators aggs = new MeasureAggregators(entry.getValue());
                            aggrResult = new Object[metrics.trueBitCount()];
                            aggs.collectStates(aggrResult);
                            measureCodec.encode(aggrResult, metricsBuf);
                            oos.writeObject(entry.getKey());
                            oos.writeObject(metricsBuf.array());
                        }
                    } finally {
                        buffMap = null;
                        if (oos != null)
                            oos.close();
                    }
                }
            }

            public void terminate() throws IOException {
                buffMap = null;
                if (ois != null)
                    ois.close();
                if (dumpedFile != null && dumpedFile.exists())
                    dumpedFile.delete();
            }
        }

        class DumpMerger implements Iterable<Pair<byte[], MeasureAggregator[]>> {
            final PriorityQueue<Pair<byte[], Integer>> minHeap;
            final List<Iterator<Pair<byte[], byte[]>>> dumpIterators;
            final List<Object[]> dumpCurrentValues;
            final MeasureAggregator[] resultMeasureAggregators = newAggregators();
            final MeasureAggregators resultAggrs = new MeasureAggregators(resultMeasureAggregators);

            public DumpMerger(List<Dump> dumps) {
                minHeap = new PriorityQueue<>(dumps.size(), new Comparator<Pair<byte[], Integer>>() {
                    @Override
                    public int compare(Pair<byte[], Integer> o1, Pair<byte[], Integer> o2) {
                        return bytesComparator.compare(o1.getFirst(), o2.getFirst());
                    }
                });
                dumpIterators = Lists.newArrayListWithCapacity(dumps.size());
                dumpCurrentValues = Lists.newArrayListWithCapacity(dumps.size());

                Iterator<Pair<byte[], byte[]>> it;
                for (int i = 0; i < dumps.size(); i++) {
                    dumpCurrentValues.add(i, null);
                    it = dumps.get(i).iterator();
                    if (it.hasNext()) {
                        dumpIterators.add(i, it);
                        enqueueFromDump(i);
                    } else {
                        dumpIterators.add(i, null);
                    }
                }
            }

            private void enqueueFromDump(int index) {
                if (dumpIterators.get(index) != null && dumpIterators.get(index).hasNext()) {
                    Pair<byte[], byte[]> pair = dumpIterators.get(index).next();
                    minHeap.offer(new Pair(pair.getKey(), index));
                    Object[] metricValues = new Object[metrics.trueBitCount()];
                    measureCodec.decode(ByteBuffer.wrap(pair.getValue()), metricValues);
                    dumpCurrentValues.set(index, metricValues);
                }
            }

            @Override
            public Iterator<Pair<byte[], MeasureAggregator[]>> iterator() {
                return new Iterator<Pair<byte[], MeasureAggregator[]>>() {
                    @Override
                    public boolean hasNext() {
                        return !minHeap.isEmpty();
                    }

                    private void internalAggregate() {
                        Pair<byte[], Integer> peekEntry = minHeap.poll();
                        resultAggrs.aggregate(dumpCurrentValues.get(peekEntry.getValue()));
                        enqueueFromDump(peekEntry.getValue());
                    }

                    @Override
                    public Pair<byte[], MeasureAggregator[]> next() {
                        // Use minimum heap to merge sort the keys,
                        // also do aggregation for measures with same keys in different dumps
                        resultAggrs.reset();
                        byte[] peekKey = minHeap.peek().getKey();
                        internalAggregate();

                        while (!minHeap.isEmpty() && bytesComparator.compare(peekKey, minHeap.peek().getKey()) == 0) {
                            internalAggregate();
                        }
                        return new Pair(peekKey, resultMeasureAggregators);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }
    }
}