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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryWaterLevel;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
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
    final BufferedMeasureCodec measureCodec;
    final AggregationCache aggrCache;
    final long spillThreshold; // 0 means no memory control && no spill
    final int storagePushDownLimit;//default to be Int.MAX
    final boolean spillEnabled;
    final TupleFilter havingFilter;

    private int aggregatedRowCount = 0;
    private MemoryWaterLevel memTracker;
    private boolean[] aggrMask;

    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req) {
        this(inputScanner, req, true);
    }

    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req, boolean spillEnabled) {
        if (!req.hasAggregation())
            throw new IllegalStateException();

        this.info = inputScanner.getInfo();
        this.dimensions = req.getDimensions();
        this.groupBy = req.getAggrGroupBy();
        this.metrics = req.getAggrMetrics();
        this.metricsAggrFuncs = req.getAggrMetricsFuncs();
        this.inputScanner = inputScanner;
        this.measureCodec = req.createMeasureCodec();
        this.aggrCache = new AggregationCache();
        this.spillThreshold = (long) (req.getAggCacheMemThreshold() * MemoryBudgetController.ONE_GB);
        this.aggrMask = new boolean[metricsAggrFuncs.length];
        this.storagePushDownLimit = req.getStoragePushDownLimit();
        this.spillEnabled = spillEnabled;
        this.havingFilter = req.getHavingFilterPushDown();

        Arrays.fill(aggrMask, true);
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
        long est = (aggrs.length + 1) / 2 * 8L + 4 + (4 /* extra */);
        for (MeasureAggregator aggr : aggrs) {
            if (aggr != null)
                est += aggr.getMemBytesEstimate();
        }
        return est;
    }

    public static long estimateSizeOf(byte[] bytes) {
        // AggregationCacheMemSizeTest reports 20 for byte[10] and 20 again for byte[16]
        // Memory alignment to 8 bytes
        return (bytes.length + 7) / 8 * 8L + 4 + (4 /* extra */);
    }

    public void trackMemoryLevel(MemoryWaterLevel tracker) {
        this.memTracker = tracker;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public void close() throws IOException {
        inputScanner.close();
        aggrCache.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        long count = 0;
        for (GTRecord r : inputScanner) {

            if (getNumOfSpills() == 0) {
                //check limit
                boolean ret = aggrCache.aggregate(r, storagePushDownLimit);

                if (!ret) {
                    logger.info("abort reading inputScanner because storage push down limit is hit");
                    break;//limit is hit
                }
            } else {//else if dumps is not empty, it means a lot of row need aggregated, so it's less likely that limit clause is helping 
                aggrCache.aggregate(r, Integer.MAX_VALUE);
            }

            count++;
        }
        logger.info("GTAggregateScanner input rows: " + count);
        return aggrCache.iterator();
    }

    public int getNumOfSpills() {
        return aggrCache.dumps.size();
    }

    public void setAggrMask(boolean[] aggrMask) {
        this.aggrMask = aggrMask;
    }

    /** return the estimate memory size of aggregation cache */
    public long getEstimateSizeOfAggrCache() {
        return aggrCache.estimatedMemSize();
    }

    class AggregationCache implements Closeable {
        final List<Dump> dumps;
        final int keyLength;
        final boolean[] compareMask;
        boolean compareAll = true;

        final Comparator<byte[]> bytesComparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                if (compareAll) {
                    return Bytes.compareTo(o1, o2);
                }

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
            for (boolean l : compareMask) {
                compareAll = compareAll && l;
            }
            keyLength = compareMask.length;
            dumps = Lists.newArrayList();
            aggBufMap = createBuffMap();
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

        boolean aggregate(GTRecord r, int stopForLimit) {
            if (++aggregatedRowCount % 100000 == 0) {
                if (memTracker != null) {
                    memTracker.markHigh();
                }

                final long estMemSize = estimatedMemSize();
                if (spillThreshold > 0 && estMemSize > spillThreshold) {
                    if (!spillEnabled) {
                        throw new ResourceLimitExceededException("aggregation's memory consumption " + estMemSize
                                + " exceeds threshold " + spillThreshold);
                    }
                    spillBuffMap(estMemSize); // spill to disk
                    aggBufMap = createBuffMap();
                }
            }

            final byte[] key = createKey(r);
            MeasureAggregator[] aggrs = aggBufMap.get(key);
            if (aggrs == null) {

                //for storage push down limit
                if (aggBufMap.size() >= stopForLimit) {
                    return false;
                }

                aggrs = newAggregators();
                aggBufMap.put(key, aggrs);
            }
            for (int i = 0; i < aggrs.length; i++) {
                if (aggrMask[i]) {
                    int col = metrics.trueBitAt(i);
                    Object metrics = info.codeSystem.decodeColumnValue(col, r.cols[col].asBuffer());
                    aggrs[i].aggregate(metrics);
                }
            }
            return true;
        }

        private void spillBuffMap(long estMemSize) throws RuntimeException {
            try {
                Dump dump = new Dump(aggBufMap, estMemSize);
                dump.flush();
                dumps.add(dump);
            } catch (Exception e) {
                throw new RuntimeException("AggregationCache failed to spill", e);
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
            Iterator<Entry<byte[], MeasureAggregator[]>> it = null;

            if (dumps.isEmpty()) {
                // the all-in-mem case
                it = aggBufMap.entrySet().iterator();
            } else {
                // the spill case
                if (!aggBufMap.isEmpty()) {
                    spillBuffMap(getEstimateSizeOfAggrCache()); // TODO allow merge in-mem map with spilled dumps
                }
                DumpMerger merger = new DumpMerger(dumps);
                it = merger.iterator();
            }

            final Iterator<Entry<byte[], MeasureAggregator[]>> input = it;

            return new Iterator<GTRecord>() {

                final ReturningRecord returningRecord = new ReturningRecord();
                Entry<byte[], MeasureAggregator[]> returningEntry = null;
                final HavingFilterChecker havingFilterChecker = (havingFilter == null) ? null
                        : new HavingFilterChecker();

                @Override
                public boolean hasNext() {
                    while (returningEntry == null && input.hasNext()) {
                        returningEntry = input.next();
                        if (havingFilterChecker != null)
                            returningEntry = havingFilterChecker.check(returningEntry);
                    }
                    return returningEntry != null;
                }

                @Override
                public GTRecord next() {
                    returningRecord.load(returningEntry.getKey(), returningEntry.getValue());
                    returningEntry = null;
                    return returningRecord.record;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        class HavingFilterChecker {

            final HavingFilterTuple tuple = new HavingFilterTuple();
            final IFilterCodeSystem cs = new HavingFilterCodeSys();

            HavingFilterChecker() {
                logger.info("Evaluating 'having' filter -- " + havingFilter);
            }

            public Entry<byte[], MeasureAggregator[]> check(Entry<byte[], MeasureAggregator[]> returningEntry) {
                tuple.aggrValues = returningEntry.getValue();
                boolean pass = havingFilter.evaluate(tuple, cs);
                return pass ? returningEntry : null;
            }
        }

        private class HavingFilterCodeSys implements IFilterCodeSystem {

            Object o2Cache;
            double n2Cache;

            @Override
            public int compare(Object o1, Object o2) {
                if (o1 == null && o2 == null)
                    return 0;

                if (o1 == null) // null is bigger to align with CubeCodeSystem
                    return 1;

                if (o2 == null) // null is bigger to align with CubeCodeSystem
                    return -1;

                // for the 'having clause', we only concern numbers and BigDecimal
                // we try to cache the o2, which should be a constant according to CompareTupleFilter.evaluate()

                double n1;
                if (o1 instanceof Number) {
                    n1 = ((Number) o1).doubleValue();
                } else if (o1 instanceof HLLCounter) {
                    n1 = ((HLLCounter) o1).getCountEstimate();
                } else if (o1 instanceof BitmapCounter) {
                    n1 = ((BitmapCounter) o1).getCount();
                } else if (o1 instanceof PercentileCounter) {
                    n1 = ((PercentileCounter) o1).getResultEstimate();
                } else {
                    throw new RuntimeException("Unknown datatype: value=" + o1 + ", class=" + o1.getClass());
                }

                double n2 = (o2Cache == o2) ? n2Cache : Double.parseDouble((String) o2);

                if (o2Cache == null) {
                    o2Cache = o2;
                    n2Cache = n2;
                }

                return Double.compare(n1, n2);
            }

            @Override
            public boolean isNull(Object code) {
                return code == null;
            }

            @Override
            public void serialize(Object code, ByteBuffer buf) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object deserialize(ByteBuffer buf) {
                throw new UnsupportedOperationException();
            }
        }

        private class HavingFilterTuple implements ITuple {
            MeasureAggregator[] aggrValues;

            @Override
            public Object getValue(TblColRef col) {
                return aggrValues[col.getColumnDesc().getZeroBasedIndex()].getState();
            }

            @Override
            public List<String> getAllFields() {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<TblColRef> getAllColumns() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object[] getAllValues() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ITuple makeCopy() {
                throw new UnsupportedOperationException();
            }
        };

        class ReturningRecord {
            final GTRecord record = new GTRecord(info);
            final Object[] tmpValues = new Object[metrics.trueBitCount()];

            void load(byte[] key, MeasureAggregator[] value) {
                int offset = 0;
                for (int i = 0; i < dimensions.trueBitCount(); i++) {
                    int c = dimensions.trueBitAt(i);
                    final int columnLength = info.codeSystem.maxCodeLength(c);
                    record.cols[c].set(key, offset, columnLength);
                    offset += columnLength;
                }

                for (int i = 0; i < value.length; i++) {
                    tmpValues[i] = value[i].getState();
                }

                byte[] bytes = measureCodec.encode(tmpValues).array();
                int[] sizes = measureCodec.getMeasureSizes();
                offset = 0;
                for (int i = 0; i < value.length; i++) {
                    int col = metrics.trueBitAt(i);
                    record.cols[col].set(bytes, offset, sizes[i]);
                    offset += sizes[i];
                }
            }
        }

        class Dump implements Iterable<Pair<byte[], byte[]>> {
            final File dumpedFile;
            SortedMap<byte[], MeasureAggregator[]> buffMap;
            final long estMemSize;

            DataInputStream dis;

            public Dump(SortedMap<byte[], MeasureAggregator[]> buffMap, long estMemSize) throws IOException {
                this.dumpedFile = File.createTempFile("KYLIN_SPILL_", ".tmp");
                this.buffMap = buffMap;
                this.estMemSize = estMemSize;
            }

            @Override
            public Iterator<Pair<byte[], byte[]>> iterator() {
                try {
                    if (dumpedFile == null || !dumpedFile.exists()) {
                        throw new RuntimeException("Dumped file cannot be found at: "
                                + (dumpedFile == null ? "<null>" : dumpedFile.getAbsolutePath()));
                    }

                    dis = new DataInputStream(new FileInputStream(dumpedFile));
                    final int count = dis.readInt();
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
                                int keyLen = dis.readInt();
                                byte[] key = new byte[keyLen];
                                dis.read(key);
                                int valueLen = dis.readInt();
                                byte[] value = new byte[valueLen];
                                dis.read(value);
                                return new Pair<>(key, value);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Cannot read AggregationCache from dumped file: " + e.getMessage());
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
                logger.info("AggregationCache(size={} est_mem_size={} threshold={}) will spill to {}", buffMap.size(),
                        estMemSize, spillThreshold, dumpedFile.getAbsolutePath());

                if (buffMap != null) {
                    DataOutputStream dos = null;
                    Object[] aggrResult = null;
                    try {
                        dos = new DataOutputStream(new FileOutputStream(dumpedFile));
                        dos.writeInt(buffMap.size());
                        for (Entry<byte[], MeasureAggregator[]> entry : buffMap.entrySet()) {
                            MeasureAggregators aggs = new MeasureAggregators(entry.getValue());
                            aggrResult = new Object[metrics.trueBitCount()];
                            aggs.collectStates(aggrResult);
                            ByteBuffer metricsBuf = measureCodec.encode(aggrResult);
                            dos.writeInt(entry.getKey().length);
                            dos.write(entry.getKey());
                            dos.writeInt(metricsBuf.position());
                            dos.write(metricsBuf.array(), 0, metricsBuf.position());
                        }
                    } finally {
                        buffMap = null;
                        IOUtils.closeQuietly(dos);
                    }
                }
            }

            public void terminate() throws IOException {
                buffMap = null;
                if (dis != null)
                    dis.close();
                if (dumpedFile != null && dumpedFile.exists())
                    dumpedFile.delete();
            }
        }

        class DumpMerger implements Iterable<Entry<byte[], MeasureAggregator[]>> {
            final PriorityQueue<Entry<byte[], Integer>> minHeap;
            final List<Iterator<Pair<byte[], byte[]>>> dumpIterators;
            final List<Object[]> dumpCurrentValues;
            final MeasureAggregator[] resultMeasureAggregators = newAggregators();
            final MeasureAggregators resultAggrs = new MeasureAggregators(resultMeasureAggregators);

            public DumpMerger(List<Dump> dumps) {
                minHeap = new PriorityQueue<>(dumps.size(), new Comparator<Entry<byte[], Integer>>() {
                    @Override
                    public int compare(Entry<byte[], Integer> o1, Entry<byte[], Integer> o2) {
                        return bytesComparator.compare(o1.getKey(), o2.getKey());
                    }
                });
                dumpIterators = Lists.newArrayListWithCapacity(dumps.size());
                dumpCurrentValues = Lists.newArrayListWithCapacity(dumps.size());

                Iterator<Pair<byte[], byte[]>> it;
                for (int i = 0; i < dumps.size(); i++) {
                    it = dumps.get(i).iterator();
                    dumpCurrentValues.add(i, null);
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
                    minHeap.offer(new SimpleEntry(pair.getKey(), index));
                    Object[] metricValues = new Object[metrics.trueBitCount()];
                    measureCodec.decode(ByteBuffer.wrap(pair.getValue()), metricValues);
                    dumpCurrentValues.set(index, metricValues);
                }
            }

            @Override
            public Iterator<Entry<byte[], MeasureAggregator[]>> iterator() {
                return new Iterator<Entry<byte[], MeasureAggregator[]>>() {
                    @Override
                    public boolean hasNext() {
                        return !minHeap.isEmpty();
                    }

                    private void internalAggregate() {
                        Entry<byte[], Integer> peekEntry = minHeap.poll();
                        resultAggrs.aggregate(dumpCurrentValues.get(peekEntry.getValue()));
                        enqueueFromDump(peekEntry.getValue());
                    }

                    @Override
                    public Entry<byte[], MeasureAggregator[]> next() {
                        // Use minimum heap to merge sort the keys,
                        // also do aggregation for measures with same keys in different dumps
                        resultAggrs.reset();

                        byte[] peekKey = minHeap.peek().getKey();
                        internalAggregate();

                        while (!minHeap.isEmpty() && bytesComparator.compare(peekKey, minHeap.peek().getKey()) == 0) {
                            internalAggregate();
                        }

                        return new SimpleEntry(peekKey, resultMeasureAggregators);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }
    }

    private static class SimpleEntry<K, V> implements Entry<K, V> {
        K k;
        V v;

        SimpleEntry(K k, V v) {
            this.k = k;
            this.v = v;
        }

        @Override
        public K getKey() {
            return k;
        }

        @Override
        public V getValue() {
            return v;
        }

        @Override
        public V setValue(V value) {
            V oldV = v;
            this.v = value;
            return oldV;
        }
    }
}
