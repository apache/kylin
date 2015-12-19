package org.apache.kylin.gridtable;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryWaterLevel;
import org.apache.kylin.cube.util.KryoUtils;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class GTAggregateScanner implements IGTScanner {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(GTAggregateScanner.class);

    final GTInfo info;
    final ImmutableBitSet dimensions; // dimensions to return, can be more than group by
    final ImmutableBitSet groupBy;
    final ImmutableBitSet metrics;
    final String[] metricsAggrFuncs;
    final IGTScanner inputScanner;
    final AggregationCache aggrCache;
    final boolean enableMemCheck;

    private int aggregatedRowCount = 0;
    private MemoryWaterLevel memTracker;

    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req, boolean enableMemCheck) {
        if (!req.hasAggregation())
            throw new IllegalStateException();

        this.info = inputScanner.getInfo();
        this.dimensions = req.getColumns().andNot(req.getAggrMetrics());
        this.groupBy = req.getAggrGroupBy();
        this.metrics = req.getAggrMetrics();
        this.metricsAggrFuncs = req.getAggrMetricsFuncs();
        this.inputScanner = inputScanner;
        this.aggrCache = new AggregationCache();
        this.enableMemCheck = enableMemCheck;
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
        logger.info("Last spill, current AggregationCache memory estimated size is: " + getEstimateSizeOfAggrCache());
        aggrCache.spillBuffMap();
        return aggrCache.iterator();
    }

    /** return the estimate memory size of aggregation cache */
    public long getEstimateSizeOfAggrCache() {
        return aggrCache.estimatedMemSize();
    }

    class AggregationCache implements Closeable {
        final static double SPILL_THRESHOLD_GB = 0.5;

        final List<Dump> dumps;
        final int keyLength;
        final boolean[] compareMask;

        final Kryo kryo = KryoUtils.getKryo();

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
            if (enableMemCheck && (++aggregatedRowCount % 1000 == 0)) {
                if (memTracker != null) {
                    memTracker.markHigh();
                }
            }

            // Here will spill to disk when aggBufMap used too large memory
            long estimated = estimatedMemSize();
            if (estimated > SPILL_THRESHOLD_GB * MemoryBudgetController.ONE_GB) {
                logger.info("AggregationCache memory estimated size is: " + estimated);
                spillBuffMap();
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
            final DumpMerger merger = new DumpMerger(dumps);

            return new Iterator<GTRecord>() {

                final Iterator<Entry<byte[], MeasureAggregator[]>> it = merger.iterator();

                final ByteBuffer metricsBuf = ByteBuffer.allocate(info.getMaxColumnLength(metrics));
                final GTRecord secondRecord = new GTRecord(info);

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public GTRecord next() {
                    Entry<byte[], MeasureAggregator[]> entry = it.next();
                    create(entry.getKey(), entry.getValue());
                    return secondRecord;
                }

                private void create(byte[] key, MeasureAggregator[] value) {
                    int offset = 0;
                    for (int i = 0; i < dimensions.trueBitCount(); i++) {
                        int c = dimensions.trueBitAt(i);
                        final int columnLength = info.codeSystem.maxCodeLength(c);
                        secondRecord.set(c, new ByteArray(key, offset, columnLength));
                        offset += columnLength;
                    }
                    metricsBuf.clear();
                    for (int i = 0; i < value.length; i++) {
                        int col = metrics.trueBitAt(i);
                        int pos = metricsBuf.position();
                        info.codeSystem.encodeColumnValue(col, value[i].getState(), metricsBuf);
                        secondRecord.cols[col].set(metricsBuf.array(), pos, metricsBuf.position() - pos);
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        class Dump implements Iterable<Entry<byte[], MeasureAggregator[]>> {
            File dumpedFile;
            Input input;
            SortedMap<byte[], MeasureAggregator[]> buffMap;

            public Dump(SortedMap<byte[], MeasureAggregator[]> buffMap) throws IOException {
                this.buffMap = buffMap;
            }

            @Override
            public Iterator<Entry<byte[], MeasureAggregator[]>> iterator() {
                try {
                    if (dumpedFile == null || !dumpedFile.exists()) {
                        throw new RuntimeException("Dumped file cannot be found at: " + (dumpedFile == null ? "<null>" : dumpedFile.getAbsolutePath()));
                    }

                    input = new Input(new FileInputStream(dumpedFile));

                    final int count = kryo.readObject(input, Integer.class);
                    return new Iterator<Entry<byte[], MeasureAggregator[]>>() {
                        int cursorIdx = 0;

                        @Override
                        public boolean hasNext() {
                            return cursorIdx < count;
                        }

                        @Override
                        public Entry<byte[], MeasureAggregator[]> next() {
                            try {
                                cursorIdx++;
                                return (ImmutablePair<byte[], MeasureAggregator[]>) kryo.readObject(input, ImmutablePair.class);
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
                    Output output = null;
                    try {
                        dumpedFile = File.createTempFile("KYLIN_AGGR_", ".tmp");

                        logger.info("AggregationCache will dump to file: " + dumpedFile.getAbsolutePath());
                        output = new Output(new FileOutputStream(dumpedFile));
                        kryo.writeObject(output, buffMap.size());
                        for (Entry<byte[], MeasureAggregator[]> entry : buffMap.entrySet()) {
                            kryo.writeObject(output, new ImmutablePair(entry.getKey(), entry.getValue()));
                        }
                    } finally {
                        buffMap = null;
                        if (output != null)
                            output.close();
                    }
                }
            }

            public void terminate() throws IOException {
                buffMap = null;
                if (input != null)
                    input.close();
                if (dumpedFile != null && dumpedFile.exists())
                    dumpedFile.delete();
            }
        }

        class DumpMerger implements Iterable<Entry<byte[], MeasureAggregator[]>> {
            final PriorityQueue<Entry<byte[], Integer>> minHeap;
            final List<Iterator<Entry<byte[], MeasureAggregator[]>>> dumpIterators;
            final List<MeasureAggregator[]> dumpCurrentValues;

            public DumpMerger(List<Dump> dumps) {
                minHeap = new PriorityQueue<>(dumps.size(), new Comparator<Entry<byte[], Integer>>() {
                    @Override
                    public int compare(Entry<byte[], Integer> o1, Entry<byte[], Integer> o2) {
                        return bytesComparator.compare(o1.getKey(), o2.getKey());
                    }
                });
                dumpIterators = Lists.newArrayListWithCapacity(dumps.size());
                dumpCurrentValues = Lists.newArrayListWithCapacity(dumps.size());

                Iterator<Entry<byte[], MeasureAggregator[]>> it;
                for (int i = 0; i < dumps.size(); i++) {
                    it = dumps.get(i).iterator();
                    if (it.hasNext()) {
                        dumpIterators.add(i, it);
                        Entry<byte[], MeasureAggregator[]> entry = it.next();
                        minHeap.offer(new ImmutablePair(entry.getKey(), i));
                        dumpCurrentValues.add(i, entry.getValue());
                    } else {
                        dumpIterators.add(i, null);
                        dumpCurrentValues.add(i, null);
                    }
                }
            }

            private void enqueueFromDump(int index) {
                if (dumpIterators.get(index) != null && dumpIterators.get(index).hasNext()) {
                    Entry<byte[], MeasureAggregator[]> entry = dumpIterators.get(index).next();
                    minHeap.offer(new ImmutablePair(entry.getKey(), index));
                    dumpCurrentValues.set(index, entry.getValue());
                }
            }

            @Override
            public Iterator<Entry<byte[], MeasureAggregator[]>> iterator() {
                return new Iterator<Entry<byte[], MeasureAggregator[]>>() {
                    @Override
                    public boolean hasNext() {
                        return !CollectionUtils.isEmpty(minHeap);
                    }

                    @Override
                    public Entry<byte[], MeasureAggregator[]> next() {
                        // Use minimum heap to merge sort the keys,
                        // also do aggregation for measures with same keys in different dumps
                        Entry<byte[], Integer> peekEntry = minHeap.poll();
                        MeasureAggregator[] mergedAggr = dumpCurrentValues.get(peekEntry.getValue());
                        enqueueFromDump(peekEntry.getValue());

                        while (!minHeap.isEmpty() && bytesComparator.compare(peekEntry.getKey(), minHeap.peek().getKey()) == 0) {
                            Entry<byte[], Integer> newPeek = minHeap.poll();

                            MeasureAggregator[] newPeekAggr = dumpCurrentValues.get(newPeek.getValue());
                            for (int i = 0; i < newPeekAggr.length; i++) {
                                mergedAggr[i].aggregate(newPeekAggr[i].getState());
                            }

                            enqueueFromDump(newPeek.getValue());
                        }

                        return new ImmutablePair(peekEntry.getKey(), mergedAggr);
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