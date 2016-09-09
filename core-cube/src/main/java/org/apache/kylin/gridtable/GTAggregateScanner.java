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
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryWaterLevel;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.datatype.DataType;
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
    final int storagePushDownLimit;//default to be Int.MAX
    final long deadline;

    private int aggregatedRowCount = 0;
    private MemoryWaterLevel memTracker;
    private boolean[] aggrMask;

    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req, long deadline) {
        if (!req.hasAggregation())
            throw new IllegalStateException();

        this.info = inputScanner.getInfo();
        this.dimensions = req.getDimensions();
        this.groupBy = req.getAggrGroupBy();
        this.metrics = req.getAggrMetrics();
        this.metricsAggrFuncs = req.getAggrMetricsFuncs();
        this.inputScanner = inputScanner;
        this.aggrCache = new AggregationCache();
        this.spillThreshold = (long) (req.getAggCacheMemThreshold() * MemoryBudgetController.ONE_GB);
        this.aggrMask = new boolean[metricsAggrFuncs.length];
        this.storagePushDownLimit = req.getStoragePushDownLimit();
        this.deadline = deadline;

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
    public long getScannedRowCount() {
        return inputScanner.getScannedRowCount();
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

            //check deadline
            if (count % GTScanRequest.terminateCheckInterval == 1 && System.currentTimeMillis() > deadline) {
                throw new GTScanTimeoutException("Timeout in GTAggregateScanner with scanned count " + count);
            }

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
        final BufferedMeasureEncoder measureCodec;

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

        private BufferedMeasureEncoder createMeasureCodec() {
            DataType[] types = new DataType[metrics.trueBitCount()];
            for (int i = 0; i < types.length; i++) {
                types[i] = info.getColumnType(metrics.trueBitAt(i));
            }

            BufferedMeasureEncoder result = new BufferedMeasureEncoder(types);
            result.setBufferSize(info.getMaxColumnLength(metrics));
            return result;
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
            if (dumps.isEmpty()) {
                // the all-in-mem case

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
            } else {
                // the spill case

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
            File dumpedFile;
            DataInputStream dis;
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
                    DataOutputStream dos = null;
                    Object[] aggrResult = null;
                    try {
                        dumpedFile = File.createTempFile("KYLIN_AGGR_", ".tmp");

                        logger.info("AggregationCache will dump to file: " + dumpedFile.getAbsolutePath());
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