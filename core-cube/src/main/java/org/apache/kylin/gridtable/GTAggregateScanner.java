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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

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

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class GTAggregateScanner implements IGTScanner, IGTBypassChecker {

    private static final Logger logger = LoggerFactory.getLogger(GTAggregateScanner.class);
    private static final int MAX_BUFFER_SIZE = 64 * 1024 * 1024;

    final GTInfo info;
    final ImmutableBitSet dimensions; // dimensions to return, can be more than group by
    final ImmutableBitSet groupBy;
    final ImmutableBitSet metrics;
    final String[] metricsAggrFuncs;
    final IGTScanner inputScanner;
    final BufferedMeasureCodec measureCodec;
    final AggregationCache aggrCache;
    long spillThreshold; // 0 means no memory control && no spill
    final int storagePushDownLimit;//default to be Int.MAX
    final StorageLimitLevel storageLimitLevel;
    final boolean spillEnabled;
    final TupleFilter havingFilter;

    private long inputRowCount = 0L;
    private MemoryWaterLevel memTracker;
    private boolean[] aggrMask;

    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req) {
        this(inputScanner, req, true);
    }

    public GTAggregateScanner(IGTScanner input, GTScanRequest req, boolean spillEnabled) {
        if (!req.hasAggregation())
            throw new IllegalStateException();

        if (input instanceof GTFilterScanner) {
            logger.info("setting IGTBypassChecker of child");
            ((GTFilterScanner) input).setChecker(this);
        } else {
            logger.info("applying a GTFilterScanner with IGTBypassChecker on top child");
            input = new GTFilterScanner(input, null, this);
        }

        this.inputScanner = input;
        this.info = this.inputScanner.getInfo();
        this.dimensions = req.getDimensions();
        this.groupBy = req.getAggrGroupBy();
        this.metrics = req.getAggrMetrics();
        this.metricsAggrFuncs = req.getAggrMetricsFuncs();
        this.measureCodec = req.createMeasureCodec();
        this.spillThreshold = (long) (req.getAggCacheMemThreshold() * MemoryBudgetController.ONE_GB);
        this.aggrMask = new boolean[metricsAggrFuncs.length];
        this.storagePushDownLimit = req.getStoragePushDownLimit();
        this.storageLimitLevel = req.getStorageLimitLevel();
        this.spillEnabled = spillEnabled;
        this.havingFilter = req.getHavingFilterPushDown();

        this.aggrCache = new AggregationCache();

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

    public long getInputRowCount() {
        return inputRowCount;
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

            //check limit
            boolean ret = aggrCache.aggregate(r);

            if (!ret) {
                logger.info("abort reading inputScanner because storage push down limit is hit");
                break;//limit is hit
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

    public boolean shouldBypass(GTRecord record) {
        return aggrCache.shouldBypass(record);
    }

    class AggregationCache implements Closeable {
        /**
         * if a limit construct is provided
         * before a gtrecord is sent to filter->aggregate pipeline, 
         * this check could help to decide if the record should be skipped
         *
         * e.g. 
         * limit is three, and current AggregationCache's key set contains (1,2,3),
         * when gtrecord with key 4 comes, it should be skipped before sending to filter 
         */
        class ByPassChecker {
            private int aggregateBufferSizeLimit = -1;
            private byte[] currentLastKey = null;
            private int[] groupOffsetsInLastKey = null;

            private int byPassCounter = 0;

            ByPassChecker(int aggregateBufferSizeLimit) {
                this.aggregateBufferSizeLimit = aggregateBufferSizeLimit;

                //init groupOffsetsInLastKey
                int p = 0;
                int idx = 0;
                this.groupOffsetsInLastKey = new int[groupBy.trueBitCount()];
                for (int i = 0; i < dimensions.trueBitCount(); i++) {
                    int c = dimensions.trueBitAt(i);
                    int l = info.codeSystem.maxCodeLength(c);
                    if (groupBy.get(c))
                        groupOffsetsInLastKey[idx++] = p;
                    p += l;
                }
            }

            /**
             * @return true if should bypass this record
             */
            boolean shouldByPass(GTRecord record) {

                if (dumps.size() > 0) {
                    return false; //rare case: limit tends to be small, when limit is applied it's not likely to have dumps
                    //TODO: what if bypass before dump happens?
                }

                Preconditions.checkState(aggBufMap.size() <= aggregateBufferSizeLimit);

                if (aggBufMap.size() == aggregateBufferSizeLimit) {
                    Preconditions.checkNotNull(currentLastKey);
                    for (int i = 0; i < groupBy.trueBitCount(); i++) {
                        int c = groupBy.trueBitAt(i);
                        ByteArray col = record.get(c);

                        int compare = Bytes.compareTo(col.array(), col.offset(), col.length(), currentLastKey,
                                groupOffsetsInLastKey[i], col.length());
                        if (compare > 0) {
                            byPassCounter++;
                            return true;
                        } else if (compare < 0) {
                            return false;
                        }
                    }
                }

                return false;
            }

            void updateOnBufferChange() {
                if (aggBufMap.size() > aggregateBufferSizeLimit) {
                    aggBufMap.pollLastEntry();
                    Preconditions.checkState(aggBufMap.size() == aggregateBufferSizeLimit);
                }

                currentLastKey = aggBufMap.lastKey();
            }

            int getByPassCounter() {
                return byPassCounter;
            }
        }

        final List<Dump> dumps;
        final int keyLength;
        final boolean[] compareMask;
        boolean compareAll = true;
        long sumSpilledSize = 0;
        ByPassChecker byPassChecker = null;

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

        TreeMap<byte[], MeasureAggregator[]> aggBufMap;

        public AggregationCache() {
            compareMask = createCompareMask();
            for (boolean l : compareMask) {
                compareAll = compareAll && l;
            }
            keyLength = compareMask.length;
            dumps = Lists.newArrayList();
            aggBufMap = createBuffMap();

            if (storageLimitLevel == StorageLimitLevel.LIMIT_ON_RETURN_SIZE) {
                //ByPassChecker is not free, if LIMIT_ON_SCAN, not worth to as it has better optimization
                byPassChecker = new ByPassChecker(storagePushDownLimit);
            }
        }

        public boolean shouldBypass(GTRecord record) {
            if (byPassChecker == null) {
                return false;
            }

            boolean b = byPassChecker.shouldByPass(record);
            return b;
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
                boolean m = groupBy.get(c);
                for (int j = 0; j < l; j++) {
                    mask[p++] = m;
                }
            }
            return mask;
        }

        private TreeMap<byte[], MeasureAggregator[]> createBuffMap() {
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

        boolean aggregate(GTRecord r) {
            if (++inputRowCount % 100000 == 0) {
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
                //TODO: what if bypass before dump happens?
                if (getNumOfSpills() == 0 && storageLimitLevel == StorageLimitLevel.LIMIT_ON_SCAN
                        && aggBufMap.size() >= storagePushDownLimit) {
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

            if (byPassChecker != null) {
                byPassChecker.updateOnBufferChange();
            }

            return true;
        }

        private void spillBuffMap(long estMemSize) throws RuntimeException {
            try {
                Dump dump = new Dump(aggBufMap, estMemSize);
                dump.flush();
                dumps.add(dump);
                sumSpilledSize += dump.size();
                // when spilled data is too much, we can modify it by other strategy.
                // this means, all spilled data is bigger than half of original spillThreshold.
                if (sumSpilledSize > spillThreshold) {
                    for (Dump current : dumps) {
                        current.spill();
                    }
                    spillThreshold += sumSpilledSize;
                    sumSpilledSize = 0;
                } else {
                    spillThreshold -= dump.size();
                }
            } catch (Exception e) {
                throw new RuntimeException("AggregationCache failed to spill", e);
            }
        }

        @Override
        public void close() throws RuntimeException {
            try {
                logger.info("closing aggrCache");
                if (byPassChecker != null) {
                    logger.info("AggregationCache byPassChecker helps to skip {} cuboid rows",
                            byPassChecker.getByPassCounter());
                }

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
                    record.cols[c].reset(key, offset, columnLength);
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
                    record.cols[col].reset(bytes, offset, sizes[i]);
                    offset += sizes[i];
                }
            }
        }

        class Dump implements Iterable<Pair<byte[], byte[]>> {
            final File dumpedFile;
            SortedMap<byte[], MeasureAggregator[]> buffMap;
            final long estMemSize;
            byte[] spillBuffer;
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

                    if (spillBuffer == null) {
                        dis = new DataInputStream(new FileInputStream(dumpedFile));
                    } else {
                        dis = new DataInputStream(new ByteArrayInputStream(spillBuffer));
                    }
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
                                dis.readFully(key);
                                int valueLen = dis.readInt();
                                byte[] value = new byte[valueLen];
                                dis.readFully(value);
                                return new Pair<>(key, value);
                            } catch (Exception e) {
                                throw new NoSuchElementException(
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

            public void spill() throws IOException {
                if (spillBuffer == null)
                    return;
                OutputStream ops = new FileOutputStream(dumpedFile);
                InputStream ips = new ByteArrayInputStream(spillBuffer);
                IOUtils.copy(ips, ops);
                spillBuffer = null;
                IOUtils.closeQuietly(ips);
                IOUtils.closeQuietly(ops);

                logger.info("Spill buffer to disk, location: {}, size = {}.", dumpedFile.getAbsolutePath(),
                        dumpedFile.length());
            }

            public int size() {
                return spillBuffer == null ? 0 : spillBuffer.length;
            }

            public void flush() throws IOException {
                logger.info("AggregationCache(size={} est_mem_size={} threshold={}) will spill to {}", buffMap.size(),
                        estMemSize, spillThreshold, dumpedFile.getAbsolutePath());
                ByteArrayOutputStream baos = new ByteArrayOutputStream(MAX_BUFFER_SIZE);
                if (buffMap != null) {
                    DataOutputStream bos = new DataOutputStream(baos);
                    Object[] aggrResult = null;
                    try {
                        bos.writeInt(buffMap.size());

                        for (Entry<byte[], MeasureAggregator[]> entry : buffMap.entrySet()) {
                            MeasureAggregators aggs = new MeasureAggregators(entry.getValue());
                            aggrResult = new Object[metrics.trueBitCount()];
                            aggs.collectStates(aggrResult);
                            ByteBuffer metricsBuf = measureCodec.encode(aggrResult);

                            bos.writeInt(entry.getKey().length);
                            bos.write(entry.getKey());
                            bos.writeInt(metricsBuf.position());
                            bos.write(metricsBuf.array(), 0, metricsBuf.position());
                        }
                    } finally {
                        buffMap = null;
                        IOUtils.closeQuietly(bos);
                    }
                }
                spillBuffer = baos.toByteArray();
                IOUtils.closeQuietly(baos);
                logger.info("Accurately spill data size = {}", spillBuffer.length);
            }

            public void terminate() throws IOException {
                buffMap = null;
                if (dis != null)
                    IOUtils.closeQuietly(dis);
                if (dumpedFile != null && dumpedFile.exists())
                    dumpedFile.delete();
                spillBuffer = null;
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
                    minHeap.offer(new SimpleEntry(pair.getFirst(), index));
                    Object[] metricValues = new Object[metrics.trueBitCount()];
                    measureCodec.decode(ByteBuffer.wrap(pair.getSecond()), metricValues);
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
