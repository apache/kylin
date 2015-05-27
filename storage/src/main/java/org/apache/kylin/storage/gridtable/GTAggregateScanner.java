package org.apache.kylin.storage.gridtable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;

public class GTAggregateScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(GTAggregateScanner.class);
    final GTInfo info;
    final BitSet dimensions; // dimensions to return, can be more than group by
    final BitSet groupBy;
    final BitSet metrics;
    final String[] metricsAggrFuncs;
    final IGTScanner inputScanner;


    public GTAggregateScanner(IGTScanner inputScanner, GTScanRequest req) {
        if (req.hasAggregation() == false)
            throw new IllegalStateException();
        
        this.info = inputScanner.getInfo();
        this.dimensions = (BitSet) req.getColumns().clone();
        this.dimensions.andNot(req.getAggrMetrics());
        this.groupBy = req.getAggrGroupBy();
        this.metrics = req.getAggrMetrics();
        this.metricsAggrFuncs = req.getAggrMetricsFuncs();
        this.inputScanner = inputScanner;
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
    public int getScannedRowBlockCount() {
        return inputScanner.getScannedRowBlockCount();
    }

    @Override
    public void close() throws IOException {
        inputScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        AggregationCacheWithBytesKey aggregationCacheWithBytesKey = new AggregationCacheWithBytesKey();
        for (GTRecord r : inputScanner) {
            aggregationCacheWithBytesKey.aggregate(r);
            MemoryChecker.checkMemory();
        }
        return aggregationCacheWithBytesKey.iterator();
    }

    class AggregationCacheWithBytesKey {
        final SortedMap<byte[], MeasureAggregator[]> aggBufMap;

        public AggregationCacheWithBytesKey() {
            aggBufMap = Maps.newTreeMap(new Comparator<byte[]>() {
                @Override
                public int compare(byte[] o1, byte[] o2) {
                    int result = 0;
                    Preconditions.checkArgument(o1.length == o2.length);
                    final int length = o1.length;
                    for (int i = 0; i < length; ++i) {
                        result = o1[i] - o2[i];
                        if (result == 0) {
                            continue;
                        } else {
                            return result;
                        }
                    }
                    return result;
                }
            });
        }

        private byte[] createKey(GTRecord record) {
            byte[] result = new byte[info.getMaxColumnLength(groupBy)];
            int offset = 0;
            for (int i = groupBy.nextSetBit(0); i >= 0; i = groupBy.nextSetBit(i + 1)) {
                final ByteArray byteArray = record.cols[i];
                final int columnLength = info.codeSystem.maxCodeLength(i);
                System.arraycopy(byteArray.array(), byteArray.offset(), result, offset, columnLength);
                offset += columnLength;
            }
            assert offset == result.length;
            return result;
        }

        void aggregate(GTRecord r) {
            final byte[] key = createKey(r);
            MeasureAggregator[] aggrs = aggBufMap.get(key);
            if (aggrs == null) {
                aggrs = new MeasureAggregator[metricsAggrFuncs.length];
                for (int i = 0, col = -1; i < aggrs.length; i++) {
                    col = metrics.nextSetBit(col + 1);
                    aggrs[i] = info.codeSystem.newMetricsAggregator(metricsAggrFuncs[i], col);
                }
                aggBufMap.put(key, aggrs);
            }
            for (int i = 0, col = -1; i < aggrs.length; i++) {
                col = metrics.nextSetBit(col + 1);
                Object metrics = info.codeSystem.decodeColumnValue(col, r.cols[col].asBuffer());
                aggrs[i].aggregate(metrics);
            }
        }

        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {

                final Iterator<Entry<byte[], MeasureAggregator[]>> it = aggBufMap.entrySet().iterator();

                final ByteBuffer metricsBuf = ByteBuffer.allocate(info.getMaxColumnLength(metrics));
                final GTRecord secondRecord;

                {
                    BitSet dimensionsAndMetrics = (BitSet) groupBy.clone();
                    dimensionsAndMetrics.or(metrics);
                    secondRecord = new GTRecord(info, dimensionsAndMetrics);
                }

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
                    for (int i = groupBy.nextSetBit(0); i >= 0; i = groupBy.nextSetBit(i + 1)) {
                        final int columnLength = info.codeSystem.maxCodeLength(i);
                        secondRecord.set(i, new ByteArray(key, offset, columnLength));
                        offset += columnLength;
                    }
                    metricsBuf.clear();
                    for (int i = 0, col = -1; i < value.length; i++) {
                        col = metrics.nextSetBit(col + 1);
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
    }

    /*
    @SuppressWarnings({ "rawtypes", "unchecked" })
    class AggregationCache {
        final SortedMap<GTRecord, MeasureAggregator[]> aggBufMap;

        public AggregationCache() {
            this.aggBufMap = Maps.newTreeMap();
        }

        void aggregate(GTRecord r) {
            r.maskForEqualHashComp(groupBy);
            MeasureAggregator[] aggrs = aggBufMap.get(r);
            if (aggrs == null) {
                aggrs = new MeasureAggregator[metricsAggrFuncs.length];
                for (int i = 0, col = -1; i < aggrs.length; i++) {
                    col = metrics.nextSetBit(col + 1);
                    aggrs[i] = info.codeSystem.newMetricsAggregator(metricsAggrFuncs[i], col);
                }
                aggBufMap.put(r.copy(dimensions), aggrs);
            }

            for (int i = 0, col = -1; i < aggrs.length; i++) {
                col = metrics.nextSetBit(col + 1);
                Object metrics = info.codeSystem.decodeColumnValue(col, r.cols[col].asBuffer());
                aggrs[i].aggregate(metrics);
            }
        }

        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {
                
                final Iterator<Entry<GTRecord, MeasureAggregator[]>> it = aggBufMap.entrySet().iterator();

                final ByteBuffer metricsBuf = ByteBuffer.allocate(info.getMaxColumnLength(metrics));
                final GTRecord oneRecord;  // avoid instance creation

                {
                    BitSet dimensionsAndMetrics = (BitSet) groupBy.clone();
                    dimensionsAndMetrics.or(metrics);
                    oneRecord = new GTRecord(info, dimensionsAndMetrics);
                }

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public GTRecord next() {
                    Entry<GTRecord, MeasureAggregator[]> entry = it.next();

                    GTRecord dims = entry.getKey();
                    for (int i = dimensions.nextSetBit(0); i >= 0; i = dimensions.nextSetBit(i + 1)) {
                        oneRecord.cols[i].set(dims.cols[i]);
                    }
                    
                    metricsBuf.clear();
                    MeasureAggregator[] aggrs = entry.getValue();
                    for (int i = 0, col = -1; i < aggrs.length; i++) {
                        col = metrics.nextSetBit(col + 1);
                        int pos = metricsBuf.position();
                        info.codeSystem.encodeColumnValue(col, aggrs[i].getState(), metricsBuf);
                        oneRecord.cols[col].set(metricsBuf.array(), pos, metricsBuf.position() - pos);
                    }
                    return oneRecord;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        transient int rowMemBytes;
        static final int MEMORY_USAGE_CAP = 500 * 1024 * 1024; // 500 MB

        public void checkMemoryUsage() {
            // about memory calculation,
            // http://seniorjava.wordpress.com/2013/09/01/java-objects-memory-size-reference/
            if (rowMemBytes <= 0) {
                if (aggBufMap.size() > 0) {
                    rowMemBytes = 0;
                    MeasureAggregator[] measureAggregators = aggBufMap.get(aggBufMap.firstKey());
                    for (MeasureAggregator agg : measureAggregators) {
                        rowMemBytes += agg.getMemBytes();
                    }
                }
            }
            int size = aggBufMap.size();
            int memUsage = (40 + rowMemBytes) * size;
            if (memUsage > MEMORY_USAGE_CAP) {
                throw new RuntimeException("Kylin coprocess memory usage goes beyond cap, (40 + " + rowMemBytes + ") * " + size + " > " + MEMORY_USAGE_CAP + ". Abord coprocessor.");
            }
        }
    }

    */

}
