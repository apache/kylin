package com.kylinolap.storage.hbase.coprocessor;

import com.google.common.collect.Maps;
import com.kylinolap.cube.measure.MeasureAggregator;

import java.util.SortedMap;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 */
public abstract class AggregationCache {
    transient int rowMemBytes;
    static final int MEMORY_USAGE_CAP = 500 * 1024 * 1024; // 500 MB
    protected final SortedMap<CoprocessorProjector.AggrKey, MeasureAggregator[]> aggBufMap;

    public AggregationCache() {
        this.aggBufMap = Maps.newTreeMap();
    }

    public abstract MeasureAggregator[] createBuffer();

    public MeasureAggregator[] getBuffer(CoprocessorProjector.AggrKey aggkey) {
        MeasureAggregator[] aggBuf = aggBufMap.get(aggkey);
        if (aggBuf == null) {
            aggBuf = createBuffer();
            aggBufMap.put(aggkey.copy(), aggBuf);
        }
        return aggBuf;
    }

    public long getSize() {
        return aggBufMap.size();
    }

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
