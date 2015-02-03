package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import com.kylinolap.metadata.measure.MeasureAggregator;
import org.apache.kylin.storage.hbase.coprocessor.AggregationCache;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorProjector;

import java.util.Map;
import java.util.Set;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 */
public class EndpointAggregationCache extends AggregationCache {

    private EndpointAggregators aggregators;

    public EndpointAggregationCache(EndpointAggregators aggregators) {
        this.aggregators = aggregators;
    }

    @Override
    public MeasureAggregator[] createBuffer() {
        return this.aggregators.createBuffer();
    }

    public Set<Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]>> getAllEntries() {
        return aggBufMap.entrySet();
    }
}
