package org.apache.kylin.storage.gridtable;

import java.util.BitSet;

import org.apache.kylin.metadata.filter.TupleFilter;

public class GridTable {

    final GTInfo info;
    final IGTStore store;
    
    public GridTable(GTInfo info, IGTStore store) {
        this.info = info;
        this.store = store;
    }
    
    public GTBuilder rebuild() {
        assert info.isShardingEnabled() == false;
        return rebuild(-1);
    }
    
    public GTBuilder rebuild(int shard) {
        assert shard < info.nShards;
        return new GTBuilder(info, shard, store);
    }
    
    public IGTScanner scan(GTRecord pkStart, GTRecord pkEndExclusive, BitSet dimensions, BitSet metrics, TupleFilter filter) {
        return new GTRawScanner(info, store, pkStart, pkEndExclusive, dimensions, metrics, filter);
    }
    
    public IGTScanner scanAndAggregate(GTRecord pkStart, GTRecord pkEndExclusive, BitSet dimensions, BitSet metrics, TupleFilter filter) {
        return new GTAggregateScanner(info, store, pkStart, pkEndExclusive, dimensions, metrics, filter);
    }
}
