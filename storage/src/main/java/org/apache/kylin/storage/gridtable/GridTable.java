package org.apache.kylin.storage.gridtable;

import java.io.IOException;
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

    public GTBuilder append() {
        assert info.isShardingEnabled() == false;
        return append(-1);
    }

    public GTBuilder append(int shard) {
        return new GTBuilder(info, shard, store, true);
    }

    public IGTScanner scan(GTRecord pkStart, GTRecord pkEndExclusive, BitSet columns, TupleFilter filterPushDown) throws IOException {
        return new GTRawScanner(info, store, pkStart, pkEndExclusive, columns, filterPushDown);
    }

    public IGTScanner scanAndAggregate(GTRecord pkStart, GTRecord pkEndExclusive, BitSet dimensions, //
            BitSet metrics, String[] metricsAggrFuncs, TupleFilter filterPushDown) throws IOException {
        BitSet columns = new BitSet();
        columns.or(dimensions);
        columns.or(metrics);
        IGTScanner rawScanner = scan(pkStart, pkEndExclusive, columns, filterPushDown);
        return new GTAggregateScanner(rawScanner, dimensions, metrics, metricsAggrFuncs);
    }

    public GTInfo getInfo() {
        return info;
    }

    public IGTStore getStore() {
        return store;
    }
}
