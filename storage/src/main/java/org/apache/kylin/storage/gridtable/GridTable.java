package org.apache.kylin.storage.gridtable;

import java.io.IOException;

public class GridTable {

    final GTInfo info;
    final IGTStore store;

    public GridTable(GTInfo info, IGTStore store) {
        this.info = info;
        this.store = store;
    }

    public GTBuilder rebuild() throws IOException {
        assert info.isShardingEnabled() == false;
        return rebuild(-1);
    }

    public GTBuilder rebuild(int shard) throws IOException {
        assert shard < info.nShards;
        return new GTBuilder(info, shard, store);
    }

    public GTBuilder append() throws IOException {
        assert info.isShardingEnabled() == false;
        return append(-1);
    }

    public GTBuilder append(int shard) throws IOException {
        return new GTBuilder(info, shard, store, true);
    }

    public IGTScanner scan(GTScanRequest req) throws IOException {
        IGTScanner result = new GTFilterScanner(info, store, req);
        if (req.hasAggregation()) {
            result = new GTAggregateScanner(result, req);
        }
        return result;
    }

    public GTInfo getInfo() {
        return info;
    }

    public IGTStore getStore() {
        return store;
    }
}
