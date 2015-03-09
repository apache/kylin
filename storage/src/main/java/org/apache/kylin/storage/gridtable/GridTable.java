package org.apache.kylin.storage.gridtable;

public class GridTable {

    final GTInfo info;
    
    public GridTable(GTInfo info, GTStore store) {
        this.info = info;
    }
    
    public GTBuilder rebuild(GTStore store) {
        assert info.nShards == 0;
        return rebuild(store, 0);
    }
    
    public GTBuilder rebuild(GTStore store, int shard) {
        return new GTBuilder(info, shard, store);
    }
}
