package org.apache.kylin.gridtable;

import java.io.Closeable;
import java.io.IOException;

public class GridTable implements Closeable {

    final GTInfo info;
    final IGTStore store;

    public GridTable(GTInfo info, IGTStore store) {
        this.info = info;
        this.store = store;
    }

    public GTBuilder rebuild() throws IOException {
        return rebuild(-1);
    }

    public GTBuilder rebuild(int shard) throws IOException {
        return new GTBuilder(info, shard, store);
    }

    public GTBuilder append() throws IOException {
        return append(-1);
    }

    public GTBuilder append(int shard) throws IOException {
        return new GTBuilder(info, shard, store, true);
    }

    public IGTScanner scan(GTScanRequest req) throws IOException {
        IGTScanner result = store.scan(req);
        return req.decorateScanner(result);
    }

    public GTInfo getInfo() {
        return info;
    }

    public IGTStore getStore() {
        return store;
    }

    @Override
    public void close() throws IOException {
        if (store instanceof Closeable) {
            ((Closeable) store).close();
        }
    }
}
