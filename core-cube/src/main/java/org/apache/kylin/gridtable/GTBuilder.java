package org.apache.kylin.gridtable;

import java.io.Closeable;
import java.io.IOException;

public class GTBuilder implements Closeable {

    @SuppressWarnings("unused")
    final private GTInfo info;
    final private IGTWriter storeWriter;

    private int writtenRowCount;

    GTBuilder(GTInfo info, int shard, IGTStore store) throws IOException {
        this(info, shard, store, false);
    }

    GTBuilder(GTInfo info, int shard, IGTStore store, boolean append) throws IOException {
        this.info = info;

        if (append) {
            storeWriter = store.append(shard);
        } else {
            storeWriter = store.rebuild(shard);
        }
    }

    public void write(GTRecord r) throws IOException {
        storeWriter.write(r);
        writtenRowCount++;
    }

    @Override
    public void close() throws IOException {
        storeWriter.close();
    }

    public int getWrittenRowCount() {
        return writtenRowCount;
    }

}
