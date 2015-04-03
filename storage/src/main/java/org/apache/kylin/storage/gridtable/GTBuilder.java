package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.apache.kylin.storage.gridtable.IGTStore.IGTStoreWriter;

public class GTBuilder implements Closeable, Flushable {

    @SuppressWarnings("unused")
    final private GTInfo info;
    final private IGTStoreWriter storeWriter;

    final private GTRowBlock block;
    final private GTRowBlock.Writer blockWriter;

    private int writtenRowCount;
    private int writtenRowBlockCount;

    GTBuilder(GTInfo info, int shard, IGTStore store) throws IOException {
        this(info, shard, store, false);
    }

    GTBuilder(GTInfo info, int shard, IGTStore store, boolean append) throws IOException {
        this.info = info;

        block = GTRowBlock.allocate(info);
        blockWriter = block.getWriter();
        if (append) {
            storeWriter = store.append(shard, blockWriter);
            if (block.isFull()) {
                blockWriter.clearForNext();
            }
        } else {
            storeWriter = store.rebuild(shard);
        }
    }

    public void write(GTRecord r) throws IOException {
        blockWriter.append(r);
        writtenRowCount++;

        if (block.isFull()) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        blockWriter.readyForFlush();
        storeWriter.write(block);
        writtenRowBlockCount++;

        if (block.isFull()) {
            blockWriter.clearForNext();
        }
    }

    @Override
    public void close() throws IOException {
        if (block.isEmpty() == false) {
            flush();
        }
        storeWriter.close();
    }

    public int getWrittenRowCount() {
        return writtenRowCount;
    }

    public int getWrittenRowBlockCount() {
        return writtenRowBlockCount;
    }
}
