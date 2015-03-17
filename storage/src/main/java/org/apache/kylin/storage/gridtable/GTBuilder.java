package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.apache.kylin.storage.gridtable.IGTStore.IGTStoreWriter;

public class GTBuilder implements Closeable, Flushable {

    @SuppressWarnings("unused")
    final private GTInfo info;
    final private IGTStoreWriter writer;
    
    private GTRowBlock block;
    private int writtenRowCount;
    private int writtenRowBlockCount;

    GTBuilder(GTInfo info, int shard, IGTStore store) {
        this(info, shard, store, false);
    }

    GTBuilder(GTInfo info, int shard, IGTStore store, boolean append) {
        this.info = info;
        
        block = GTRowBlock.allocate(info);
        if (append) {
            writer = store.append(shard, block);
            if (block.isFull())
                block.clearForNext();
        }
        else {
            writer = store.rebuild(shard);
        }
    }
    
    public void write(GTRecord r) throws IOException {
        block.append(r);
        writtenRowCount++;
        
        if (block.isFull()) {
            flush();
        }
    }
    
    @Override
    public void flush() throws IOException {
        block.flipCellBlockBuffers();
        writer.write(block);
        writtenRowBlockCount++;
        
        block.clearForNext();
    }
    
    @Override
    public void close() throws IOException {
        if (block.isEmpty() == false) {
            flush();
        }
        writer.close();
    }
    
    public int getWrittenRowCount() {
        return writtenRowCount;
    }

    public int getWrittenRowBlockCount() {
        return writtenRowBlockCount;
    }
}
