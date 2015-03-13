package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.storage.gridtable.IGTStore.IGTStoreWriter;

public class GTBuilder implements Closeable, Flushable {

    final private GTInfo info;
    final private IGTStoreWriter writer;
    
    private GTRowBlock block;

    GTBuilder(GTInfo info, int shard, IGTStore store) {
        this.info = info;
        this.writer = store.rebuild(shard);
        this.block = GTRowBlock.allocate(info);
    }

    public void write(GTRecord r) throws IOException {
        // add record to block
        if (block.isEmpty()) {
            makePrimaryKey(r, block.primaryKey);
        }
        for (int i = 0; i < info.colBlocks.length; i++) {
            r.exportColumnBlock(i, block.cellBlockBuffers[i]);
        }
        
        block.nRows++;
        if (block.nRows >= info.rowBlockSize || info.isRowBlockEnabled() == false) {
            flush();
        }
    }
    
    private void makePrimaryKey(GTRecord r, ByteArray buf) {
        r.exportColumns(info.primaryKey, buf);
    }

    @Override
    public void flush() throws IOException {
        writer.write(block);
        block.clear();
        block.seqId++;
    }

    @Override
    public void close() throws IOException {
        if (block.isEmpty() == false) {
            flush();
        }
        writer.close();
    }
}
