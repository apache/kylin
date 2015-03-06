package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kylin.storage.gridtable.GTStore.GTBlockWriter;

public class GTBuilder implements Closeable, Flushable {

    final private GTInfo info;
    final private GTBlockWriter writer;
    
    private GTRowBlock block;

    public GTBuilder(GTInfo info, int shard, GTStore store) {
        this.info = info;
        this.writer = store.rebuild(shard);
        this.block = new GTRowBlock(info);
    }

    public void write(GTRecord r) throws IOException {
        // add record to block
        if (block.isEmpty()) {
            makePrimaryKey(r, block.primaryKey);
        }
        for (int c = 0; c < info.nColBlocks; c++) {
            ByteBuffer cellBuf = block.cellBlocks[c];
            for (int i = info.colBlockCuts[c], end = info.colBlockCuts[c + 1]; i < end; i++) {
                append(cellBuf, r.cols[i]);
            }
        }
        
        block.nRows++;
        if (block.nRows >= info.rowBlockSize || info.isRowBlockEnabled() == false) {
            flush();
        }
    }
    
    private void makePrimaryKey(GTRecord r, ByteBuffer buf) {
        buf.clear();
        
        for (int i : info.primaryKey) {
            append(buf, r.cols[i]);
        }
        
        buf.flip();
    }

    private void append(ByteBuffer buf, ByteBuffer data) {
        buf.put(data.array(), data.arrayOffset(), data.limit());
    }

    @Override
    public void flush() throws IOException {
        writer.write(block);
        block.clear();
    }

    @Override
    public void close() throws IOException {
        if (block.isEmpty() == false) {
            flush();
        }
        writer.close();
    }
}
