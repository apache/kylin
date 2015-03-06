package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface GTStore {
    
    public GTInfo getInfo();
    
    public String getStorageDescription();
    
    public GTBlockWriter rebuild(int shard);
    
    public GTBlockScanner scan(ByteBuffer pkStart, ByteBuffer pkEndExclusive, int[] colBlocks);
    
    public interface GTBlockWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    public interface GTBlockScanner extends Iterator<GTRowBlock>, Closeable {
    }
}
