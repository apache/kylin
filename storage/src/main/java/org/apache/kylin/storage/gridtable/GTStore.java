package org.apache.kylin.storage.gridtable;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface GTStore {
    
    public GTInfo getInfo();
    
    public String getStorageDescription();
    
    // ============================================================================
    
    public GTWriter rebuild(int shard);
    
    public GTScanner scan(ByteBuffer pkStart, ByteBuffer pkEndExclusive, ConciseSet selectedRowBlcoks, int[] selectedColBlocks);
    
    public interface GTWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    public interface GTScanner extends Iterator<GTRowBlock>, Closeable {
    }
    
    // ============================================================================
    
    public void saveRowBlockIndex(int col, GTRowBlockIndex index);
    
    public GTRowBlockIndex loadRowBlockIndex(int col);
    
}
