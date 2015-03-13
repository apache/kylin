package org.apache.kylin.storage.gridtable;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.kylin.common.util.ByteArray;

public interface IGTStore {
    
    public GTInfo getInfo();
    
    public String getStorageDescription();
    
    // ============================================================================
    
    public IGTStoreWriter rebuild(int shard);
    
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEndExclusive, ConciseSet selectedRowBlocks, BitSet selectedColBlocks);
    
    public interface IGTStoreWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    public interface IGTStoreScanner extends Iterator<GTRowBlock>, Closeable {
    }
    
    // ============================================================================
    
    public void saveRowBlockIndex(int col, GTRowBlockIndex index);
    
    public GTRowBlockIndex loadRowBlockIndex(int col);
    
}
