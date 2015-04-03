package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.kylin.common.util.ByteArray;

public interface IGTStore {
    
    public GTInfo getInfo();
    
    public String getStorageDescription();
    
    public IGTStoreWriter rebuild(int shard) throws IOException;
    
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException;
    
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException;
    
    public interface IGTStoreWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    public interface IGTStoreScanner extends Iterator<GTRowBlock>, Closeable {
    }
    
}
