package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.TupleFilter;

public interface IGTStore {
    
    public GTInfo getInfo();
    
    public String getStorageDescription();
    
    public IGTStoreWriter rebuild(int shard);
    
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast);
    
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, TupleFilter filterPushDown);
    
    public interface IGTStoreWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    public interface IGTStoreScanner extends Iterator<GTRowBlock>, Closeable {
    }
    
}
