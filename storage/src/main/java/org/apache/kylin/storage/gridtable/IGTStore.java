package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.kylin.common.util.ImmutableBitSet;

public interface IGTStore {

    GTInfo getInfo();
    
    IGTStoreWriter rebuild(int shard) throws IOException;
    
    IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException;
    
    IGTStoreScanner scan(GTRecord pkStart, GTRecord pkEnd, ImmutableBitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException;

    interface IGTStoreWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    interface IGTStoreScanner extends Iterator<GTRowBlock>, Closeable {
    }
    
}
