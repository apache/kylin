package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.kylin.common.util.ByteArray;

public interface IGTStore {

    GTInfo getInfo();
    
    IGTStoreWriter rebuild(int shard) throws IOException;
    
    IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException;
    
    IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException;

    interface IGTStoreWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }
    
    interface IGTStoreScanner extends Iterator<GTRowBlock>, Closeable {
    }
    
}
