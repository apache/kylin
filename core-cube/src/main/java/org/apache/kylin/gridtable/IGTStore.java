package org.apache.kylin.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface IGTStore {

    GTInfo getInfo();

    IGTStoreWriter rebuild(int shard) throws IOException;

    IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException;

    IGTStoreScanner scan(GTScanRequest scanRequest) throws IOException;

    interface IGTStoreWriter extends Closeable {
        void write(GTRowBlock block) throws IOException;
    }

    interface IGTStoreScanner extends Iterator<GTRowBlock>, Closeable {
    }

}
