package org.apache.kylin.gridtable;

import java.io.IOException;

public interface IGTStore {

    GTInfo getInfo();

    IGTWriter rebuild(int shard) throws IOException;

    IGTWriter append(int shard) throws IOException;

    IGTScanner scan(GTScanRequest scanRequest) throws IOException;

}
