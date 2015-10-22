package org.apache.kylin.gridtable;

import java.io.IOException;

public interface IGTStore {

    GTInfo getInfo();

    IGTWriter rebuild() throws IOException;

    IGTWriter append() throws IOException;

    IGTScanner scan(GTScanRequest scanRequest) throws IOException;

}
