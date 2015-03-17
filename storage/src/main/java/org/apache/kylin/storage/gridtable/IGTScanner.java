package org.apache.kylin.storage.gridtable;

import java.io.Closeable;

public interface IGTScanner extends Iterable<GTRecord>, Closeable {
    
    public int getScannedRowCount();
    
    public int getScannedRowBlockCount();

}
