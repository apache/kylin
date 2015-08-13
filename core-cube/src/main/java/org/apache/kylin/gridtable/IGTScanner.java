package org.apache.kylin.gridtable;

import java.io.Closeable;

public interface IGTScanner extends Iterable<GTRecord>, Closeable {

    GTInfo getInfo();

    int getScannedRowCount();

    int getScannedRowBlockCount();

}
