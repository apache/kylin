package org.apache.kylin.gridtable;

import java.io.Closeable;
import java.io.IOException;

public interface IGTWriter extends Closeable {

    void write(GTRecord rec) throws IOException;
}
