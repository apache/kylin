package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface IKVStoreWriter extends Closeable {

    void write(ByteBuffer key, ByteBuffer value) throws IOException;
    
}
