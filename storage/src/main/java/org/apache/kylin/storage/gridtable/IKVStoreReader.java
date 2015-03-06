package org.apache.kylin.storage.gridtable;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hadoop.hbase.util.Pair;

public interface IKVStoreReader extends Iterator<Pair<ByteBuffer, ByteBuffer>>, Closeable {

}
