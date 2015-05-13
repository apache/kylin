package org.apache.kylin.invertedindex.invertedindex;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

/**
 */
public class LZFTest {
    @Test
    public void test() throws IOException {

        byte[] raw = new byte[] { 1, 2, 3, 3, 2, 23 };
        byte[] data = LZFEncoder.encode(raw);

        byte[] data2 = new byte[data.length * 2];
        java.lang.System.arraycopy(data, 0, data2, 0, data.length);
        ImmutableBytesWritable bytes = new ImmutableBytesWritable();
        bytes.set(data2, 0, data.length);

        try {
            byte[] uncompressed = LZFDecoder.decode(bytes.get(), bytes.getOffset(), bytes.getLength());
        } catch (IOException e) {
            throw new RuntimeException("LZF decode failure", e);
        }
    }
}
