package com.kylinolap.common.util;

import junit.framework.TestCase;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * by honma
 */
public class BytesUtilTest extends TestCase {
    @Test
    public void test() {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        int[] x = new int[] { 1, 2, 3 };
        BytesUtil.writeIntArray(x, buffer);
        buffer.flip();

        byte[] buf = new byte[buffer.limit()];
        System.arraycopy(buffer.array(), 0, buf, 0, buffer.limit());

        ByteBuffer newBuffer = ByteBuffer.wrap(buf);
        int[] y = BytesUtil.readIntArray(newBuffer);
        assertEquals(y[2], 3);
    }


}