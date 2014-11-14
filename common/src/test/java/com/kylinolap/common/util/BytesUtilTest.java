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
        BytesUtil.writeIntArray(x,buffer);
        buffer.flip();
        by

    }


}