/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class BytesUtilTest {
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
        Assert.assertEquals(y[2], 3);
    }

    @Test
    public void testBooleanArray() {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        boolean[] x = new boolean[] { true, false, true };
        BytesUtil.writeBooleanArray(x, buffer);
        buffer.flip();
        boolean[] y = BytesUtil.readBooleanArray(buffer);
        Assert.assertEquals(y[2], true);
        Assert.assertEquals(y[1], false);
    }

    @Test
    public void testWriteReadUnsignedInt() {
        testWriteReadUnsignedInt(735033, 3);
        testWriteReadUnsignedInt(73503300, 4);
    }

    public void testWriteReadUnsignedInt(int testInt, int length) {
        ByteArray ba = new ByteArray(new byte[length]);
        BytesUtil.writeUnsigned(testInt, length, ba.asBuffer());

        byte[] newBytes = new byte[length];
        System.arraycopy(ba.array(), 0, newBytes, 0, length);
        int value = BytesUtil.readUnsigned(new ByteArray(newBytes).asBuffer(), length);

        Assert.assertEquals(value, testInt);

        byte[] anOtherNewBytes = new byte[length];
        BytesUtil.writeUnsigned(testInt, anOtherNewBytes, 0, length);

        Assert.assertTrue(Arrays.equals(anOtherNewBytes, ba.array()));
    }

    @Test
    public void testReadable() {
        String x = "\\x00\\x00\\x00\\x00\\x00\\x01\\xFC\\xA8";
        byte[] bytes = BytesUtil.fromReadableText(x);
        String y = BytesUtil.toHex(bytes);
        Assert.assertEquals(x, y);
    }

}
