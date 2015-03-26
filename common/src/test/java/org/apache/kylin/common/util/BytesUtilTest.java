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

import junit.framework.TestCase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

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

    @Test
    public void testBooleanArray() {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        boolean[] x = new boolean[] { true, false, true };
        BytesUtil.writeBooleanArray(x, buffer);
        buffer.flip();
        boolean[] y = BytesUtil.readBooleanArray(buffer);
        assertEquals(y[2], true);
        assertEquals(y[1], false);
    }

    @Test
    public void testWriteReadUnsignedInt() {

        int testInt = 735033;
        ByteArray ba = new ByteArray(new byte[3]);
        BytesUtil.writeUnsigned(testInt, 3, ba.asBuffer());

        byte[] newBytes = new byte[3];
        System.arraycopy(ba.array(), 0, newBytes, 0, 3);
        int value = BytesUtil.readUnsigned(new ByteArray(newBytes).asBuffer(), 3);

        assertEquals(value, testInt);

        byte[] anOtherNewBytes = new byte[3];
        BytesUtil.writeUnsigned(testInt, anOtherNewBytes, 0, 3);

        assertTrue(Arrays.equals(anOtherNewBytes, ba.array()));
    }

}
