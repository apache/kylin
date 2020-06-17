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

package org.apache.kylin.engine.mr;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ByteArrayWritableTest {

    @Test
    public void basicTest() {
        ByteArrayWritable byteArrayWritable = new ByteArrayWritable(5);
        Assert.assertEquals(5, byteArrayWritable.length());
        Assert.assertNotEquals(0, byteArrayWritable.hashCode());
        byteArrayWritable.set(new byte[] { 0x1, 0x2, 0x3 });

        Assert.assertArrayEquals(new byte[] { 1, 2, 3 }, byteArrayWritable.array());
        Assert.assertEquals(0, byteArrayWritable.offset());
        Assert.assertEquals(3, byteArrayWritable.length());

        Assert.assertEquals("01 02 03", byteArrayWritable.toString());

        ByteArrayWritable byteArrayWritableNull = new ByteArrayWritable(null);
        Assert.assertEquals(0, byteArrayWritableNull.hashCode());
    }

    @Test
    public void testCompare() {
        ByteArrayWritable b1 = new ByteArrayWritable(new byte[] { 0x1, 0x2, 0x3 });
        ByteArrayWritable b2 = new ByteArrayWritable();

        Assert.assertFalse(b1.equals(1));

        b2.set(new byte[] { 0x1, 0x2, 0x3 });

        Assert.assertTrue(b1.equals(b2));
        Assert.assertTrue(b1.equals(new byte[] { 1, 2, 3 }));
    }

    @Test()
    public void testIO() throws IOException {
        ByteArrayWritable byteArrayWritable = new ByteArrayWritable(new byte[] { 0x1, 0x2, 0x3 });
        ByteArrayWritable byteArrayWritableNull = new ByteArrayWritable(null);
        ByteArrayWritable byteArrayWritableSlice = new ByteArrayWritable(new byte[] { 0x1, 0x2, 0x3 }, 1, 2);

        byteArrayWritable.asBuffer();
        byteArrayWritableNull.asBuffer();
        byteArrayWritableSlice.asBuffer();

        OutputStream outputStream = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
            }
        };
        DataOutput output = new DataOutputStream(outputStream);
        byteArrayWritable.write(output);

        InputStream inputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        };
        DataInput input = new DataInputStream(inputStream);
        byteArrayWritableNull.readFields(input);
    }
}