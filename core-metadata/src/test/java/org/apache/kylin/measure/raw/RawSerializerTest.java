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

package org.apache.kylin.measure.raw;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RawSerializerTest extends LocalFileMetadataTestCase {
    private static RawSerializer rawSerializer;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();

        DataType.register("raw");
        rawSerializer = new RawSerializer(DataType.getType("raw"));
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testPeekLength() {
        ByteBuffer out = ByteBuffer.allocate(1024 * 1024 * 128);
        int size = 127;
        List<ByteArray> input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 1, rawSerializer.peekLength(out));

        size = 128;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 2, rawSerializer.peekLength(out));

        size = 255;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 2, rawSerializer.peekLength(out));

        size = 256;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 3, rawSerializer.peekLength(out));

        size = 1024 * 63;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 3, rawSerializer.peekLength(out));

        size = 1024 * 64;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 4, rawSerializer.peekLength(out));
    }

    @Test
    public void testNormal() {
        List<ByteArray> input = getValueList(1024);
        List<ByteArray> output = doSAndD(input);
        assertEquals(input, output);
    }

    @Test
    public void testNull() {
        List<ByteArray> output = doSAndD(null);
        assertEquals(output.size(), 0);
        List<ByteArray> input = new ArrayList<ByteArray>();
        output = doSAndD(input);
        assertEquals(input, output);
    }

    @Test(expected = RuntimeException.class)
    public void testOverflow() {
        List<ByteArray> input = getValueList(512 * 1024);
        doSAndD(input);
    }

    private List<ByteArray> doSAndD(List<ByteArray> input) {
        ByteBuffer out = ByteBuffer.allocate(rawSerializer.maxLength());
        out.mark();
        rawSerializer.serialize(input, out);
        out.reset();
        return rawSerializer.deserialize(out);
    }

    private List<ByteArray> getValueList(int size) {
        if (size == -1) {
            return null;
        }
        List<ByteArray> valueList = new ArrayList<ByteArray>(size);
        for (Integer i = 0; i < size; i++) {
            ByteArray key = new ByteArray(1);
            BytesUtil.writeUnsigned(i, key.array(), 0, key.length());
            valueList.add(key);
        }
        return valueList;
    }

}
