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

package org.apache.kylin.measure.bitmap;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BitmapSerializerTest extends LocalFileMetadataTestCase {
    @BeforeAll
    static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterAll
    static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    void testBitmapSerDe() {
        BitmapSerializer serializer = new BitmapSerializer(DataType.ANY);

        BitmapCounter counter = RoaringBitmapCounterFactory.INSTANCE.newBitmap(1, 1234, 5678, 100000);

        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        serializer.serialize(counter, buffer);
        int size = buffer.position();
        buffer.flip();

        assertEquals(size, serializer.peekLength(buffer));
        assertEquals(0, buffer.position()); // peek doesn't change buffer

        BitmapCounter counter2 = serializer.deserialize(buffer);
        assertEquals(size, buffer.position()); // deserialize advance positions to next record
        assertEquals(4, counter2.getCount());

        buffer.flip();
        for (int i = 0; i < size; i++) {
            buffer.put((byte) 0); // clear buffer content
        }
        assertEquals(4, counter2.getCount());

        buffer = ByteBuffer.allocate(size - 1);
        try {
            serializer.serialize(counter, buffer);
            Assertions.fail();
        } catch (Exception e) {
            assertTrue(e instanceof BufferOverflowException);
        }
    }
}