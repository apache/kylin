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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by sunyerui on 15/12/31.
 */
public class BitmapSerializerTest extends LocalFileMetadataTestCase {
    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testSerDeCounter() {
        BitmapCounter counter = new BitmapCounter();
        counter.add(1);
        counter.add(3333);
        counter.add("123".getBytes());
        counter.add(123);
        assertEquals(3, counter.getCount());

        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024 * 1024);
        BitmapSerializer serializer = new BitmapSerializer(DataType.ANY);
        serializer.serialize(counter, buffer);
        int len = buffer.position();

        buffer.position(0);
        BitmapSerializer deSerializer = new BitmapSerializer(DataType.ANY);
        BitmapCounter counter2 = deSerializer.deserialize(buffer);
        assertEquals(3, counter2.getCount());

        buffer.position(0);
        assertEquals(len, deSerializer.peekLength(buffer));
        assertEquals(8 * 1024 * 1024, deSerializer.maxLength());
        System.out.println("counter size " + deSerializer.getStorageBytesEstimate());
    }
}