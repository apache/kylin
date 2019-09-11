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

package org.apache.kylin.metadata.datatype;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class SerializerTest extends LocalFileMetadataTestCase {

    private static BigDecimalSerializer bigDecimalSerializer;
    private static LongSerializer longSerializer;
    private static DoubleSerializer doubleSerializer;

    private ByteBuffer buffer;

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @BeforeClass
    public static void beforeClass() {
        staticCreateTestMetadata();
        bigDecimalSerializer = new BigDecimalSerializer(DataType.getType("decimal"));
        longSerializer = new LongSerializer(DataType.getType("long"));
        doubleSerializer = new DoubleSerializer(DataType.getType("double"));
    }

    @Before
    public void setup() {
        buffer = ByteBuffer.allocate(256);
    }

    @After
    public void clean() {
        buffer.clear();
    }

    @Test
    public void testScaleOutOfRange() {
        BigDecimal input = new BigDecimal("1234.1234567890");
        buffer.mark();
        bigDecimalSerializer.serialize(input, buffer);
        buffer.reset();
        BigDecimal output = bigDecimalSerializer.deserialize(buffer);
        assertEquals(input.setScale(bigDecimalSerializer.type.getScale(), BigDecimal.ROUND_HALF_EVEN), output);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOutOfPrecision() {
        BigDecimal input = new BigDecimal("66855344214907231736.4924");
        bigDecimalSerializer.serialize(input, buffer);
    }

    @Test
    public void testNull() {
        //BigDecimal NULL
        buffer.mark();
        bigDecimalSerializer.serialize(null, buffer);
        buffer.reset();
        assertEquals(null, bigDecimalSerializer.deserialize(buffer));

        //Long NULL
        buffer.mark();
        longSerializer.serialize(null, buffer);
        buffer.reset();
        assertEquals(null, longSerializer.deserialize(buffer));

        //Double NULL
        buffer.mark();
        doubleSerializer.serialize(null, buffer);
        buffer.reset();
        assertEquals(null, doubleSerializer.deserialize(buffer));
    }

    @Test
    public void testNormal() {
        // Long
        testNumberSerialization(longSerializer, 1024L);
        testNumberSerialization(longSerializer, -1024L);
        testNumberSerialization(longSerializer, 0L);
        testNumberSerialization(longSerializer, Long.MAX_VALUE);
        testNumberSerialization(longSerializer, Long.MIN_VALUE);

        //Double
        testNumberSerialization(doubleSerializer, 1234.56d);
        testNumberSerialization(doubleSerializer, -1234.56d);
        testNumberSerialization(doubleSerializer, 0.0);
        testNumberSerialization(doubleSerializer, Double.MAX_VALUE);
        testNumberSerialization(doubleSerializer, -Double.MAX_VALUE);
        testNumberSerialization(doubleSerializer, Double.MIN_VALUE);
        testNumberSerialization(doubleSerializer, -Double.MAX_VALUE);

        //Bigdecimal
        testNumberSerialization(bigDecimalSerializer, new BigDecimal(0));
        testNumberSerialization(bigDecimalSerializer, new BigDecimal(123456789.0987654321 * 123456789.0987654321));
        testNumberSerialization(bigDecimalSerializer, new BigDecimal(-123456789.0987654321 * 123456789.0987654321));
    }

    private <T> void testNumberSerialization(DataTypeSerializer<T> serializer, T number) {
        buffer.mark();
        serializer.serialize(number, buffer);
        buffer.reset();
        if (number instanceof Double)
            assertEquals(((Double) number), (Double) serializer.deserialize(buffer), 0.0000001);
        else
            assertEquals(number, serializer.deserialize(buffer));
    }
}
