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

package org.apache.kylin.dimension;

import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FixedLenHexDimEncTest {

    @Test
    void testConstructor() {
        try {
            new FixedLenHexDimEnc(0);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expect
        }

        new FixedLenHexDimEnc(8);
    }

    @Test
    void testNull() {
        for (int i = 1; i < 9; i++) {
            FixedLenHexDimEnc enc = new FixedLenHexDimEnc(i);

            byte[] buf = new byte[enc.getLengthOfEncoding()];
            enc.encode(null, buf, 0);
            Assertions.assertTrue(DimensionEncoding.isNull(buf, 0, buf.length));
            String decode = enc.decode(buf, 0, buf.length);
            Assertions.assertEquals(null, decode);

            buf = new byte[enc.getLengthOfEncoding()];
            DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
            ser.serialize(null, ByteBuffer.wrap(buf));
            Assertions.assertTrue(DimensionEncoding.isNull(buf, 0, buf.length));
            decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
            Assertions.assertEquals(null, decode);
        }
    }

    @Test
    void testEncodeDecode() {
        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(4);
        testEncodeDecode(enc, "AF12");
        testEncodeDecode(enc, "0000");
        testEncodeDecode(enc, "FFF0");
        try {
            testEncodeDecode(enc, "abcd");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <abcd> but was: <ABCD>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFF");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFFF> but was: <null>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFF");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFF> but was: <FFF0>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFF0");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFFF0> but was: <null>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFF10");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFF10> but was: <FFF1>", e.getMessage());
        }
    }


    @Test
    void testEncodeDecode2() {
        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(5);
        testEncodeDecode(enc, "AF121");
        testEncodeDecode(enc, "00000");
        
        //with a little extra room all F is supported
        testEncodeDecode(enc, "FFFFF");
        
        try {
            testEncodeDecode(enc, "FFF");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFF> but was: <FFF00>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFFF0");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFFFF0> but was: <FFFFF>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFF10");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFFF10> but was: <FFFF1>", e.getMessage());
        }

    }

    private void testEncodeDecode(FixedLenHexDimEnc enc, String value) {
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        String valueStr = value;
        enc.encode(valueStr, buf, 0);
        String decode = enc.decode(buf, 0, buf.length);
        Assertions.assertEquals(valueStr, decode);
    }

    @Test
    void testSerDes() {

        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(4);
        testSerDes(enc, "AF12");
        testSerDes(enc, "0000");
        testSerDes(enc, "FFF0");
        try {
            testSerDes(enc, "FFFF");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFFF> but was: <null>", e.getMessage());
        }
        try {
            testSerDes(enc, "FFF");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFF> but was: <FFF0>", e.getMessage());
        }
        try {
            testSerDes(enc, "FFFF0");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFFF0> but was: <null>", e.getMessage());
        }
        try {
            testSerDes(enc, "FFF10");
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertEquals("expected: <FFF10> but was: <FFF1>", e.getMessage());
        }
    }

    private void testSerDes(FixedLenHexDimEnc enc, String value) {
        DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        String valueStr = value;
        ser.serialize(valueStr, ByteBuffer.wrap(buf));
        String decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
        Assertions.assertEquals(valueStr, decode);
    }

}
