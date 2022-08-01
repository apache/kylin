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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IntegerDimEncTest {

    private static List<long[]> successValue;
    private static List<long[]> failValue;

    @BeforeClass
    public static void initTestValue() {
        successValue = new ArrayList<>();
        successValue.add(new long[] { -127, 0, 127 });
        successValue.add(new long[] { -32767, -127, 0, 127, 32767 });
        successValue.add(new long[] { -8388607, -32767, -127, 0, 127, 32767, 8388607 });
        successValue.add(new long[] { -2147483647L, -8388607, -32767, -127, 0, 127, 32767, 8388607, 2147483647L });
        successValue.add(new long[] { -549755813887L, -2147483647L, -8388607, -32767, -127, 0, 127, 32767, 8388607,
                2147483647L, 549755813887L });
        successValue.add(new long[] { -140737488355327L, -549755813887L, -2147483647L, -8388607, -32767, -127, 0, 127,
                32767, 8388607, 2147483647L, 549755813887L, 140737488355327L });
        successValue.add(new long[] { -36028797018963967L, -140737488355327L, -549755813887L, -2147483647L, -8388607,
                -32767, -127, 0, 127, 32767, 8388607, 2147483647L, 549755813887L, 140737488355327L,
                36028797018963967L });
        successValue.add(new long[] { //
                -9223372036854775807L, //
                -36028797018963967L, //
                -140737488355327L, //
                -549755813887L, //
                -2147483647L, //
                -8388607, //
                -32767, //
                -127, //
                0, //
                127, // (2 ^ 7) - 1
                32767, // (2 ^ 15)  - 1
                8388607, // (2 ^ 23) - 1
                2147483647L, // (2 ^ 31) - 1
                549755813887L, // (2 ^ 39) - 1
                140737488355327L, // (2 ^ 47) - 1
                36028797018963967L, // (2 ^ 55) - 1
                9223372036854775807L }); // (2 ^ 63) - 1

        failValue = new ArrayList<>();
        failValue.add(new long[] { -128, 128 });
        failValue.add(new long[] { -32768, 32768 });
        failValue.add(new long[] { -8388608, 8388608 });
        failValue.add(new long[] { -2147483648L, 2147483648L });
        failValue.add(new long[] { -549755813888L, 549755813888L });
        failValue.add(new long[] { -140737488355328L, 140737488355328L });
        failValue.add(new long[] { -36028797018963968L, 36028797018963968L });
        failValue.add(new long[] { -9223372036854775808L });
    }

    @Test
    public void testConstructor() {
        try {
            new IntegerDimEnc(0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expect
        }
        try {
            new IntegerDimEnc(9);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expect
        }
        new IntegerDimEnc(8);
    }

    @Test
    public void testNull() {
        for (int i = 1; i < 9; i++) {
            IntegerDimEnc enc = new IntegerDimEnc(i);

            byte[] buf = new byte[enc.getLengthOfEncoding()];
            enc.encode(null, buf, 0);
            Assert.assertTrue(DimensionEncoding.isNull(buf, 0, buf.length));
            String decode = enc.decode(buf, 0, buf.length);
            Assert.assertEquals(null, decode);

            buf = new byte[enc.getLengthOfEncoding()];
            DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
            ser.serialize(null, ByteBuffer.wrap(buf));
            Assert.assertTrue(DimensionEncoding.isNull(buf, 0, buf.length));
            decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
            Assert.assertEquals(null, decode);
        }
    }

    @Test
    public void testEncodeDecode() {
        for (int i = 1; i <= successValue.size(); i++) {
            IntegerDimEnc enc = new IntegerDimEnc(i);
            for (long value : successValue.get(i - 1)) {
                testEncodeDecode(enc, value);
            }

            for (long value : failValue.get(i - 1)) {
                try {
                    testEncodeDecode(enc, value);
                    Assert.fail();
                } catch (Throwable e) {
                    Assert.assertEquals("expected:<" + value + "> but was:<null>", e.getMessage());
                }
            }
        }
    }

    private void testEncodeDecode(IntegerDimEnc enc, long value) {
        String valueStr = "" + value;
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        enc.encode(valueStr, buf, 0);
        String decode = enc.decode(buf, 0, buf.length);
        Assert.assertEquals(valueStr, decode);
    }

    @Test
    public void testSerDes() {
        for (int i = 1; i <= successValue.size(); i++) {
            IntegerDimEnc enc = new IntegerDimEnc(i);

            testSerDes(enc, 127);
            for (long value : successValue.get(i - 1)) {
                testSerDes(enc, value);
            }
            for (long value : failValue.get(i - 1)) {
                try {
                    testSerDes(enc, value);
                    Assert.fail();
                } catch (Throwable e) {
                    Assert.assertEquals("expected:<" + value + "> but was:<null>", e.getMessage());
                }
            }
        }
    }

    private void testSerDes(IntegerDimEnc enc, long value) {
        DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        String valueStr = "" + value;
        ser.serialize(valueStr, ByteBuffer.wrap(buf));
        String decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
        Assert.assertEquals(valueStr, decode);
    }
}
