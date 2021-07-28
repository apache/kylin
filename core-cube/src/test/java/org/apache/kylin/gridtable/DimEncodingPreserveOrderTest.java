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

package org.apache.kylin.gridtable;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.dimension.OneMoreByteVLongDimEnc;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Ordering;

@Ignore
public class DimEncodingPreserveOrderTest {
    private static List<long[]> successValue;
    private static List<long[]> failValue;

    @BeforeClass
    public static void initTestValue() {
        successValue = new ArrayList<>();
        successValue.add(new long[] { -127, 0, 127 });
        successValue.add(new long[] { -32767, -127, 0, 127, 32767 });
        successValue.add(new long[] { -8388607, -32767, -127, 0, 127, 32767, 8388607 });
        successValue.add(new long[] { -2147483647L, -8388607, -32767, -127, 0, 127, 32767, 8388607, 2147483647L });
        successValue.add(new long[] { -549755813887L, -2147483647L, -8388607, -32767, -127, 0, 127, 32767, 8388607, 2147483647L, 549755813887L });
        successValue.add(new long[] { -140737488355327L, -549755813887L, -2147483647L, -8388607, -32767, -127, 0, 127, 32767, 8388607, 2147483647L, 549755813887L, 140737488355327L });
        successValue.add(new long[] { -36028797018963967L, -140737488355327L, -549755813887L, -2147483647L, -8388607, -32767, -127, 0, 127, 32767, 8388607, 2147483647L, 549755813887L, 140737488355327L, 36028797018963967L });
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
    public void testOneMoreByteVLongDimEncPreserveOrder() {
        // TODO: better test
        OneMoreByteVLongDimEnc enc = new OneMoreByteVLongDimEnc(2);
        List<ByteArray> encodedValues = Lists.newArrayList();
        encodedValues.add(encode(enc, -32768L));
        encodedValues.add(encode(enc, -10000L));
        encodedValues.add(encode(enc, -100L));
        encodedValues.add(encode(enc, 0L));
        encodedValues.add(encode(enc, 100L));
        encodedValues.add(encode(enc, 10000L));
        encodedValues.add(encode(enc, 32767L));
        encodedValues.add(encode(enc, null));

        assertTrue(Ordering.from(new DefaultGTComparator()).isOrdered(encodedValues));
    }

    @Test
    public void testVLongDimEncPreserveOrder() {
        for (int i = 1; i <= successValue.size(); i++) {
            IntegerDimEnc enc = new IntegerDimEnc(i);
            List<ByteArray> encodedValues = Lists.newArrayList();
            for (long value : successValue.get(i - 1)) {
                encodedValues.add(encode(enc, value));
            }
            encodedValues.add(encode(enc, null));
            assertTrue(Ordering.from(new DefaultGTComparator()).isOrdered(encodedValues));
        }
    }

    private ByteArray encode(DimensionEncoding enc, Object value) {
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        String valueStr = value == null ? null : value.toString();
        enc.encode(valueStr, buf, 0);
        return new ByteArray(buf);
    }

    @Test
    public void testFixedLengthHexDimEncPreserveOrder() {
        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(4);
        List<ByteArray> encodedValues = Lists.newArrayList();
        encodedValues.add(encode(enc, "0000"));
        encodedValues.add(encode(enc, "0001"));
        encodedValues.add(encode(enc, "FFF0"));
        encodedValues.add(encode(enc, null));

        assertTrue(Ordering.from(new DefaultGTComparator()).isOrdered(encodedValues));
    }

}
