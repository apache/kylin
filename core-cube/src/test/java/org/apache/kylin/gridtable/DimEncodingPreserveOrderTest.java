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

import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.apache.kylin.dimension.OneMoreByteVLongDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

public class DimEncodingPreserveOrderTest {
    @Test
    public void testOneMoreByteVLongDimEncPreserveOrder() {
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
        IntegerDimEnc enc = new IntegerDimEnc(2);
        List<ByteArray> encodedValues = Lists.newArrayList();
        encodedValues.add(encode(enc, -32767L));
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
    public void testVLongDimEncPreserveOrder2() {
        IntegerDimEnc enc = new IntegerDimEnc(8);
        List<ByteArray> encodedValues = Lists.newArrayList();
        encodedValues.add(encode(enc, -Long.MAX_VALUE));
        encodedValues.add(encode(enc, -10000L));
        encodedValues.add(encode(enc, -100L));
        encodedValues.add(encode(enc, 0L));
        encodedValues.add(encode(enc, 100L));
        encodedValues.add(encode(enc, 10000L));
        encodedValues.add(encode(enc, Long.MAX_VALUE));
        encodedValues.add(encode(enc, null));

        assertTrue(Ordering.from(new DefaultGTComparator()).isOrdered(encodedValues));
    }

    private ByteArray encode(DimensionEncoding enc, Object value) {
        if (value != null) {
            byte[] buf = new byte[enc.getLengthOfEncoding()];

            String valueStr = "" + value;
            byte[] bytes = Bytes.toBytes(valueStr);

            enc.encode(bytes, bytes.length, buf, 0);
            return new ByteArray(buf);
        } else {
            byte[] buf = new byte[enc.getLengthOfEncoding()];
            enc.encode(null, 0, buf, 0);
            return new ByteArray(buf);
        }
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
