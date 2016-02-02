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

package org.apache.kylin.metadata.measure.fixedlen;

import org.apache.kylin.metadata.model.DataType;
import org.junit.Test;

/**
 */
public class FixedPointLongCodecTest {

    @Test
    public void testEncode1() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12.12345");
        org.junit.Assert.assertEquals(1212345, x);
    }

    @Test
    public void testEncode2() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12.1234");
        org.junit.Assert.assertEquals(1212340, x);
    }

    @Test
    public void testEncode3() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12.123456");
        org.junit.Assert.assertEquals(1212345, x);
    }

    @Test
    public void testEncode4() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12");
        org.junit.Assert.assertEquals(1200000, x);
    }

    @Test
    public void testDecode1() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        String x = codec.restoreDecimalPoint(1212345);
        org.junit.Assert.assertEquals("12.12345", x);
    }
}
