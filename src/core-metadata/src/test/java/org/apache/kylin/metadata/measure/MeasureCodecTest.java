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

package org.apache.kylin.metadata.measure;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class MeasureCodecTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void basicTest() {
        MeasureDesc[] descs = new MeasureDesc[] { measure("double"), measure("long"), measure("decimal"),
                measure("HLLC16"), measure("bitmap") };
        BufferedMeasureCodec codec = new BufferedMeasureCodec(descs);

        Double d = new Double(1.0);
        Long l = new Long(2);
        BigDecimal b = new BigDecimal("333.1234");
        HLLCounter hllc = new HLLCounter(16);
        hllc.add("1234567");
        hllc.add("abcdefg");
        BitmapCounter bitmap = RoaringBitmapCounterFactory.INSTANCE.newBitmap();
        bitmap.add(123);
        bitmap.add(45678);
        bitmap.add(Integer.MAX_VALUE - 10);
        Object[] values = new Object[] { d, l, b, hllc, bitmap };

        ByteBuffer buf = codec.encode(values);
        buf.flip();
        System.out.println("size: " + buf.limit());

        Object[] copy = new Object[values.length];

        codec.decode(buf, copy);

        for (int i = 0; i < values.length; i++) {
            Object x = values[i];
            Object y = copy[i];
            assertEquals(x, y);
        }
    }

    private MeasureDesc measure(String returnType) {
        MeasureDesc desc = new MeasureDesc();
        FunctionDesc func = FunctionDesc.newInstance(null, null, returnType);
        desc.setFunction(func);
        return desc;
    }
}
