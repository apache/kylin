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

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Test;

public class GTTwoLayerAggregateParamTest {

    @Test
    public void testSerializeEmpty() {
        GTTwoLayerAggregateParam twoLayerAggParam = new GTTwoLayerAggregateParam();

        ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        GTTwoLayerAggregateParam.serializer.serialize(twoLayerAggParam, buffer);
        buffer.flip();
        GTTwoLayerAggregateParam aTwoLayerAggParam = GTTwoLayerAggregateParam.serializer.deserialize(buffer);

        Assert.assertEquals(twoLayerAggParam.vanishDimMask, aTwoLayerAggParam.vanishDimMask);
    }

    @Test
    public void testSerialize() {
        ImmutableBitSet vanishDimMask = ImmutableBitSet.valueOf(0);
        ImmutableBitSet outsideLayerMetrics = ImmutableBitSet.valueOf(41);
        String[] outsideLayerMetricsFuncs = new String[] { "STDDEV_SUM" };
        int[] insideLayerMetrics = new int[] { 26 };
        String[] insideLayerMetricsFuncs = new String[] { "SUM" };
        GTTwoLayerAggregateParam twoLayerAggParam = new GTTwoLayerAggregateParam(vanishDimMask, outsideLayerMetrics,
                outsideLayerMetricsFuncs, insideLayerMetrics, insideLayerMetricsFuncs);

        ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        GTTwoLayerAggregateParam.serializer.serialize(twoLayerAggParam, buffer);
        buffer.flip();
        GTTwoLayerAggregateParam aTwoLayerAggParam = GTTwoLayerAggregateParam.serializer.deserialize(buffer);

        Assert.assertEquals(twoLayerAggParam.vanishDimMask, aTwoLayerAggParam.vanishDimMask);
    }
}