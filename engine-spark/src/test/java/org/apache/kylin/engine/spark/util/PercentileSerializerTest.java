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
 *
 */

package org.apache.kylin.engine.spark.util;

import org.apache.kylin.measure.percentile.PercentileCounter;
import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PercentileSerializerTest {

    @Test
    public void testSerialization() {
        Kryo kryo = new Kryo();
        kryo.register(PercentileCounter.class, new PercentileCounterSerializer());
        double compression = 100;
        double quantile = 0.8;
        PercentileCounter origin_counter = new PercentileCounter(compression, quantile);
        for (int i = 1; i < 10; i++) {
            origin_counter.add(i);
        }
        byte[] buffer = serialize(kryo, origin_counter);
        PercentileCounter deserialized_counter = deserialize(kryo, buffer, PercentileCounter.class);
        Assert.assertEquals("Compression Error", origin_counter.getCompression(), deserialized_counter.getCompression(),
                0.00000001);
        Assert.assertEquals("QuantileRatio Error", origin_counter.getQuantileRatio(),
                deserialized_counter.getQuantileRatio(), 0.00000001);
        Assert.assertEquals("Estimation Error", origin_counter.getResultEstimate(),
                deserialized_counter.getResultEstimate(), 0.00000001);
    }

    public static <T> T deserialize(final Kryo kryo, final byte[] in, final Class<T> clazz) {
        final Input input = new Input(in);
        return kryo.readObject(input, clazz);
    }

    public static byte[] serialize(final Kryo kryo, final Object o) {
        if (o == null) {
            throw new NullPointerException("Can't serialize null");
        }
        final Output output = new Output(4096);
        kryo.writeObject(output, o);
        output.flush();
        return output.getBuffer();
    }

}
