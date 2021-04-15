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

package org.apache.kylin.engine.flink.util;

import java.nio.ByteBuffer;

import org.apache.kylin.measure.percentile.PercentileCounter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A customized kryo serializer for {@link PercentileCounter}
 */
public class PercentileCounterSerializer extends Serializer<PercentileCounter> {

    @Override
    public void write(Kryo kryo, Output output, PercentileCounter counter) {
        int length = counter.getRegisters().byteSize();
        ByteBuffer buffer = ByteBuffer.allocate(length);
        counter.getRegisters().asSmallBytes(buffer);
        output.writeDouble(counter.getCompression());
        output.writeDouble(counter.getQuantileRatio());
        output.writeInt(buffer.position());
        output.write(buffer.array(), 0, buffer.position());
    }

    @Override
    public PercentileCounter read(Kryo kryo, Input input, Class type) {
        double compression = input.readDouble();
        double quantileRatio = input.readDouble();
        int length = input.readInt();
        byte[] buffer = new byte[length];

        int offset = 0;
        int bytesRead;
        while ((bytesRead = input.read(buffer, offset, buffer.length - offset)) != -1) {
            offset += bytesRead;
            if (offset >= buffer.length) {
                break;
            }
        }

        PercentileCounter counter = new PercentileCounter(compression, quantileRatio);
        counter.readRegisters(ByteBuffer.wrap(buffer));
        return counter;
    }
}
