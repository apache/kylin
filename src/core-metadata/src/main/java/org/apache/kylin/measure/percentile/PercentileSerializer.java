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

package org.apache.kylin.measure.percentile;

import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class PercentileSerializer extends DataTypeSerializer<PercentileCounter> {
    // be thread-safe and avoid repeated obj creation
    private transient ThreadLocal<PercentileCounter> current = null;

    private double compression;

    public PercentileSerializer(DataType type) {
        this.compression = type.getPrecision();
    }

    public PercentileSerializer(int precision) {
        this.compression = precision;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return current().peekLength(in);
    }

    @Override
    public int maxLength() {
        return current().maxLength();
    }

    @Override
    public int getStorageBytesEstimate() {
        return current().getBytesEstimate();
    }

    protected double getStorageBytesEstimate(double count) {
        return current().getBytesEstimate(count);
    }

    private PercentileCounter current() {
        if (current == null) {
            current = new ThreadLocal<>();
        }

        PercentileCounter counter = current.get();
        if (counter == null) {
            counter = new PercentileCounter(compression);
            current.set(counter);
        }
        return counter;
    }

    @Override
    public void serialize(PercentileCounter value, ByteBuffer out) {
        value.writeRegisters(out);
    }

    @Override
    public PercentileCounter deserialize(ByteBuffer in) {
        PercentileCounter counter = new PercentileCounter(compression);
        counter.readRegisters(in);
        return counter;
    }
}
