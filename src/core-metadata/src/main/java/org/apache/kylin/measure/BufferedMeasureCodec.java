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

package org.apache.kylin.measure;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * This class embeds a reusable byte buffer for measure encoding, and is not thread-safe.
 * The buffer will grow to accommodate BufferOverflowException until a limit.
 * The problem here to solve is some measure type cannot provide accurate DataTypeSerializer.maxLength()
 */
@SuppressWarnings({ "unchecked" })
public class BufferedMeasureCodec implements java.io.Serializable {
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB
    public static final int MAX_BUFFER_SIZE = 1 * 1024 * DEFAULT_BUFFER_SIZE; // 1 GB

    final private MeasureCodec codec;

    private transient ByteBuffer buf;
    final private int[] measureSizes;

    public BufferedMeasureCodec(Collection<MeasureDesc> measureDescs) {
        this.codec = new MeasureCodec(measureDescs);
        this.measureSizes = new int[codec.getMeasuresCount()];
    }

    public BufferedMeasureCodec(MeasureDesc... measureDescs) {
        this.codec = new MeasureCodec(measureDescs);
        this.measureSizes = new int[codec.getMeasuresCount()];
    }

    public BufferedMeasureCodec(DataType... dataTypes) {
        this.codec = new MeasureCodec(dataTypes);
        this.measureSizes = new int[codec.getMeasuresCount()];
    }

    public BufferedMeasureCodec(String... dataTypes) {
        this.codec = new MeasureCodec(dataTypes);
        this.measureSizes = new int[codec.getMeasuresCount()];
    }

    /** return the buffer that contains result of last encoding */
    public ByteBuffer getBuffer() {
        return buf;
    }

    /** return the measure sizes of last encoding */
    public int[] getMeasureSizes() {
        return measureSizes;
    }

    public void setBufferSize(int size) {
        buf = null; // release memory for GC
        buf = ByteBuffer.allocate(size);
    }

    public void decode(ByteBuffer buf, Object[] result) {
        codec.decode(buf, result);
    }

    public ByteBuffer encode(Object[] values) {
        if (buf == null) {
            setBufferSize(DEFAULT_BUFFER_SIZE);
        }

        assert values.length == codec.getMeasuresCount();

        while (true) {
            try {
                buf.clear();
                for (int i = 0, pos = 0; i < codec.getMeasuresCount(); i++) {
                    codec.encode(i, values[i], buf);
                    measureSizes[i] = buf.position() - pos;
                    pos = buf.position();
                }
                return buf;

            } catch (BufferOverflowException boe) {
                if (buf.capacity() >= MAX_BUFFER_SIZE)
                    throw boe;

                setBufferSize(buf.capacity() * 2);
            }
        }
    }
}
