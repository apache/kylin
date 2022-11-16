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

package org.apache.kylin.metadata.datatype;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.BytesSerializer;

import com.google.common.collect.Maps;

/**
 * Note: the implementations MUST be thread-safe.
 */
abstract public class DataTypeSerializer<T> implements BytesSerializer<T>, java.io.Serializable {

    final static Map<String, Class<?>> implementations = Maps.newHashMap();
    protected transient ThreadLocal current = new ThreadLocal();
    static {
        implementations.put("char", StringSerializer.class);
        implementations.put("varchar", StringSerializer.class);
        implementations.put("decimal", BigDecimalSerializer.class);
        implementations.put("double", DoubleSerializer.class);
        implementations.put("float", DoubleSerializer.class);
        implementations.put("bigint", LongSerializer.class);
        implementations.put("long", LongSerializer.class);
        implementations.put("integer", LongSerializer.class);
        implementations.put("int", LongSerializer.class);
        implementations.put("tinyint", LongSerializer.class);
        implementations.put("smallint", LongSerializer.class);
        implementations.put("int4", Int4Serializer.class);
        implementations.put("long8", Long8Serializer.class);
        implementations.put("boolean", BooleanSerializer.class);
        implementations.put("date", DateTimeSerializer.class);
        implementations.put("datetime", DateTimeSerializer.class);
        implementations.put("timestamp", DateTimeSerializer.class);
    }

    public static void register(String dataTypeName, Class<? extends DataTypeSerializer<?>> impl) {
        implementations.put(dataTypeName, impl);
    }

    public static DataTypeSerializer<?> create(String dataType) {
        return create(DataType.getType(dataType));
    }

    public static DataTypeSerializer<?> create(DataType type) {
        Class<?> clz = implementations.get(type.getName());
        if (clz == null)
            throw new RuntimeException("No DataTypeSerializer for type " + type);

        try {
            return (DataTypeSerializer<?>) clz.getConstructor(DataType.class).newInstance(type);
        } catch (Exception e) {
            throw new RuntimeException(e); // never happen
        }
    }

    /** Peek into buffer and return the length of serialization which is previously written by this.serialize().
     *  The current position of input buffer is guaranteed to be at the beginning of the serialization.
     *  The implementation must not alter the buffer position by its return. */
    abstract public int peekLength(ByteBuffer in);

    /** Return the max number of bytes to the longest possible serialization */
    abstract public int maxLength();

    /** Get an estimate of the average size in bytes of this kind of serialized data */
    abstract public int getStorageBytesEstimate();

    protected double getStorageBytesEstimate(double count) {
        return 0;
    }

    /** An optional convenient method that converts a string to this data type (for dimensions) */
    public T valueOf(String str) {
        throw new UnsupportedOperationException();
    }

    /** If the query is exactAggregation and has some memory hungry measures,
     * we could directly return final result to speed up the query.
     * If the DataTypeSerializer support this,
     * which should override the getFinalResult method, besides that, the deserialize and peekLength method should also support it, like {@link org.apache.kylin.measure.bitmap.BitmapSerializer} */
    public boolean supportDirectReturnResult() {
        return false;
    }

    /** An optional method that converts a expensive buffer to lightweight buffer containing final result (for memory hungry measures) */
    public ByteBuffer getFinalResult(ByteBuffer in) {
        throw new UnsupportedOperationException();
    }

    /** Convert from obj to string */
    public String toString(T value) {
        if (value == null)
            return "NULL";
        else
            return value.toString();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        current = new ThreadLocal();
    }
}
