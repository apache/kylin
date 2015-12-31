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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.BytesSerializer;

import com.google.common.collect.Maps;

/**
 * Note: the implementations MUST be thread-safe.
 */
abstract public class DataTypeSerializer<T> implements BytesSerializer<T> {

    final static Map<String, Class<?>> implementations = Maps.newHashMap();
    static {
        implementations.put("varchar", StringSerializer.class);
        implementations.put("decimal", BigDecimalSerializer.class);
        implementations.put("double", DoubleSerializer.class);
        implementations.put("float", DoubleSerializer.class);
        implementations.put("bigint", LongSerializer.class);
        implementations.put("long", LongSerializer.class);
        implementations.put("integer", LongSerializer.class);
        implementations.put("int", LongSerializer.class);
        implementations.put("smallint", LongSerializer.class);
        implementations.put("date", DateTimeSerializer.class);
        implementations.put("datetime", DateTimeSerializer.class);
        implementations.put("timestamp", DateTimeSerializer.class);
    }

    public static void register(String dataTypeName, Class<? extends DataTypeSerializer<?>> impl) {
        implementations.put(dataTypeName, impl);
    }

    public static DataTypeSerializer<?> create(String dataType) {
        return create(DataType.getInstance(dataType));
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

    /** Get an estimate of size in bytes of the serialized data */
    abstract public int getStorageBytesEstimate();

    /** An optional convenient method that converts a string to this data type (for dimensions) */
    public T valueOf(String str) {
        throw new UnsupportedOperationException();
    }

    /** Convert from obj to string */
    public String toString(T value) {
        if (value == null)
            return "NULL";
        else
            return value.toString();
    }
}
