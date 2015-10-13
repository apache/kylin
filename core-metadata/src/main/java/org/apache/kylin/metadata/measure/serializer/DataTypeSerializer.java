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

package org.apache.kylin.metadata.measure.serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.metadata.model.DataType;

import com.google.common.collect.Maps;

/**
 * @author yangli9
 * 
 */
abstract public class DataTypeSerializer<T> implements BytesSerializer<T> {

    final static Map<String, Class<?>> implementations;
    static {
        HashMap<String, Class<?>> impl = Maps.newHashMap();
        impl.put("varchar", StringSerializer.class);
        impl.put("decimal", BigDecimalSerializer.class);
        impl.put("double", DoubleSerializer.class);
        impl.put("float", DoubleSerializer.class);
        impl.put("bigint", LongSerializer.class);
        impl.put("long", LongSerializer.class);
        impl.put("integer", LongSerializer.class);
        impl.put("int", LongSerializer.class);
        impl.put("smallint", LongSerializer.class);
        impl.put("date", DateTimeSerializer.class);
        impl.put("datetime", DateTimeSerializer.class);
        impl.put("timestamp", DateTimeSerializer.class);
        implementations = Collections.unmodifiableMap(impl);

    }

    public static DataTypeSerializer<?> create(String dataType) {
        return create(DataType.getInstance(dataType));
    }

    public static DataTypeSerializer<?> create(DataType type) {
        if (type.isHLLC()) {
            return new HLLCSerializer(type);
        }

        if (type.isTopN()) {
            return new TopNCounterSerializer(type);
        }

        Class<?> clz = implementations.get(type.getName());
        if (clz == null)
            throw new RuntimeException("No MeasureSerializer for type " + type);

        try {
            return (DataTypeSerializer<?>) clz.getConstructor(DataType.class).newInstance(type);
        } catch (Exception e) {
            throw new RuntimeException(e); // never happen
        }
    }
    
    /** peek into buffer and return the length of serialization */
    abstract public int peekLength(ByteBuffer in);

    /** return the max number of bytes to the longest serialization */
    abstract public int maxLength();

    /** get an estimate of size in bytes of the serialized data */
    abstract public int getStorageBytesEstimate();

    /** convert from String to obj (string often come as byte[] in mapred) */
    abstract public T valueOf(byte[] value);

    /** convert from String to obj */
    public T valueOf(String value) {
        try {
            return valueOf(value.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
    }

    /** convert from obj to string */
    public String toString(T value) {
        if (value == null)
            return "NULL";
        else
            return value.toString();
    }
}
