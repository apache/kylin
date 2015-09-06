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

import java.util.HashMap;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.metadata.model.DataType;

/**
 * @author yangli9
 * 
 */
abstract public class MeasureSerializer<T> implements BytesSerializer<T> {

    final static HashMap<String, Class<?>> implementations = new HashMap<String, Class<?>>();
    static {
        implementations.put("decimal", BigDecimalSerializer.class);
        implementations.put("double", DoubleSerializer.class);
        implementations.put("float", DoubleSerializer.class);
        implementations.put("bigint", LongSerializer.class);
        implementations.put("long", LongSerializer.class);
        implementations.put("integer", LongSerializer.class);
        implementations.put("int", LongSerializer.class);
    }

    public static MeasureSerializer<?> create(String dataType) {
        DataType type = DataType.getInstance(dataType);
        if (type.isHLLC()) {
            return new HLLCSerializer(type.getPrecision());
        }

        Class<?> clz = implementations.get(type.getName());
        if (clz == null)
            throw new RuntimeException("No MeasureSerializer for type " + dataType);

        try {
            return (MeasureSerializer<?>) clz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e); // never happen
        }
    }

    abstract public T valueOf(byte[] value);

    public String toString(T value) {
        if (value == null)
            return "NULL";
        else
            return value.toString();
    }
}
