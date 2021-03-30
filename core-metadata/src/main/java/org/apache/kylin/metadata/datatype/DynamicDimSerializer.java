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

/**
 * For dynamic dimensions, the code length must be fixed
 */
public class DynamicDimSerializer<T> extends DataTypeSerializer<T> {

    private final DataTypeSerializer<T> dimDataTypeSerializer;

    public DynamicDimSerializer(DataTypeSerializer<T> dimDataTypeSerializer) {
        this.dimDataTypeSerializer = dimDataTypeSerializer;
    }

    public void serialize(T value, ByteBuffer out) {
        dimDataTypeSerializer.serialize(value, out);
    }

    public T deserialize(ByteBuffer in) {
        return dimDataTypeSerializer.deserialize(in);
    }

    public int peekLength(ByteBuffer in) {
        return maxLength();
    }

    public int maxLength() {
        return dimDataTypeSerializer.maxLength();
    }

    public int getStorageBytesEstimate() {
        return dimDataTypeSerializer.getStorageBytesEstimate();
    }

    /** An optional convenient method that converts a string to this data type (for dimensions) */
    public T valueOf(String str) {
        return dimDataTypeSerializer.valueOf(str);
    }

    /** Convert from obj to string */
    public String toString(T value) {
        return dimDataTypeSerializer.toString(value);
    }
}
