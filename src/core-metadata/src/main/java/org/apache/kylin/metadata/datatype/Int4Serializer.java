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

import org.apache.kylin.common.util.BytesUtil;

/**
 */
public class Int4Serializer extends DataTypeSerializer<Integer> {

    public Int4Serializer(DataType type) {
    }

    @Override
    public void serialize(Integer value, ByteBuffer out) {
        BytesUtil.writeUnsigned(value, 4, out);
    }

    @Override
    public Integer deserialize(ByteBuffer in) {
        return BytesUtil.readUnsigned(in, 4);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return 4;
    }

    @Override
    public int maxLength() {
        return 4;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 4;
    }

    @Override
    public Integer valueOf(String str) {
        return Integer.parseInt(str);
    }
}
