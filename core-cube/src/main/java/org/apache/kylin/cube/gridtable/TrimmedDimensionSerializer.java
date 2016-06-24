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

package org.apache.kylin.cube.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class TrimmedDimensionSerializer extends DataTypeSerializer<Object> {

    final int fixedLen;

    public TrimmedDimensionSerializer(int fixedLen) {
        this.fixedLen = fixedLen;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return fixedLen;
    }

    @Override
    public int maxLength() {
        return fixedLen;
    }

    @Override
    public int getStorageBytesEstimate() {
        return fixedLen;
    }

    @Override
    public void serialize(Object value, ByteBuffer out) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object deserialize(ByteBuffer in) {
        throw new UnsupportedOperationException();
    }
}