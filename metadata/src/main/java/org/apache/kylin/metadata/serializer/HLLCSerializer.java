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

package org.apache.kylin.metadata.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.DataType;

/**
 * @author yangli9
 * 
 */
public class HLLCSerializer extends DataTypeSerializer<HyperLogLogPlusCounter> {

    HyperLogLogPlusCounter current;

    public HLLCSerializer(DataType type) {
        current = new HyperLogLogPlusCounter(type.getPrecision());
    }

    @Override
    public void serialize(HyperLogLogPlusCounter value, ByteBuffer out) {
        try {
            value.writeRegisters(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HyperLogLogPlusCounter deserialize(ByteBuffer in) {
        try {
            current.readRegisters(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return current;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return current.peekLength(in);
    }
    
    @Override
    public int maxLength() {
        return current.maxLength();
    }

    @Override
    public HyperLogLogPlusCounter valueOf(byte[] value) {
        current.clear();
        if (value == null)
            current.add("__nUlL__");
        else
            current.add(value);
        return current;
    }

}
