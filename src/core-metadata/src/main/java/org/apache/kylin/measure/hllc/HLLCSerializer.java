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

package org.apache.kylin.measure.hllc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * @author yangli9
 *
 */
public class HLLCSerializer extends DataTypeSerializer<HLLCounter> {

    private int precision;

    public HLLCSerializer(DataType type) {
        this.precision = type.getPrecision();
    }

    public HLLCSerializer(int precision) {
        this.precision = precision;
    }

    @Override
    public void serialize(HLLCounter value, ByteBuffer out) {
        try {
            value.writeRegisters(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HLLCounter current() {
        HLLCounter hllc = (HLLCounter) current.get();
        if (hllc == null) {
            hllc = new HLLCounter(precision);
            current.set(hllc);
        }
        return hllc;
    }

    @Override
    public HLLCounter deserialize(ByteBuffer in) {
        HLLCounter hllc = new HLLCounter(precision);
        try {
            hllc.readRegisters(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hllc;
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
        return current().maxLength();
    }

}
