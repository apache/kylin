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
package org.apache.kylin.metadata.measure.fixedlen;

import java.nio.ByteBuffer;

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.DataType;

/**
 * Created by Hongbin Ma(Binmahone) on 2/10/15.
 */
public class FixedHLLCodec extends FixedLenMeasureCodec<HyperLogLogPlusCounter> {

    private DataType type;
    private int presision;
    private HyperLogLogPlusCounter current;

    public FixedHLLCodec(DataType type) {
        this.type = type;
        this.presision = type.getPrecision();
        this.current = new HyperLogLogPlusCounter(this.presision);
    }

    @Override
    public int getLength() {
        return 1 << presision;
    }

    @Override
    public DataType getDataType() {
        return type;
    }

    @Override
    public HyperLogLogPlusCounter valueOf(String value) {
        current.clear();
        if (value == null)
            current.add("__nUlL__");
        else
            current.add(value.getBytes());
        return current;
    }

    @Override
    public Object getValue() {
        return current;
    }

    @Override
    public HyperLogLogPlusCounter read(byte[] buf, int offset) {
        current.readRegistersArray(ByteBuffer.wrap(buf, offset, buf.length - offset));
        return current;
    }

    @Override
    public void write(HyperLogLogPlusCounter v, byte[] buf, int offset) {
        v.writeRegistersArray(ByteBuffer.wrap(buf, offset, buf.length - offset));
    }
}
