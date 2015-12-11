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

import java.math.BigDecimal;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;

public class FixedPointLongCodec extends FixedLenMeasureCodec<LongWritable> {

    private static final int SIZE = 8;
    // number of digits after decimal point
    int scale;
    BigDecimal scalePower;
    DataType type;
    // avoid mass object creation
    LongWritable current = new LongWritable();

    public FixedPointLongCodec(DataType type) {
        this.type = type;
        this.scale = Math.max(0, type.getScale());
        this.scalePower = new BigDecimal((long) Math.pow(10, scale));
    }

    @Override
    public int getLength() {
        return SIZE;
    }

    @Override
    public DataType getDataType() {
        return type;
    }

    @Override
    public LongWritable valueOf(String value) {
        if (value == null)
            current.set(0L);
        else
            current.set((new BigDecimal(value).multiply(scalePower).longValue()));
        return current;
    }

    @Override
    public String getValue() {
        if (scale == 0)
            return current.toString();
        else
            return "" + (new BigDecimal(current.get()).divide(scalePower));
    }

    @Override
    public LongWritable read(byte[] buf, int offset) {
        current.set(BytesUtil.readLong(buf, offset, SIZE));
        return current;
    }

    @Override
    public void write(LongWritable v, byte[] buf, int offset) {
        BytesUtil.writeLong(v == null ? 0 : v.get(), buf, offset, SIZE);
    }
}
