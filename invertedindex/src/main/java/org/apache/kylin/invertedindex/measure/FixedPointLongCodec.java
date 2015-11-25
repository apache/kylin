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

package org.apache.kylin.invertedindex.measure;

import java.nio.ByteBuffer;

import org.apache.kylin.common.datatype.DataType;
import org.apache.kylin.common.datatype.LongMutable;
import org.apache.kylin.common.util.BytesUtil;

public class FixedPointLongCodec extends FixedLenMeasureCodec<LongMutable> {

    private static final int SIZE = 8;
    // number of digits after decimal point
    int scale;
    DataType type;
    // avoid massive object creation
    LongMutable current = new LongMutable();

    public FixedPointLongCodec(DataType type) {
        this.type = type;
        this.scale = Math.max(0, type.getScale());
    }

    @Override
    public int getLength() {
        return SIZE;
    }

    @Override
    public DataType getDataType() {
        return type;
    }

    long getValueIgnoringDecimalPoint(String value) {
        int index = value.indexOf('.');

        if (index == 0 || index == value.length() - 1) {
            throw new RuntimeException("Bad decimal format: " + value);
        } else if (index < 0) {
            return Long.valueOf(value) * (int) Math.pow(10, scale);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(value.substring(0, index));

            //if there are more than scale digits after the decimal point, the tail will be discarded
            int end = Math.min(value.length(), index + scale + 1);
            sb.append(value.substring(index + 1, end));
            int diff = index + scale + 1 - value.length();
            //if there are less than scale digits after the decimal point, the tail will be compensated
            for (int i = 0; i < diff; i++) {
                sb.append('0');
            }
            return Long.valueOf(sb.toString());
        }
    }

    String restoreDecimalPoint(long value) {
        if (scale < 0) {
            throw new RuntimeException("Bad scale: " + scale + " with value: " + value);
        } else if (scale == 0) {
            return Long.toString(value);
        } else {
            return String.format("%." + scale + "f", value / (Math.pow(10, scale)));
        }
    }

    @Override
    public LongMutable valueOf(String value) {
        if (value == null)
            current.set(0L);
        else
            current.set(getValueIgnoringDecimalPoint(value));
        return current;
    }

    @Override
    public String getValue() {
        if (scale == 0)
            return current.toString();
        else
            return restoreDecimalPoint(current.get());
    }

    @Override
    public LongMutable read(byte[] buf, int offset) {
        current.set(BytesUtil.readLong(buf, offset, SIZE));
        return current;
    }

    @Override
    public void write(LongMutable v, byte[] buf, int offset) {
        BytesUtil.writeLong(v == null ? 0 : v.get(), buf, offset, SIZE);
    }

    @Override
    public LongMutable read(ByteBuffer buffer) {
        current.set(BytesUtil.readLong(buffer, SIZE));
        return current;
    }
}
