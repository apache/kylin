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

package org.apache.kylin.measure.extendedcolumn;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class ExtendedColumnSerializer extends DataTypeSerializer<ByteArray> {

    private int extendedColumnSize;
    private int maxLength;

    public ExtendedColumnSerializer(DataType dataType) {
        this.extendedColumnSize = dataType.getPrecision();
        this.maxLength = this.extendedColumnSize + 4;//4 bytes for the length preamble
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int size = BytesUtil.readVInt(in);
        int total = in.position() - mark;
        if (size >= 0) {
            //size <0 is the null case
            total += size;
        }
        in.position(mark);
        return total;
    }

    @Override
    public int maxLength() {
        return maxLength;
    }

    @Override
    public int getStorageBytesEstimate() {
        return extendedColumnSize / 2;
    }

    @Override
    public void serialize(ByteArray value, ByteBuffer out) {
        if (value != null && value.array() != null) {
            BytesUtil.writeByteArray(value.array(), value.offset(), value.length(), out);
        } else {
            BytesUtil.writeByteArray(null, out);
        }
    }

    @Override
    public ByteArray deserialize(ByteBuffer in) {
        //the array in ByteArray is garanteed to be completed owned by the ByteArray
        return new ByteArray(BytesUtil.readByteArray(in));
    }
}
