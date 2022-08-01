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

package org.apache.kylin.dimension;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class AbstractDateDimEnc extends DimensionEncoding {
    private static final long serialVersionUID = 1L;

    interface IValueCodec extends Serializable {
        long valueToCode(String value);

        String codeToValue(long code);
    }

    // ============================================================================
    private int fixedLen;
    private IValueCodec codec;

    public AbstractDateDimEnc() {
    }

    protected AbstractDateDimEnc(int fixedLen, IValueCodec codec) {
        this.fixedLen = fixedLen;
        this.codec = codec;
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    @Override
    public void encode(String value, byte[] output, int outputOffset) {
        if (value == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);
            return;
        }

        long code = codec.valueToCode(value);
        BytesUtil.writeLong(code, output, outputOffset, fixedLen);
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }

        long code = BytesUtil.readLong(bytes, offset, fixedLen);
        if (code < 0)
            throw new IllegalArgumentException();

        return codec.codeToValue(code);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new DataTypeSerializer<Object>() {

            private byte[] currentBuf() {
                byte[] buf = (byte[]) current.get();
                if (buf == null) {
                    buf = new byte[fixedLen];
                    current.set(buf);
                }
                return buf;
            }

            @Override
            public void serialize(Object value, ByteBuffer out) {
                byte[] buf = currentBuf();
                String valueStr = value == null ? null : value.toString();
                encode(valueStr, buf, 0);
                out.put(buf);
            }

            @Override
            public Object deserialize(ByteBuffer in) {
                byte[] buf = currentBuf();
                in.get(buf);
                return decode(buf, 0, buf.length);
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
            public Object valueOf(String str) {
                return str;
            }
        };
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(fixedLen);
        out.writeObject(codec);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.fixedLen = in.readInt();
        this.codec = (IValueCodec) in.readObject();
    }

}
