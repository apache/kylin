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

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedLenDimEnc extends DimensionEncoding implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(FixedLenDimEnc.class);

    // row key fixed length place holder
    public static final byte ROWKEY_PLACE_HOLDER_BYTE = 9;

    public static final String ENCODING_NAME = "fixed_length";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new FixedLenDimEnc(Integer.parseInt(args[0]));
        }
    }

    // ============================================================================

    private int fixedLen;

    private transient int avoidVerbose = 0;

    //no-arg constructor is required for Externalizable
    public FixedLenDimEnc() {
    }

    public FixedLenDimEnc(int len) {
        this.fixedLen = len;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FixedLenDimEnc that = (FixedLenDimEnc) o;

        return fixedLen == that.fixedLen;

    }

    @Override
    public int hashCode() {
        return fixedLen;
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    @Override
    public void encode(String valueStr, byte[] output, int outputOffset) {
        if (valueStr == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);
            return;
        }

        byte[] value = Bytes.toBytes(valueStr);
        int valueLen = value.length;
        if (valueLen > fixedLen) {
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn(
                        "Expect at most " + fixedLen + " bytes, but got " + valueLen + ", will truncate, value string: "
                                + Bytes.toString(value, 0, valueLen) + " times:" + avoidVerbose);
            }
        }

        int n = Math.min(valueLen, fixedLen);
        System.arraycopy(value, 0, output, outputOffset, n);

        if (n < fixedLen) {
            Arrays.fill(output, outputOffset + n, outputOffset + fixedLen, ROWKEY_PLACE_HOLDER_BYTE);
        }
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }

        while (len > 0 && bytes[offset + len - 1] == ROWKEY_PLACE_HOLDER_BYTE)
            len--;

        return Bytes.toString(bytes, offset, len);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new FixedLenSerializer();
    }

    public class FixedLenSerializer extends DataTypeSerializer<Object> {

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
            String str = value == null ? null : value.toString();
            encode(str, buf, 0);
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
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(fixedLen);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fixedLen = in.readShort();
    }

}
