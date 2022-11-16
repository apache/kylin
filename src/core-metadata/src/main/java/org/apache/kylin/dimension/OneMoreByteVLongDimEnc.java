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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * not being used yet, prepared for future
 */
public class OneMoreByteVLongDimEnc extends DimensionEncoding implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(OneMoreByteVLongDimEnc.class);

    private static final long[] CAP = { 0, 0x7fL, 0x7fffL, 0x7fffffL, 0x7fffffffL, 0x7fffffffffL, 0x7fffffffffffL,
            0x7fffffffffffffL, 0x7fffffffffffffffL };
    private static final long[] MASK = { 0, 0xffL, 0xffffL, 0xffffffL, 0xffffffffL, 0xffffffffffL, 0xffffffffffffL,
            0xffffffffffffffL, 0xffffffffffffffffL };
    private static final long[] TAIL = { 0, 0x80L, 0x8000L, 0x800000L, 0x80000000L, 0x8000000000L, 0x800000000000L,
            0x80000000000000L, 0x8000000000000000L };
    static {
        for (int i = 1; i < TAIL.length; ++i) {
            long head = ~MASK[i];
            TAIL[i] = head | TAIL[i];
        }
    }
    public static final String ENCODING_NAME = "one_more_byte_vlong";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new OneMoreByteVLongDimEnc(Integer.parseInt(args[0]));
        }
    };

    // ============================================================================

    private int fixedLen;
    private int byteLen;

    transient private int avoidVerbose = 0;

    //no-arg constructor is required for Externalizable
    public OneMoreByteVLongDimEnc() {
    }

    public OneMoreByteVLongDimEnc(int len) {
        if (len <= 0 || len >= CAP.length)
            throw new IllegalArgumentException();

        this.fixedLen = len;
        this.byteLen = fixedLen + 1;//one additional byte to indicate null
    }

    @Override
    public int getLengthOfEncoding() {
        return byteLen;
    }

    @Override
    public void encode(String valueStr, byte[] output, int outputOffset) {
        if (valueStr == null) {
            Arrays.fill(output, outputOffset, outputOffset + byteLen, NULL);
            return;
        }

        long integer = Long.parseLong(valueStr);
        if (integer > CAP[fixedLen] || integer < TAIL[fixedLen]) {
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + fixedLen + " bytes, but got " + valueStr + ", will truncate, hit times:"
                        + avoidVerbose);
            }
        }

        BytesUtil.writeByte(integer >= 0 ? (byte) 1 : (byte) 0, output, outputOffset, 1);
        BytesUtil.writeSignedLong(integer, output, outputOffset + 1, fixedLen);
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }

        long integer = BytesUtil.readSignedLong(bytes, offset + 1, len - 1);
        return String.valueOf(integer);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new VLongSerializer();
    }

    public class VLongSerializer extends DataTypeSerializer<Object> {

        private byte[] currentBuf() {
            byte[] buf = (byte[]) current.get();
            if (buf == null) {
                buf = new byte[byteLen];
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
            return byteLen;
        }

        @Override
        public int maxLength() {
            return byteLen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return byteLen;
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        OneMoreByteVLongDimEnc that = (OneMoreByteVLongDimEnc) o;

        return fixedLen == that.fixedLen;

    }

    @Override
    public int hashCode() {
        return fixedLen;
    }
}
