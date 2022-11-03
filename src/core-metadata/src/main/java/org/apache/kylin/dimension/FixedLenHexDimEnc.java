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

import com.google.common.base.Preconditions;

/**
 * used to store hex values like "1A2BFF"
 * <p>
 * <p>
 * limitations: (take FixedLenHexDimEnc(2) as example: )
 * <p>
 * 1. "FFFF" will become null encode and decode
 * 2. "AB" will become "AB00"
 *
 * <p>
 * Due to these limitations hex representation of hash values(with no padding, better with even characters) is more suitable
 */
public class FixedLenHexDimEnc extends DimensionEncoding implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(FixedLenHexDimEnc.class);

    public static byte[] dict = new byte[256];
    public static byte[] revdict = new byte[16];

    static {
        for (int i = 0; i < dict.length; i++) {
            dict[i] = -1;
        }
        dict['0'] = 0;
        dict['1'] = 1;
        dict['2'] = 2;
        dict['3'] = 3;
        dict['4'] = 4;
        dict['5'] = 5;
        dict['6'] = 6;
        dict['7'] = 7;
        dict['8'] = 8;
        dict['9'] = 9;
        dict['A'] = 10;
        dict['B'] = 11;
        dict['C'] = 12;
        dict['D'] = 13;
        dict['E'] = 14;
        dict['F'] = 15;
        dict['a'] = 10;
        dict['b'] = 11;
        dict['c'] = 12;
        dict['d'] = 13;
        dict['e'] = 14;
        dict['f'] = 15;

        revdict[0] = '0';
        revdict[1] = '1';
        revdict[2] = '2';
        revdict[3] = '3';
        revdict[4] = '4';
        revdict[5] = '5';
        revdict[6] = '6';
        revdict[7] = '7';
        revdict[8] = '8';
        revdict[9] = '9';
        revdict[10] = 'A';
        revdict[11] = 'B';
        revdict[12] = 'C';
        revdict[13] = 'D';
        revdict[14] = 'E';
        revdict[15] = 'F';
    }

    // row key fixed length place holder
    public static final byte ROWKEY_PLACE_HOLDER_BYTE = 0;

    public static final String ENCODING_NAME = "fixed_length_hex";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new FixedLenHexDimEnc(Integer.parseInt(args[0]));
        }
    }

    // ============================================================================

    private int hexLength;
    private int bytelen;

    transient private int avoidVerbose = 0;
    transient private int avoidVerbose2 = 0;

    //no-arg constructor is required for Externalizable
    public FixedLenHexDimEnc() {
    }

    public FixedLenHexDimEnc(int len) {
        if (len < 1) {
            throw new IllegalArgumentException("len has to be positive: " + len);
        }
        this.hexLength = len;
        this.bytelen = (hexLength + 1) / 2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FixedLenHexDimEnc that = (FixedLenHexDimEnc) o;

        return hexLength == that.hexLength;
    }

    @Override
    public int hashCode() {
        return hexLength;
    }

    @Override
    public int getLengthOfEncoding() {
        return bytelen;
    }

    @Override
    public void encode(String valueStr, byte[] output, int outputOffset) {
        if (valueStr == null) {
            Arrays.fill(output, outputOffset, outputOffset + bytelen, NULL);
            return;
        }

        byte[] value = Bytes.toBytes(valueStr);
        int valueLen = value.length;
        int endOffset = outputOffset + bytelen;

        if (valueLen > hexLength) {
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + hexLength + " bytes, but got " + valueLen
                        + ", will truncate, value string: " + Bytes.toString(value, 0, valueLen) + " times:"
                        + avoidVerbose);
            }
        }

        if (valueLen >= hexLength && isF(value, 0, hexLength)) {
            if (avoidVerbose2++ % 10000 == 0) {
                logger.warn("All 'F' value: " + Bytes.toString(value, 0, valueLen)
                        + "will become null after encode/decode. times:" + avoidVerbose);
            }
        }

        int n = Math.min(valueLen, hexLength);
        for (int i = 0; i < n; i += 2) {
            byte temp = 0;
            byte iCode = dict[value[i]];
            temp |= (iCode << 4);

            int j = i + 1;
            if (j < n) {
                byte jCode = dict[value[j]];
                temp |= jCode;
            }

            output[outputOffset++] = temp;
        }

        Arrays.fill(output, outputOffset, endOffset, ROWKEY_PLACE_HOLDER_BYTE);
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        Preconditions.checkArgument(len == bytelen, "len " + len + " not equals " + bytelen);

        if (isNull(bytes, offset, len)) {
            return null;
        }

        byte[] ret = new byte[hexLength];
        for (int i = 0; i < ret.length; i += 2) {
            byte temp = bytes[i / 2];
            ret[i] = revdict[(temp & 0xF0) >>> 4];

            int j = i + 1;
            if (j < hexLength) {
                ret[j] = revdict[temp & 0x0F];
            }
        }

        return Bytes.toString(ret, 0, ret.length);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new FixedLenSerializer();
    }

    public class FixedLenSerializer extends DataTypeSerializer<Object> {

        private byte[] currentBuf() {
            byte[] buf = (byte[]) current.get();
            if (buf == null) {
                buf = new byte[bytelen];
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
            return bytelen;
        }

        @Override
        public int maxLength() {
            return bytelen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return bytelen;
        }

        @Override
        public Object valueOf(String str) {
            return str;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(hexLength);
        out.writeShort(bytelen);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        hexLength = in.readShort();
        bytelen = in.readShort();
    }

    private boolean isF(byte[] value, int offset, int length) {
        for (int i = offset; i < length + offset; ++i) {
            if (value[i] != 'F') {
                return false;
            }
        }
        return true;
    }

}
