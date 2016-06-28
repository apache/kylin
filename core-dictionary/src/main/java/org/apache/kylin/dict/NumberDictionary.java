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

package org.apache.kylin.dict;

import org.apache.kylin.common.util.Bytes;

/**
 * @author yangli9
 * 
 */
@SuppressWarnings("serial")
public class NumberDictionary<T> extends TrieDictionary<T> {

    public static final int MAX_DIGITS_BEFORE_DECIMAL_POINT_LEGACY = 16;
    public static final int MAX_DIGITS_BEFORE_DECIMAL_POINT = 19;

    // encode a number into an order preserving byte sequence
    // for positives -- padding '0'
    // for negatives -- '-' sign, padding '9', invert digits, and terminate by ';'
    static class NumberBytesCodec {
        int maxDigitsBeforeDecimalPoint;
        byte[] buf;
        int bufOffset;
        int bufLen;

        NumberBytesCodec(int maxDigitsBeforeDecimalPoint) {
            this.maxDigitsBeforeDecimalPoint = maxDigitsBeforeDecimalPoint;
            this.buf = new byte[maxDigitsBeforeDecimalPoint * 3];
            this.bufOffset = 0;
            this.bufLen = 0;
        }

        void encodeNumber(byte[] value, int offset, int len) {
            if (len == 0) {
                bufOffset = 0;
                bufLen = 0;
                return;
            }

            if (len > buf.length) {
                throw new IllegalArgumentException("Too many digits for NumberDictionary: " + Bytes.toString(value, offset, len) + ". Internal buffer is only " + buf.length + " bytes");
            }

            boolean negative = value[offset] == '-';

            // terminate negative ';'
            int start = buf.length - len;
            int end = buf.length;
            if (negative) {
                start--;
                end--;
                buf[end] = ';';
            }

            // copy & find decimal point
            int decimalPoint = end;
            for (int i = start, j = offset; i < end; i++, j++) {
                buf[i] = value[j];
                if (buf[i] == '.' && i < decimalPoint) {
                    decimalPoint = i;
                }
            }
            // remove '-' sign
            if (negative) {
                start++;
            }

            // prepend '0'
            int nZeroPadding = maxDigitsBeforeDecimalPoint - (decimalPoint - start);
            if (nZeroPadding < 0 || nZeroPadding + 1 > start)
                throw new IllegalArgumentException("Too many digits for NumberDictionary: " + Bytes.toString(value, offset, len) + ". Expect " + maxDigitsBeforeDecimalPoint + " digits before decimal point at max.");
            for (int i = 0; i < nZeroPadding; i++) {
                buf[--start] = '0';
            }

            // consider negative
            if (negative) {
                buf[--start] = '-';
                for (int i = start + 1; i < buf.length; i++) {
                    int c = buf[i];
                    if (c >= '0' && c <= '9') {
                        buf[i] = (byte) ('9' - (c - '0'));
                    }
                }
            } else {
                buf[--start] = '0';
            }

            bufOffset = start;
            bufLen = buf.length - start;
        }

        int decodeNumber(byte[] returnValue, int offset) {
            if (bufLen == 0) {
                return 0;
            }

            int in = bufOffset;
            int end = bufOffset + bufLen;
            int out = offset;

            // sign
            boolean negative = buf[in] == '-';
            if (negative) {
                returnValue[out++] = '-';
                in++;
                end--;
            }

            // remove padding
            byte padding = (byte) (negative ? '9' : '0');
            for (; in < end; in++) {
                if (buf[in] != padding)
                    break;
            }

            // all paddings before '.', special case for '0'
            if (in == end || !(buf[in] >= '0' && buf[in] <= '9')) {
                returnValue[out++] = '0';
            }

            // copy the rest
            if (negative) {
                for (; in < end; in++, out++) {
                    int c = buf[in];
                    if (c >= '0' && c <= '9') {
                        c = '9' - (c - '0');
                    }
                    returnValue[out] = (byte) c;
                }
            } else {
                System.arraycopy(buf, in, returnValue, out, end - in);
                out += end - in;
            }

            return out - offset;
        }
    }

    static ThreadLocal<NumberBytesCodec> localCodec = new ThreadLocal<NumberBytesCodec>();

    // ============================================================================

    public NumberDictionary() { // default constructor for Writable interface
        super();
    }

    public NumberDictionary(byte[] trieBytes) {
        super(trieBytes);
    }

    protected NumberBytesCodec getCodec() {
        NumberBytesCodec codec = localCodec.get();
        if (codec == null) {
            codec = new NumberBytesCodec(MAX_DIGITS_BEFORE_DECIMAL_POINT_LEGACY);
            localCodec.set(codec);
        }
        return codec;
    }

    @Override
    protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        NumberBytesCodec codec = getCodec();
        codec.encodeNumber(value, offset, len);
        return super.getIdFromValueBytesImpl(codec.buf, codec.bufOffset, codec.bufLen, roundingFlag);
    }

    @Override
    protected boolean isNullObjectForm(T value) {
        return value == null || value.equals("");
    }

    @Override
    protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        NumberBytesCodec codec = getCodec();
        codec.bufOffset = 0;
        codec.bufLen = super.getValueBytesFromIdImpl(id, codec.buf, 0);
        return codec.decodeNumber(returnValue, offset);
    }

    @Override
    public void enableIdToValueBytesCache() {
        enableIdToValueBytesCache(new EnableIdToValueBytesCacheVisitor() {
            NumberBytesCodec codec = getCodec();
            byte[] tmp = new byte[getSizeOfValue()];

            @Override
            public byte[] getBuffer() {
                return codec.buf;
            }

            @Override
            public byte[] makeValueBytes(byte[] buf, int length) {
                // the given buf is the codec buf, which we returned in getBuffer()
                codec.bufOffset = 0;
                codec.bufLen = length;
                int numLen = codec.decodeNumber(tmp, 0);

                byte[] result = new byte[numLen];
                System.arraycopy(tmp, 0, result, 0, numLen);
                return result;
            }
        });
    }

    public static void main(String[] args) throws Exception {
        NumberDictionaryBuilder<String> b = new NumberDictionaryBuilder<String>(new StringBytesConverter());
        b.addValue("10");
        b.addValue("100");
        b.addValue("40");
        b.addValue("7");
        TrieDictionary<String> dict = b.build(0);

        dict.enableIdToValueBytesCache();
        for (int i = 0; i <= dict.getMaxId(); i++) {
            System.out.println(Bytes.toString(dict.getValueBytesFromId(i)));
        }
    }
}