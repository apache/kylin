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

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.apache.kylin.common.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Created by xiefan on 17-1-20.
 */
public class Number2BytesConverter implements BytesConverter<String>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int MAX_DIGITS_BEFORE_DECIMAL_POINT_LEGACY = 16;

    public static final int MAX_DIGITS_BEFORE_DECIMAL_POINT = 19;

    int maxDigitsBeforeDecimalPoint;

    static final transient InternalThreadLocal<Map<Integer, NumberBytesCodec>> LOCAL = new InternalThreadLocal<Map<Integer, NumberBytesCodec>>();

    static NumberBytesCodec getCodec(int maxDigitsBeforeDecimalPoint) {
        Map<Integer, NumberBytesCodec> codecMap = LOCAL.get();
        if (codecMap == null) {
            codecMap = Maps.newHashMap();
            LOCAL.set(codecMap);
        }
        NumberBytesCodec codec = codecMap.get(maxDigitsBeforeDecimalPoint);
        if (codec == null) {
            codec = new NumberBytesCodec(maxDigitsBeforeDecimalPoint);
            codecMap.put(maxDigitsBeforeDecimalPoint, codec);
        }
        return codec;
    }

    public Number2BytesConverter(){
        this.maxDigitsBeforeDecimalPoint = MAX_DIGITS_BEFORE_DECIMAL_POINT;
    }

    public Number2BytesConverter(int maxDigitsBeforeDecimalPoint) {
        this.maxDigitsBeforeDecimalPoint = maxDigitsBeforeDecimalPoint;
    }

    public void setMaxDigitsBeforeDecimalPoint(int maxDigitsBeforeDecimalPoint) {
        this.maxDigitsBeforeDecimalPoint = maxDigitsBeforeDecimalPoint;
    }

    @Override
    public byte[] convertToBytes(String v) {
        v = normalizeNumber(v);
        NumberBytesCodec codec = getCodec(this.maxDigitsBeforeDecimalPoint);
        byte[] num = Bytes.toBytes(v);
        codec.encodeNumber(num, 0, num.length);
        return Bytes.copy(codec.buf, codec.bufOffset, codec.bufLen);
    }

    public static String normalizeNumber(String v) {
        boolean badBegin = (v.startsWith("0") && v.length() > 1 && v.charAt(1) != '.') //
                || (v.startsWith("-0") && v.length() > 2 && v.charAt(2) != '.') //
                || v.startsWith("+");
        if (badBegin) {
            v = new BigDecimal(v).toPlainString();
        }
        
        while (v.contains(".") && (v.endsWith("0") || v.endsWith("."))) {
            v = v.substring(0, v.length() - 1);
        }
        
        return v;
    }

    @Override
    public String convertFromBytes(byte[] b, int offset, int length) {
        NumberBytesCodec codec = getCodec(this.maxDigitsBeforeDecimalPoint);
        byte[] backup = codec.buf;
        codec.buf = b;
        codec.bufOffset = offset;
        codec.bufLen = length;
        int len = codec.decodeNumber(backup, 0);
        codec.buf = backup;
        return Bytes.toString(backup, 0, len);
    }

    @Override
    public byte[] convertBytesValueFromBytes(byte[] b, int offset, int length) {
        NumberBytesCodec codec = getCodec(this.maxDigitsBeforeDecimalPoint);
        byte[] backup = codec.buf;
        codec.buf = b;
        codec.bufOffset = offset;
        codec.bufLen = length;
        int len = codec.decodeNumber(backup, 0);
        codec.buf = backup;
        byte[] bytes = new byte[len];
        System.arraycopy(backup, 0, bytes, 0 , len);
        return bytes;
    }

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

            // remove 0 in tail after the decimal point
            if (decimalPoint != end) {
                if (negative == true) {
                    while (buf[bufOffset + bufLen - 2] == '9' && (bufOffset + bufLen - 2 > decimalPoint)) {
                        bufLen--;
                    }

                    if (bufOffset + bufLen - 2 == decimalPoint) {
                        bufLen--;
                    }

                    buf[bufOffset + bufLen - 1] = ';';
                } else {
                    while (buf[bufOffset + bufLen - 1] == '0' && (bufOffset + bufLen - 1 > decimalPoint)) {
                        bufLen--;
                    }

                    if (bufOffset + bufLen - 1 == decimalPoint) {
                        bufLen--;
                    }

                }
            }
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
}