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

package org.apache.kylin.common.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Locale;

import org.apache.kylin.guava30.shaded.common.primitives.Shorts;

public class BytesUtil {

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public static void writeByte(byte num, byte[] bytes, int offset, int size) {
        for (int i = offset + size - 1; i >= offset; i--) {
            bytes[i] = num;
            num >>>= 8;
        }
    }

    public static void writeShort(short num, byte[] bytes, int offset, int size) {
        for (int i = offset + size - 1; i >= offset; i--) {
            bytes[i] = (byte) num;
            num >>>= 8;
        }
    }

    public static byte[] writeShort(short num) {
        byte[] output = new byte[Shorts.BYTES];
        writeShort(num, output, 0, output.length);
        return output;
    }

    public static long readShort(byte[] bytes, int offset, int size) {
        short num = 0;
        for (int i = offset, n = offset + size; i < n; i++) {
            num <<= 8;
            num |= (short) bytes[i] & 0xFF;
        }
        return num;
    }

    public static short readShort(byte[] bytes) {
        return (short) readShort(bytes, 0, Shorts.BYTES);
    }

    public static void writeUnsigned(int num, byte[] bytes, int offset, int size) {
        for (int i = offset + size - 1; i >= offset; i--) {
            bytes[i] = (byte) num;
            num >>>= 8;
        }
    }

    public static int readUnsigned(byte[] bytes, int offset, int size) {
        int integer = 0;
        for (int i = offset, n = offset + size; i < n; i++) {
            integer <<= 8;
            integer |= (int) bytes[i] & 0xFF;
        }
        return integer;
    }

    public static void writeUnsigned(int num, int size, ByteBuffer out) {
        int mask = 0xff << ((size - 1) * 8);
        for (int i = size; i > 0; i--) {
            int v = (num & mask) >> (i - 1) * 8;
            out.put((byte) v);
            mask = mask >> 8;
        }
    }

    public static int readUnsigned(ByteBuffer in, int size) {
        int integer = 0;
        for (int i = 0; i < size; i++) {
            integer = integer << 8;
            integer |= (in.get() & 0xff);
        }

        return integer;
    }

    public static int readUnsigned(ByteArray in, int offset, int size) {
        int integer = 0;
        offset += in.offset();
        byte[] bytes = in.array();
        for (int i = offset, n = offset + size; i < n; i++) {
            integer <<= 8;
            integer |= (int) bytes[i] & 0xFF;
        }
        return integer;
    }

    public static void writeSignedLong(long num, byte[] bytes, int offset, int size) {
        writeLong(num, bytes, offset, size);
    }

    public static long readSignedLong(byte[] bytes, int offset, int size) {
        long integer = (bytes[offset] & 0x80) == 0 ? 0 : -1;
        for (int i = offset, n = offset + size; i < n; i++) {
            integer <<= 8;
            integer |= (int) bytes[i] & 0xFF;
        }
        return integer;
    }

    public static void writeLong(long num, byte[] bytes, int offset, int size) {
        for (int i = offset + size - 1; i >= offset; i--) {
            bytes[i] = (byte) num;
            num >>>= 8;
        }
    }

    public static long readLong(byte[] bytes, int offset, int size) {
        long integer = 0;
        for (int i = offset, n = offset + size; i < n; i++) {
            integer <<= 8;
            integer |= (long) bytes[i] & 0xFF;
        }
        return integer;
    }

    public static void writeLong(long num, ByteBuffer out) {
        for (int i = 0; i < 8; i++) {
            out.put((byte) num);
            num >>>= 8;
        }
    }

    public static long readLong(ByteBuffer in) {
        long integer = 0;
        long mask = 0xff;
        int shift = 0;
        for (int i = 0; i < 8; i++) {
            integer |= (in.get() << shift) & mask;
            mask = mask << 8;
            shift += 8;
        }
        return integer;
    }

    /**
     * No. bytes needed to store a value as big as the given
     */
    public static int sizeForValue(long maxValue) {
        int size = 0;
        while (maxValue > 0) {
            size++;
            maxValue >>>= 8;
        }
        return size;
    }

    public static int compareByteUnsigned(byte b1, byte b2) {
        int i1 = (int) b1 & 0xFF;
        int i2 = (int) b2 & 0xFF;
        return i1 - i2;
    }

    public static byte[] subarray(byte[] bytes, int start, int end) {
        byte[] r = new byte[end - start];
        System.arraycopy(bytes, start, r, 0, r.length);
        return r;
    }

    public static int compareBytes(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
        int r = 0;
        for (int i = 0; i < length; i++) {
            r = src[srcOffset + i] - dst[dstOffset + i];
            if (r != 0)
                break;
        }
        return r;
    }

    public static boolean isPositiveShort(int i) {
        return (i & 0xFFFF7000) == 0;
    }

    // from WritableUtils
    // ============================================================================

    public static void writeVInt(int i, ByteBuffer out) {

        writeVLong(i, out);

    }

    public static void writeVLong(long i, ByteBuffer out) {

        if (i >= -112 && i <= 127) {
            out.put((byte) i);
            return;
        }

        int len = -112;
        if (i < 0) {
            i ^= -1L; // take one's complement'
            len = -120;
        }

        long tmp = i;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }

        out.put((byte) len);

        len = (len < -120) ? -(len + 120) : -(len + 112);

        for (int idx = len; idx != 0; idx--) {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            out.put((byte) ((i & mask) >> shiftbits));
        }
    }

    public static long readVLong(ByteBuffer in) {
        byte firstByte = in.get();
        int len = decodeVIntSize(firstByte);
        if (len == 1) {
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
            byte b = in.get();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }

    public static int readVInt(ByteBuffer in) {
        long n = readVLong(in);
        if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
            throw new IllegalArgumentException("value too long to fit in integer");
        }
        return (int) n;
    }

    private static boolean isNegativeVInt(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

    private static int decodeVIntSize(byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    public static void writeUTFString(String str, ByteBuffer out) {
        byte[] bytes = str == null ? null : Bytes.toBytes(str);
        writeByteArray(bytes, out);
    }

    public static String readUTFString(ByteBuffer in) {
        byte[] bytes = readByteArray(in);
        return bytes == null ? null : Bytes.toString(bytes);
    }

    public static void writeAsciiString(String str, ByteBuffer out) {
        if (str == null) {
            BytesUtil.writeVInt(-1, out);
            return;
        }
        int len = str.length();
        BytesUtil.writeVInt(len, out);

        for (int i = 0; i < len; i++) {
            out.put((byte) str.charAt(i));
        }
    }

    public static String readAsciiString(ByteBuffer in) {
        int len = BytesUtil.readVInt(in);
        if (len < 0) {
            return null;
        }
        String result;
        try {
            if (in.hasArray()) {
                int pos = in.position();
                result = new String(in.array(), pos, len, "ISO-8859-1");
                in.position(pos + len);
            } else {
                byte[] tmp = new byte[len];
                in.get(tmp);
                result = new String(tmp, "ISO-8859-1");
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
        return result;
    }

    public static void writeAsciiStringArray(String[] strs, ByteBuffer out) {
        writeVInt(strs.length, out);
        for (int i = 0; i < strs.length; i++)
            writeAsciiString(strs[i], out);
    }

    public static String[] readAsciiStringArray(ByteBuffer in) {
        int len = readVInt(in);
        String[] strs = new String[len];
        for (int i = 0; i < len; i++)
            strs[i] = readAsciiString(in);
        return strs;
    }

    public static void writeIntArray(int[] array, ByteBuffer out) {
        if (array == null) {
            writeVInt(-1, out);
            return;
        }
        writeVInt(array.length, out);
        for (int i = 0; i < array.length; ++i) {
            writeVInt(array[i], out);
        }
    }

    public static int[] readIntArray(ByteBuffer in) {
        int len = readVInt(in);
        if (len < 0)
            return null;
        int[] array = new int[len];

        for (int i = 0; i < len; ++i) {
            array[i] = readVInt(in);
        }
        return array;
    }

    public static void writeByteArray(byte[] array, ByteBuffer out) {
        if (array == null) {
            writeVInt(-1, out);
            return;
        }
        writeVInt(array.length, out);
        out.put(array);
    }

    public static void writeByteArray(byte[] array, int offset, int length, ByteBuffer out) {
        if (array == null) {
            writeVInt(-1, out);
            return;
        }
        writeVInt(length, out);
        out.put(array, offset, length);
    }

    public static byte[] readByteArray(ByteBuffer in) {
        int len = readVInt(in);
        if (len < 0)
            return null;

        byte[] array = new byte[len];
        in.get(array);
        return array;
    }

    public static int peekByteArrayLength(ByteBuffer in) {
        int start = in.position();
        int arrayLen = readVInt(in);
        int sizeLen = in.position() - start;
        in.position(start);

        if (arrayLen < 0)
            return sizeLen;
        else
            return sizeLen + arrayLen;
    }

    public static void writeBooleanArray(boolean[] array, ByteBuffer out) {
        if (array == null) {
            writeVInt(-1, out);
            return;
        }
        writeVInt(array.length, out);
        byte b_true = (byte) 1;
        byte b_false = (byte) 0;

        for (int i = 0; i < array.length; i++) {
            if (array[i])
                out.put(b_true);
            else
                out.put(b_false);
        }
    }

    public static boolean[] readBooleanArray(ByteBuffer in) {
        int len = readVInt(in);
        if (len < 0)
            return null;

        boolean[] array = new boolean[len];
        byte b_true = (byte) 1;
        for (int i = 0; i < array.length; i++) {
            byte temp = in.get();
            if (temp == b_true)
                array[i] = true;
            else
                array[i] = false;
        }
        return array;
    }

    public static String toReadableText(byte[] array) {
        if (array == null)
            return null;
        return toHex(array);
    }

    /**
     * this method only works for hex strings
     */
    public static byte[] fromReadableText(String text) {
        String[] tokens = text.split("\\\\x");
        byte[] ret = new byte[tokens.length - 1];
        for (int i = 1; i < tokens.length; ++i) {
            int x = Bytes.toBinaryFromHex((byte) tokens[i].charAt(0));
            x = x << 4;
            int y = Bytes.toBinaryFromHex((byte) tokens[i].charAt(1));
            ret[i - 1] = (byte) (x + y);
        }
        return ret;
    }

    public static String toHex(byte[] array) {
        return toHex(array, 0, array.length);
    }

    public static String toHex(byte[] array, int offset, int length) {
        StringBuilder sb = new StringBuilder(length * 4);
        for (int i = 0; i < length; i++) {
            int b = array[offset + i];
            sb.append(String.format(Locale.ROOT, "\\x%02X", b & 0xFF));
        }
        return sb.toString();
    }

    public static byte[] mergeBytes(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null && bytes2 == null) {
            throw new NullPointerException();
        }
        if (bytes1 == null) {
            return bytes2;
        }
        if (bytes2 == null) {
            return bytes1;
        }
        byte[] bytes = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, bytes, 0, bytes1.length);
        System.arraycopy(bytes2, 0, bytes, bytes1.length, bytes2.length);
        return bytes;
    }

}
