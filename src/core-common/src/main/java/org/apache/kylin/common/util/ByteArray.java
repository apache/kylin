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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author yangli9
 */
public class ByteArray implements Comparable<ByteArray>, Serializable {

    private static final long serialVersionUID = 1L;

    public static ByteArray allocate(int length) {
        return new ByteArray(new byte[length]);
    }

    public static ByteArray copyOf(byte[] array, int offset, int length) {
        byte[] space = new byte[length];
        System.arraycopy(array, offset, space, 0, length);
        return new ByteArray(space, 0, length);
    }

    // ============================================================================

    private byte[] data;
    private int offset;
    private int length;

    public ByteArray() {
        this(null, 0, 0);
    }

    public ByteArray(int capacity) {
        this(new byte[capacity], 0, capacity);
    }

    public ByteArray(byte[] data) {
        this(data, 0, data == null ? 0 : data.length);
    }

    public ByteArray(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public byte[] array() {
        return data;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    //notice this will have a length header
    public void exportData(ByteBuffer out) {
        BytesUtil.writeByteArray(this.data, this.offset, this.length, out);
    }

    public static ByteArray importData(ByteBuffer in) {
        byte[] bytes = BytesUtil.readByteArray(in);
        return new ByteArray(bytes);
    }

    public ByteBuffer asBuffer() {
        if (data == null)
            return null;
        else if (offset == 0 && length == data.length)
            return ByteBuffer.wrap(data);
        else
            return ByteBuffer.wrap(data, offset, length).slice();
    }

    public byte[] toBytes() {
        return Bytes.copy(this.array(), this.offset(), this.length());
    }

    public void setLength(int pos) {
        this.length = pos;
    }

    public void reset(byte[] data, int offset, int len) {
        this.data = data;
        this.offset = offset;
        this.length = len;
    }

    public byte get(int i) {
        return data[offset + i];
    }

    @Override
    public int hashCode() {
        if (data == null) {
            return 0;
        } else {
            if (length <= Bytes.SIZEOF_LONG && length > 0) {
                // to avoid hash collision of byte arrays those are converted
                // from nearby integers/longs, which is the case for kylin dictionary
                long value = BytesUtil.readLong(data, offset, length);
                return (int) (value ^ (value >>> 32));
            }
            return Bytes.hashCode(data, offset, length);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ByteArray o = (ByteArray) obj;
        if (this.data == null && o.data == null)
            return true;
        else if (this.data == null || o.data == null)
            return false;
        else
            return Bytes.equals(this.data, this.offset, this.length, o.data, o.offset, o.length);
    }

    @Override
    public int compareTo(ByteArray o) {
        if (this.data == null && o.data == null)
            return 0;
        else if (this.data == null)
            return -1;
        else if (o.data == null)
            return 1;
        else
            return Bytes.compareTo(this.data, this.offset, this.length, o.data, o.offset, o.length);
    }

    public String toReadableText() {
        if (data == null) {
            return null;
        } else {
            return BytesUtil.toHex(data, offset, length);
        }
    }

    @Override
    public String toString() {
        if (data == null)
            return "";
        else
            return Bytes.toStringBinary(data, offset, length);
    }

}
