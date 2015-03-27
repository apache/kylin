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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author yangli9
 */
public class ByteArray implements Comparable<ByteArray> {

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
        set(null, 0, 0);
    }

    public ByteArray(int capacity) {
        set(new byte[capacity], 0, capacity);
    }

    public ByteArray(byte[] data) {
        set(data, 0, data.length);
    }

    public ByteArray(byte[] data, int offset, int length) {
        set(data, offset, length);
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

    public void set(byte[] array) {
        set(array, 0, array.length);
    }

    public void set(byte[] array, int offset, int length) {
        this.data = array;
        this.offset = offset;
        this.length = length;
    }

    public void set(ByteArray o) {
        set(o.data, o.offset, o.length);
    }

    public void setLength(int length) {
        this.length = length;
    }

    public ByteArray copy() {
        ByteArray copy = new ByteArray(length);
        copy.copyFrom(this);
        return copy;
    }

    public void copyFrom(ByteArray other) {
        System.arraycopy(other.array(), other.offset, data, offset, other.length);
        this.length = other.length;
    }

    public ByteBuffer asBuffer() {
        if (data == null)
            return null;
        else if (offset == 0 && length == data.length)
            return ByteBuffer.wrap(data);
        else
            return ByteBuffer.wrap(data, offset, length).slice();
    }

    @Override
    public int hashCode() {
        if (data == null)
            return 0;
        else
            return Bytes.hashCode(data, offset, length);
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

    @Override
    public String toString() {
        return Bytes.toString(data, offset, length);
    }

}
