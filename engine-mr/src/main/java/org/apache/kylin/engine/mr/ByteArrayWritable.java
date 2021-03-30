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

package org.apache.kylin.engine.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.kylin.common.util.Bytes;

public class ByteArrayWritable implements WritableComparable<ByteArrayWritable> {

    private byte[] data;
    private int offset;
    private int length;

    public ByteArrayWritable() {
        this(null, 0, 0);
    }

    public ByteArrayWritable(int capacity) {
        this(new byte[capacity], 0, capacity);
    }

    public ByteArrayWritable(byte[] data) {
        this(data, 0, data == null ? 0 : data.length);
    }

    public ByteArrayWritable(byte[] data, int offset, int length) {
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

    public void set(byte[] array) {
        set(array, 0, array.length);
    }

    public void set(byte[] array, int offset, int length) {
        this.data = array;
        this.offset = offset;
        this.length = length;
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
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.length);
        out.write(this.data, this.offset, this.length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.length = in.readInt();
        this.data = new byte[this.length];
        in.readFully(this.data, 0, this.length);
        this.offset = 0;
    }

    // Below methods copied from BytesWritable
    /**
     * Define the sort order of the BytesWritable.
     * @param that The other bytes writable
     * @return Positive if left is bigger than right, 0 if they are equal, and
     *         negative if left is smaller than right.
     */
    public int compareTo(ByteArrayWritable that) {
        return WritableComparator.compareBytes(this.data, this.offset, this.length, that.data, that.offset,
                that.length);
    }

    /**
     * Compares the bytes in this object to the specified byte array
     * @param that
     * @return Positive if left is bigger than right, 0 if they are equal, and
     *         negative if left is smaller than right.
     */
    public int compareToByteArray(final byte[] that) {
        return WritableComparator.compareBytes(this.data, this.offset, this.length, that, 0, that.length);
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof byte[]) {
            return compareToByteArray((byte[]) other) == 0;
        }
        if (other instanceof ByteArrayWritable) {
            return compareTo((ByteArrayWritable) other) == 0;
        }
        return false;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(3 * this.length);
        final int endIdx = this.offset + this.length;
        for (int idx = this.offset; idx < endIdx; idx++) {
            sb.append(' ');
            String num = Integer.toHexString(0xff & this.data[idx]);
            // if it is only one digit, add a leading 0.
            if (num.length() < 2) {
                sb.append('0');
            }
            sb.append(num);
        }
        return sb.length() > 0 ? sb.substring(1) : "";
    }

    /** A Comparator optimized for ByteArrayWritable.
     */
    public static class Comparator extends WritableComparator {
        private BytesWritable.Comparator instance = new BytesWritable.Comparator();

        /** constructor */
        public Comparator() {
            super(ByteArrayWritable.class);
        }

        /**
         * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return instance.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(ByteArrayWritable.class, new Comparator());
    }
}
