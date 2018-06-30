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

package org.apache.kylin.storage.hbase.steps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.kylin.common.util.BytesUtil;

public class RowKeyWritable implements WritableComparable<RowKeyWritable>, Serializable {
    private byte[] data;
    private int offset;
    private int length;
    private final static SerializableKVComparator kvComparator = new SerializableKVComparator();

    static {
        WritableComparator.define(RowKeyWritable.class, new RowKeyComparator());
    }

    public RowKeyWritable() {
        super();
    }

    public RowKeyWritable(byte[] bytes) {
        this.data = bytes;
        this.offset = 0;
        this.length = bytes.length;
    }

    public void set(byte[] array) {
        set(array, 0, array.length);
    }

    public void set(byte[] array, int offset, int length) {
        this.data = array;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getData() {
        return data;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
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

    @Override
    public int compareTo(RowKeyWritable other) {
        return kvComparator.compare(this.data, this.offset, this.length, other.data, other.offset, other.length);
    }

    @Override
    public String toString() {
        return BytesUtil.toHex(data, offset, length);
    }

    public static class SerializableKVComparator extends KeyValue.KVComparator implements Serializable {

    }

    public static class RowKeyComparator extends WritableComparator implements Serializable {
        public static final RowKeyComparator INSTANCE = new RowKeyComparator();
        private SerializableKVComparator kvComparator = new SerializableKVComparator();
        private static final int LENGTH_BYTES = 4;

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return kvComparator.compare(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2 + LENGTH_BYTES, l2 - LENGTH_BYTES);
        }
    }
}
