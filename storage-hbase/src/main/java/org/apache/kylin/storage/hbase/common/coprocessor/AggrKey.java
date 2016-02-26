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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.util.Arrays;
import java.util.LinkedList;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dimension.FixedLenDimEnc;

import com.google.common.collect.Lists;

/**
 */
public class AggrKey implements Comparable<AggrKey> {

    final byte[] groupByMask;
    final transient int[] groupByMaskSet;
    transient int hashcode;
    byte[] data;
    int offset;

    AggrKey(byte[] groupByMask) {
        this.groupByMask = groupByMask;
        LinkedList<Integer> list = Lists.newLinkedList();
        for (int i = 0; i < groupByMask.length; i++) {
            if (groupByMask[i] != 0) {
                list.add(i);
            }
        }
        groupByMaskSet = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            groupByMaskSet[i] = list.get(i);
        }
    }

    private AggrKey(byte[] groupByMask, int[] groupByMaskSet, byte[] data, int offset, int hashcode) {
        this.groupByMask = groupByMask;
        this.groupByMaskSet = groupByMaskSet;
        this.data = data;
        this.offset = offset;
        this.hashcode = hashcode;
    }

    private int calculateHash() {
        int hash = 1;
        for (int i = 0; i < groupByMaskSet.length; i++) {
            byte t = data[offset + groupByMaskSet[i]];
            if (t != FixedLenDimEnc.ROWKEY_PLACE_HOLDER_BYTE) {
                hash = (31 * hash) + t;
            }
        }
        return hash;
    }

    public byte[] get() {
        return data;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return groupByMask.length;
    }

    void set(byte[] data, int offset) {
        this.data = data;
        this.offset = offset;
        this.hashcode = calculateHash();
    }

    public byte[] getGroupByMask() {
        return this.groupByMask;
    }

    public byte[] copyBytes() {
        return Arrays.copyOfRange(data, offset, offset + length());
    }

    AggrKey copy() {
        AggrKey copy = new AggrKey(this.groupByMask, this.groupByMaskSet, copyBytes(), 0, this.hashcode);
        return copy;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AggrKey other = (AggrKey) obj;
        return compareTo(other) == 0;
    }

    @Override
    public int compareTo(AggrKey o) {
        int comp = this.length() - o.length();
        if (comp != 0)
            return comp;

        for (int i = 0; i < groupByMaskSet.length; i++) {
            comp = BytesUtil.compareByteUnsigned(this.data[this.offset + groupByMaskSet[i]], o.data[o.offset + groupByMaskSet[i]]);
            if (comp != 0)
                return comp;
        }
        return 0;
    }
}
