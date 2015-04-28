package org.apache.kylin.storage.hbase.coprocessor;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.BytesUtil;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by qianzhou on 4/20/15.
 */
public class AggrKey implements Comparable<AggrKey> {

    final byte[] groupByMask;

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

    private AggrKey(byte[] groupByMask, int[] groupByMaskSet) {
        this.groupByMask = groupByMask;
        this.groupByMaskSet = groupByMaskSet;
    }

    final transient int[] groupByMaskSet;
    byte[] data;
    int offset;

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
    }

    public byte[] getGroupByMask() {
        return this.groupByMask;
    }

    public byte[] copyBytes() {
        return Arrays.copyOfRange(data, offset, offset + length());
    }

    AggrKey copy() {
        AggrKey copy = new AggrKey(this.groupByMask, this.groupByMaskSet);
        copy.set(copyBytes(), 0);
        return copy;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = 0; i < groupByMaskSet.length; i++) {
            hash = (31 * hash) + data[offset + groupByMaskSet[i]];
        }
        return hash;
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
        if (this.length() != other.length())
            return false;

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
