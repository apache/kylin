package org.apache.kylin.common.util;

import java.util.BitSet;

public class ImmutableBitSet {

    public static final ImmutableBitSet EMPTY = new ImmutableBitSet(new BitSet());

    final private BitSet set;
    final private int[] arr;

    public ImmutableBitSet(int index) {
        this(newBitSet(index));
    }

    private static BitSet newBitSet(int index) {
        BitSet set = new BitSet(index);
        set.set(index);
        return set;
    }

    public ImmutableBitSet(int indexFrom, int indexTo) {
        this(newBitSet(indexFrom, indexTo));
    }
    
    private static BitSet newBitSet(int indexFrom, int indexTo) {
        BitSet set = new BitSet(indexTo);
        set.set(indexFrom, indexTo);
        return set;
    }

    public ImmutableBitSet(BitSet set) {
        this.set = (BitSet) set.clone();
        this.arr = new int[set.cardinality()];

        int j = 0;
        for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1)) {
            arr[j++] = i;
        }
    }

    /** return number of true bits */
    public int trueBitCount() {
        return arr.length;
    }

    /** return the i-th true bit */
    public int trueBitAt(int i) {
        return arr[i];
    }
    
    /** return the bit's index among true bits */
    public int trueBitIndexOf(int bitIndex) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == bitIndex)
                return i;
        }
        return -1;
    }

    public BitSet mutable() {
        return (BitSet) set.clone();
    }

    public ImmutableBitSet set(int bitIndex) {
        return set(bitIndex, true);
    }
    
    public ImmutableBitSet set(int bitIndex, boolean value) {
        if (set.get(bitIndex) == value) {
            return this;
        } else {
            BitSet mutable = mutable();
            mutable.set(bitIndex, value);
            return new ImmutableBitSet(mutable);
        }
    }
    
    public ImmutableBitSet or(ImmutableBitSet another) {
        BitSet mutable = mutable();
        mutable.or(another.set);
        return new ImmutableBitSet(mutable);
    }
    
    public ImmutableBitSet andNot(ImmutableBitSet another) {
        BitSet mutable = mutable();
        mutable.andNot(another.set);
        return new ImmutableBitSet(mutable);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        ImmutableBitSet other = (ImmutableBitSet) obj;
        return this.set.equals(other.set);
    }
    
    @Override
    public String toString() {
        return set.toString();
    }

    // ============================================================================

    public boolean get(int bitIndex) {
        return set.get(bitIndex);
    }

    public int cardinality() {
        return set.cardinality();
    }

    public boolean intersects(ImmutableBitSet another) {
        return set.intersects(another.set);
    }

    public boolean isEmpty() {
        return set.isEmpty();
    }
}
