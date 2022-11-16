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
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ImmutableBitSet implements Iterable<Integer>, Serializable {
    private static final long serialVersionUID = 2670748367234738640L;

    public static final ImmutableBitSet EMPTY = new ImmutableBitSet(new BitSet());

    public static ImmutableBitSet valueOf(int... values) {
        return new ImmutableBitSet(BitSets.valueOf(values), false);
    }

    public static ImmutableBitSet valueOf(List<Integer> values) {
        return new ImmutableBitSet(BitSets.valueOf(values), false);
    }

    // ============================================================================

    final private BitSet set;
    final private int[] arr;

    public ImmutableBitSet(int index) {
        this(newBitSet(index));
    }

    private ImmutableBitSet(BitSet set, boolean needClone) {
        if (needClone) {
            this.set = (BitSet) set.clone();
        } else {
            this.set = set;
        }
        this.arr = new int[set.cardinality()];

        int j = 0;
        for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1)) {
            arr[j++] = i;
        }
    }

    public ImmutableBitSet(BitSet set) {
        this(set, true);
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

    public static final BytesSerializer<ImmutableBitSet> serializer = new BytesSerializer<ImmutableBitSet>() {
        @Override
        public void serialize(ImmutableBitSet value, ByteBuffer out) {
            BytesUtil.writeByteArray(value.set.toByteArray(), out);
        }

        @Override
        public ImmutableBitSet deserialize(ByteBuffer in) {
            BitSet bitSet = BitSet.valueOf(BytesUtil.readByteArray(in));
            return new ImmutableBitSet(bitSet);
        }
    };

    /**
     * Iterate over the positions of true value.
     * @return the iterator
     */
    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < arr.length;
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return arr[index++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public int size() {
        return set.size();
    }
}
