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

package org.apache.kylin.measure.hllc;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.guava30.shaded.common.hash.HashFunction;
import org.apache.kylin.guava30.shaded.common.hash.Hashing;

/**
 * Deprecated, use HLLCounter instead.
 *
 * About compression, test on HLLC data shows
 *
 * - LZF compression ratio is around 65%-80%, fast
 * - GZIP compression ratio is around 41%-46%, very slow
 */
@Deprecated
@SuppressWarnings("serial")
public class HLLCounterOld implements Serializable, Comparable<HLLCounterOld> {

    private final int p;
    private final int m;
    private final HashFunction hashFunc;
    byte[] registers;
    int singleBucket;

    public HLLCounterOld() {
        this(10);
    }

    public HLLCounterOld(int p) {
        this(p, Hashing.murmur3_128());
    }

    public HLLCounterOld(HLLCounterOld another) {
        this(another.p, another.hashFunc);
        merge(another);
    }

    /** The larger p is, the more storage (2^p bytes), the better accuracy */
    private HLLCounterOld(int p, HashFunction hashFunc) {
        this.p = p;
        this.m = 1 << p;//(int) Math.pow(2, p);
        this.hashFunc = hashFunc;
        this.registers = new byte[m];
        this.singleBucket = -1;
    }

    public void clear() {
        byte zero = (byte) 0;
        if (singleBucket == -1) {
            //nothing
        } else if (singleBucket >= 0) {
            registers[singleBucket] = 0;
        } else {
            Arrays.fill(registers, zero);
        }
        singleBucket = -1;
    }

    public void add(int value) {
        add(hashFunc.hashInt(value).asLong());
    }

    public void add(String value) {
        add(hashFunc.hashString(value, Charset.defaultCharset()).asLong());
    }

    public void add(byte[] value) {
        add(hashFunc.hashBytes(value).asLong());
    }

    public void add(byte[] value, int offset, int length) {
        add(hashFunc.hashBytes(value, offset, length).asLong());
    }

    protected void add(long hash) {
        int bucketMask = m - 1;
        int bucket = (int) (hash & bucketMask);
        int firstOnePos = Long.numberOfLeadingZeros(hash | bucketMask) + 1;

        if (firstOnePos > registers[bucket])
            registers[bucket] = (byte) firstOnePos;

        if (singleBucket == -1)
            singleBucket = bucket;
        else
            singleBucket = Integer.MIN_VALUE;
    }

    public void merge(HLLCounterOld another) {
        assert this.p == another.p;
        assert this.hashFunc == another.hashFunc;

        // quick path for single value HLLC
        if (another.singleBucket == -1) {
            return;
        } else if (another.singleBucket >= 0) {
            int b = another.singleBucket;
            if (registers[b] < another.registers[b])
                registers[b] = another.registers[b];
        } else {
            // normal path
            for (int i = 0; i < m; i++) {
                if (registers[i] < another.registers[i])
                    registers[i] = another.registers[i];
            }
        }
        singleBucket = Integer.MIN_VALUE;
    }

    public long getCountEstimate() {
        return new HLLCSnapshot(this).getCountEstimate();
    }

    public int getPrecision() {
        return this.p;
    }

    public double getErrorRate() {
        return 1.04 / Math.sqrt(m);
    }

    private int size() {
        if (singleBucket == -1) {
            return 0;
        } else if (singleBucket >= 0) {
            return 1;
        } else {
            int size = 0;
            for (int i = 0; i < m; i++) {
                if (registers[i] > 0)
                    size++;
            }
            return size;
        }
    }

    @Override
    public String toString() {
        return "" + getCountEstimate();
    }

    // ============================================================================

    // a memory efficient snapshot of HLL registers which can yield count
    // estimate later
    public static class HLLCSnapshot {
        byte p;
        double registerSum;
        int zeroBuckets;

        public HLLCSnapshot(HLLCounterOld hllc) {
            p = (byte) hllc.p;
            registerSum = 0;
            zeroBuckets = 0;

            byte[] registers = hllc.registers;
            for (int i = 0; i < hllc.m; i++) {
                if (registers[i] == 0) {
                    registerSum++;
                    zeroBuckets++;
                } else {
                    registerSum += 1.0 / (1L << registers[i]);
                }
            }
        }

        public long getCountEstimate() {
            int m = 1 << p;
            double alpha = 0.7213 / (1 + 1.079 / m);
            double estimate = alpha * m * m / registerSum;

            // small cardinality adjustment
            if (zeroBuckets >= m * 0.07) { // (reference presto's HLL impl)
                estimate = m * Math.log(m * 1.0 / zeroBuckets);
            } else if (HyperLogLogPlusTable.isBiasCorrection(m, estimate)) {
                estimate = HyperLogLogPlusTable.biasCorrection(p, estimate);
            }

            return Math.round(estimate);
        }
    }

    // ============================================================================

    public void writeRegisters(final ByteBuffer out) throws IOException {

        final int indexLen = getRegisterIndexSize();
        int size = size();

        // decide output scheme -- map (3*size bytes) or array (2^p bytes)
        byte scheme;
        if (5 + (indexLen + 1) * size < m) // 5 is max len of vint
            scheme = 0; // map
        else
            scheme = 1; // array
        out.put(scheme);

        if (scheme == 0) { // map scheme
            BytesUtil.writeVInt(size, out);
            if (singleBucket == -1) {
                // no non-zero register
            } else if (singleBucket >= 0) {
                writeUnsigned(singleBucket, indexLen, out);
                out.put(registers[singleBucket]);
            } else {
                for (int i = 0; i < m; i++) {
                    if (registers[i] > 0) {
                        writeUnsigned(i, indexLen, out);
                        out.put(registers[i]);
                    }
                }
            }
        } else if (scheme == 1) { // array scheme
            out.put(registers);
        } else
            throw new IllegalStateException();
    }

    public void readRegisters(ByteBuffer in) throws IOException {
        byte scheme = in.get();

        if (scheme == 0) { // map scheme
            clear();
            int size = BytesUtil.readVInt(in);
            if (size > m)
                throw new IllegalArgumentException("register size (" + size + ") cannot be larger than m (" + m + ")");
            int indexLen = getRegisterIndexSize();
            int key = 0;
            for (int i = 0; i < size; i++) {
                key = readUnsigned(in, indexLen);
                registers[key] = in.get();
            }

            if (size == 0)
                singleBucket = -1;
            else if (size == 1)
                singleBucket = key;
            else
                singleBucket = Integer.MIN_VALUE;

        } else if (scheme == 1) { // array scheme
            in.get(registers);
            singleBucket = Integer.MIN_VALUE;
        } else
            throw new IllegalStateException();
    }

    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int len;

        byte scheme = in.get();
        if (scheme == 0) { // map scheme
            int size = BytesUtil.readVInt(in);
            int indexLen = getRegisterIndexSize();
            len = in.position() - mark + (indexLen + 1) * size;
        } else {
            len = in.position() - mark + m;
        }

        in.position(mark);
        return len;
    }

    public int maxLength() {
        return 1 + m;
    }

    /*public void writeRegistersArray(final ByteBuffer out) {
        out.put(this.registers);
    }
    
    public void readRegistersArray(ByteBuffer in) {
        in.get(registers, 0, m);
        singleBucket = Integer.MIN_VALUE;
    }*/

    private int getRegisterIndexSize() {
        return (p - 1) / 8 + 1; // 2 when p=16, 3 when p=17
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hashFunc == null) ? 0 : hashFunc.hashCode());
        result = prime * result + p;
        result = prime * result + Arrays.hashCode(registers);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HLLCounterOld other = (HLLCounterOld) obj;
        if (hashFunc == null) {
            if (other.hashFunc != null)
                return false;
        } else if (!hashFunc.equals(other.hashFunc))
            return false;
        if (p != other.p)
            return false;
        if (!Arrays.equals(registers, other.registers))
            return false;
        return true;
    }

    @Override
    public int compareTo(HLLCounterOld o) {
        if (o == null)
            return 1;

        long e1 = this.getCountEstimate();
        long e2 = o.getCountEstimate();

        if (e1 == e2)
            return 0;
        else if (e1 > e2)
            return 1;
        else
            return -1;
    }

    public static void main(String[] args) throws IOException {
        dumpErrorRates();
    }

    static void dumpErrorRates() {
        for (int p = 10; p <= 18; p++) {
            double rate = new HLLCounterOld(p).getErrorRate();
            double er = Math.round(rate * 10000) / 100D;
            double er2 = Math.round(rate * 2 * 10000) / 100D;
            double er3 = Math.round(rate * 3 * 10000) / 100D;
            long size = Math.round(Math.pow(2, p));
            System.out.println("HLLC" + p + ",\t" + size + " bytes,\t68% err<" + er + "%" + ",\t95% err<" + er2 + "%"
                    + ",\t99.7% err<" + er3 + "%");
        }
    }

    /**
     *
     * @param num
     * @param size
     * @param out
     */
    public static void writeUnsigned(int num, int size, ByteBuffer out) {
        for (int i = 0; i < size; i++) {
            out.put((byte) num);
            num >>>= 8;
        }
    }

    public static int readUnsigned(ByteBuffer in, int size) {
        int integer = 0;
        int mask = 0xff;
        int shift = 0;
        for (int i = 0; i < size; i++) {
            integer |= (in.get() << shift) & mask;
            mask = mask << 8;
            shift += 8;
        }
        return integer;
    }
}
