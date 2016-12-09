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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.kylin.common.util.BytesUtil;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

@SuppressWarnings("serial")
public class HyperLogLogPlusCounterNew implements Serializable, Comparable<HyperLogLogPlusCounterNew> {

    private int p;

    private int m;

    private HashFunction hashFunc = Hashing.murmur3_128();

    private Register register;

    public static double overflowFactor = 0.01;

    public HyperLogLogPlusCounterNew(int p, RegisterType type, HashFunction hashFunc) {
        this.p = p;
        this.m = 1 << p;//(int) Math.pow(2, p);
        this.hashFunc = hashFunc;
        if (type == RegisterType.SPARSE) {
            double over = overflowFactor * m;
            this.register = new SparseRegister((int) over);
        } else {
            this.register = new DenseRegister(p);
        }
    }

    public HyperLogLogPlusCounterNew() {
        this(10, RegisterType.SPARSE, Hashing.murmur3_128());
    }

    public HyperLogLogPlusCounterNew(int p) {
        this(p, RegisterType.SPARSE, Hashing.murmur3_128());
    }

    public HyperLogLogPlusCounterNew(int p, RegisterType type) {
        this(p, type, Hashing.murmur3_128());
    }

    public HyperLogLogPlusCounterNew(int p, HashFunction hashFunc) {
        this(p, RegisterType.SPARSE, hashFunc);
    }

    public HyperLogLogPlusCounterNew(HyperLogLogPlusCounterNew another) {
        this(another.p, another.hashFunc);
        merge(another);
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
        Byte b = register.get(bucket);
        if (b == null || (byte) firstOnePos > b) {
            register.set(bucket, (byte) firstOnePos);
        }
        if (register instanceof SparseRegister) {
            if (((SparseRegister) register).isOverThreshold()) {
                register = ((SparseRegister) register).toDense(p);
            }
        }
    }

    public void merge(HyperLogLogPlusCounterNew another) {
        assert this.p == another.p;
        assert this.hashFunc == another.hashFunc;
        if (register instanceof SparseRegister && another.register instanceof SparseRegister) {
            register.merge(another.register);
            if (((SparseRegister) register).isOverThreshold()) {
                register = ((SparseRegister) register).toDense(p);
            }
        } else if (register instanceof SparseRegister && another.register instanceof DenseRegister) {
            register = ((SparseRegister) register).toDense(p);
            register.merge(another.register);
        } else {
            register.merge(another.register);
        }
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

        public HLLCSnapshot(HyperLogLogPlusCounterNew hllc) {
            p = (byte) hllc.p;
            registerSum = 0;
            zeroBuckets = 0;
            Register register = hllc.getRegister();
            DenseRegister dr;
            if (register instanceof SparseRegister) {
                dr = ((SparseRegister) register).toDense(p);
            } else {
                dr = (DenseRegister) register;
            }
            byte[] registers = dr.getRawRegister();
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

    public static void main(String[] args) throws IOException {
        dumpErrorRates();
    }

    static void dumpErrorRates() {
        for (int p = 10; p <= 18; p++) {
            double rate = new HyperLogLogPlusCounterNew(p, RegisterType.SPARSE).getErrorRate();
            double er = Math.round(rate * 10000) / 100D;
            double er2 = Math.round(rate * 2 * 10000) / 100D;
            double er3 = Math.round(rate * 3 * 10000) / 100D;
            long size = Math.round(Math.pow(2, p));
            System.out.println("HLLC" + p + ",\t" + size + " bytes,\t68% err<" + er + "%" + ",\t95% err<" + er2 + "%" + ",\t99.7% err<" + er3 + "%");
        }
    }

    public Register getRegister() {
        return register;
    }

    public void clear() {
        register.clear();
    }

    public RegisterType getRegisterType() {
        if (register instanceof SparseRegister)
            return RegisterType.SPARSE;
        else
            return RegisterType.DENSE;
    }

    // ============================================================================

    public void writeRegisters(final ByteBuffer out) throws IOException {

        final int indexLen = getRegisterIndexSize();
        int size = size();

        // decide output scheme -- map (3*size bytes) or array (2^p bytes)
        byte scheme;
        //byte type;
        if (register instanceof SparseRegister || 5 + (indexLen + 1) * size < m) {
            scheme = 0; //map
        } else {
            scheme = 1; // array
        }
        out.put(scheme);
        if (scheme == 0) { // map scheme
            BytesUtil.writeVInt(size, out);
            if (register instanceof SparseRegister) { //sparse　register
                Collection<Map.Entry<Integer, Byte>> allValue = ((SparseRegister) register).getAllValue();
                for (Map.Entry<Integer, Byte> entry : allValue) {
                    writeUnsigned(entry.getKey(), indexLen, out);
                    out.put(entry.getValue());
                }
            } else { //dense register
                byte[] registers = ((DenseRegister) register).getRawRegister();
                for (int i = 0; i < m; i++) {
                    if (registers[i] > 0) {
                        writeUnsigned(i, indexLen, out);
                        out.put(registers[i]);
                    }
                }
            }
        } else if (scheme == 1) { // array scheme
            out.put(((DenseRegister) register).getRawRegister());
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
            double over = overflowFactor * m;
            if (size > (int) over) {
                this.register = new DenseRegister(p);
            } else {
                this.register = new SparseRegister((int) over);//default is sparse
            }
            int indexLen = getRegisterIndexSize();
            int key = 0;
            for (int i = 0; i < size; i++) {
                key = readUnsigned(in, indexLen);
                register.set(key, in.get());
            }
        } else if (scheme == 1) { // array scheme
            this.register = new DenseRegister(p);
            for (int i = 0; i < m; i++) {
                register.set(i, in.get());
            }
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

    private int getRegisterIndexSize() {
        return (p - 1) / 8 + 1; // 2 when p=16, 3 when p=17
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hashFunc == null) ? 0 : hashFunc.hashCode());
        result = prime * result + p;
        result = prime * result + register.getHashCode();
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
        HyperLogLogPlusCounterNew other = (HyperLogLogPlusCounterNew) obj;
        if (hashFunc == null) {
            if (other.hashFunc != null)
                return false;
        } else if (!hashFunc.equals(other.hashFunc))
            return false;
        if (p != other.p)
            return false;
        if (this.getRegisterType() != other.getRegisterType())
            return false;
        if (register.getHashCode() != other.register.getHashCode())
            return false;
        return true;
    }

    @Override
    public int compareTo(HyperLogLogPlusCounterNew o) {
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

    private int size() {
        return register.getSize();
    }

}
