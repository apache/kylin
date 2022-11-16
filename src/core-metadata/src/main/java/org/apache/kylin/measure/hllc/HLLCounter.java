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
import java.util.Collection;
import java.util.Map;

import org.apache.kylin.common.util.BytesUtil;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

@SuppressWarnings("serial")
public class HLLCounter implements Serializable, Comparable<HLLCounter> {

    // not final for test purpose
    static double OVERFLOW_FACTOR = 0.01;

    private int p;

    private int m;

    private HashFunction hashFunc = Hashing.murmur3_128();

    private Register register;

    public HLLCounter() {
        this(10, RegisterType.SINGLE_VALUE, Hashing.murmur3_128());
    }

    public HLLCounter(int p) {
        this(p, RegisterType.SINGLE_VALUE, Hashing.murmur3_128());
    }

    public HLLCounter(int p, HashFunction hashFunc) {
        this(p, RegisterType.SINGLE_VALUE, hashFunc);
    }

    public HLLCounter(HLLCounter another) {
        this(another.p, another.getRegisterType(), another.hashFunc);
        merge(another);
    }

    public HLLCounter(int p, RegisterType type) {
        this(p, type, Hashing.murmur3_128());
    }

    HLLCounter(int p, RegisterType type, HashFunction hashFunc) {
        this.p = p;
        this.m = 1 << p;//(int) Math.pow(2, p);
        this.hashFunc = hashFunc;

        if (type == RegisterType.SINGLE_VALUE) {
            this.register = new SingleValueRegister();
        } else if (type == RegisterType.SPARSE) {
            this.register = new SparseRegister();
        } else {
            this.register = new DenseRegister(p);
        }
    }

    private boolean isDense(int size) {
        double over = OVERFLOW_FACTOR * m;
        return size > (int) over;
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

    public void addHashDirectly(long hash) {
        add(hash);
    }

    protected void add(long hash) {
        int bucketMask = m - 1;
        int bucket = (int) (hash & bucketMask);
        int firstOnePos = Long.numberOfLeadingZeros(hash | bucketMask) + 1;
        if (register.getRegisterType() == RegisterType.SINGLE_VALUE) {
            SingleValueRegister sr = (SingleValueRegister) register;
            int pos = sr.getSingleValuePos();
            if (pos < 0 || pos == bucket) { //one or zero value
                setIfBigger(register, bucket, (byte) firstOnePos);
            } else { //two value
                this.register = sr.toSparse();
                setIfBigger(register, bucket, (byte) firstOnePos);
            }
        } else {
            setIfBigger(register, bucket, (byte) firstOnePos);
            toDenseIfNeeded();
        }
    }

    private void setIfBigger(Register register, int pos, byte value) {
        byte b = register.get(pos);
        if (value > b) {
            register.set(pos, value);
        }
    }

    private void toDenseIfNeeded() {
        if (register.getRegisterType() == RegisterType.SPARSE) {
            if (isDense(register.getSize())) {
                register = ((SparseRegister) register).toDense(p);
            }
        }
    }

    public void merge(HLLCounter another) {
        assert this.p == another.p;
        assert this.hashFunc == another.hashFunc;
        switch (register.getRegisterType()) {
        case SINGLE_VALUE:
            switch (another.getRegisterType()) {
            case SINGLE_VALUE:
                if (register.getSize() > 0 && another.register.getSize() > 0) {
                    register = ((SingleValueRegister) register).toSparse();
                } else {
                    SingleValueRegister sr = (SingleValueRegister) another.register;
                    if (sr.getSize() > 0)
                        register.set(sr.getSingleValuePos(), sr.getValue());
                    return;
                }
                break;
            case SPARSE:
                register = ((SingleValueRegister) register).toSparse();
                break;
            case DENSE:
                register = ((SingleValueRegister) register).toDense(this.p);
                break;
            default:
                break;
            }

            break;
        case SPARSE:
            if (another.getRegisterType() == RegisterType.DENSE) {
                register = ((SparseRegister) register).toDense(p);
            }
            break;
        default:
            break;
        }
        register.merge(another.register);
        toDenseIfNeeded();
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

    // a memory efficient snapshot of HLL registers which can yield count estimate later
    public static class HLLCSnapshot {
        byte p;
        double registerSum;
        int zeroBuckets;

        public HLLCSnapshot(HLLCounter hllc) {
            p = (byte) hllc.p;
            registerSum = 0;
            zeroBuckets = 0;
            Register register = hllc.getRegister();
            DenseRegister dr;
            if (register.getRegisterType() == RegisterType.SINGLE_VALUE) {
                dr = ((SingleValueRegister) register).toDense(p);
            } else if (register.getRegisterType() == RegisterType.SPARSE) {
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
            double rate = new HLLCounter(p, RegisterType.SPARSE).getErrorRate();
            double er = Math.round(rate * 10000) / 100D;
            double er2 = Math.round(rate * 2 * 10000) / 100D;
            double er3 = Math.round(rate * 3 * 10000) / 100D;
            long size = Math.round(Math.pow(2, p));
            System.out.println("HLLC" + p + ",\t" + size + " bytes,\t68% err<" + er + "%" + ",\t95% err<" + er2 + "%"
                    + ",\t99.7% err<" + er3 + "%");
        }
    }

    public Register getRegister() {
        return register;
    }

    public void clear() {
        register.clear();
    }

    // ============================================================================

    public void writeRegisters(final ByteBuffer out) throws IOException {

        final int indexLen = getRegisterIndexSize();
        int size = register.getSize();

        // decide output scheme -- map (3*size bytes) or array (2^p bytes)
        byte scheme;
        if (register instanceof SingleValueRegister || register instanceof SparseRegister //
                || 5 + (indexLen + 1) * size < m) {
            scheme = 0; // map
        } else {
            scheme = 1; // array
        }
        out.put(scheme);
        if (scheme == 0) { // map scheme
            BytesUtil.writeVInt(size, out);
            if (register.getRegisterType() == RegisterType.SINGLE_VALUE) { //single value register
                if (size > 0) {
                    SingleValueRegister sr = (SingleValueRegister) register;
                    writeUnsigned(sr.getSingleValuePos(), indexLen, out);
                    out.put(sr.getValue());
                }
            } else if (register.getRegisterType() == RegisterType.SPARSE) { //sparse register
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
            if (isDense(size)) {
                register = new DenseRegister(p);
            } else if (size <= 1) {
                register = new SingleValueRegister();
            } else {
                register = new SparseRegister();
            }
            int indexLen = getRegisterIndexSize();
            int key = 0;
            for (int i = 0; i < size; i++) {
                key = readUnsigned(in, indexLen);
                register.set(key, in.get());
            }
        } else if (scheme == 1) { // array scheme
            if (register.getRegisterType() != RegisterType.DENSE) {
                register = new DenseRegister(p);
            }
            in.get(((DenseRegister) register).getRawRegister());
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
        result = prime * result + register.hashCode();
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
        HLLCounter other = (HLLCounter) obj;
        if (!hashFunc.equals(other.hashFunc))
            return false;
        if (p != other.p)
            return false;
        if (!register.equals(other.register))
            return false;
        return true;
    }

    @Override
    public int compareTo(HLLCounter o) {
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

    public RegisterType getRegisterType() {
        return register.getRegisterType();
    }

}
