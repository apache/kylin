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

package org.apache.kylin.invertedindex.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;

import org.roaringbitmap.RoaringBitmap;

/**
 * @author yangli9
 */
public class BitMapContainer implements ColumnValueContainer {

    int valueLen;
    int nValues;
    int size;
    RoaringBitmap[] sets;
    boolean closedForChange;

    transient byte[] temp;

    public BitMapContainer(TableRecordInfoDigest digest, int col) {
        this.valueLen = digest.length(col);
        this.size = 0;
        this.nValues = digest.getMaxID(col) + 1;
        this.sets = null;
        this.closedForChange = false;

        this.temp = new byte[valueLen];
    }

    @Override
    public void append(ImmutableBytesWritable valueBytes) {
        int value = BytesUtil.readUnsigned(valueBytes.get(), valueBytes.getOffset(), valueLen);
        append(value);
    }

    public void append(int value) {
        checkUpdateMode();
        if (value == Dictionary.NULL_ID[valueLen]) {
            value = nValues; // set[nValues] holds NULL
        }
        sets[value].add(size);
        size++;
    }

    @Override
    public void getValueAt(int i, ImmutableBytesWritable valueBytes) {
        int value = getValueIntAt(i);
        BytesUtil.writeUnsigned(value, temp, 0, valueLen);
        valueBytes.set(temp, 0, valueLen);
    }

    @Override
    public RoaringBitmap getBitMap(Integer startId, Integer endId) {
        if (startId == null && endId == null) {
            return sets[this.nValues];
        }

        int start = 0;
        int end = this.nValues - 1;
        if (startId != null) {
            start = startId;
        }
        if (endId != null) {
            end = endId;
        }

        return RoaringBitmap.or(Arrays.copyOfRange(sets, start, end+1));
    }

    private RoaringBitmap getBitMap(int valueId) {
        if (valueId >= 0 && valueId <= getMaxValueId())
            return sets[valueId];
        else
            return sets[this.nValues];
    }

    @Override
    public int getMaxValueId() {
        return this.nValues - 1;
    }

    public int getValueIntAt(int i) {
        for (int v = 0; v < nValues; v++) {
            if (sets[v].contains(i)) {
                return v;
            }
        }
        // if v is not in [0..nValues-1], then it must be nValue (NULL)
        return Dictionary.NULL_ID[valueLen];
    }

    private void checkUpdateMode() {
        if (isClosedForChange()) {
            throw new IllegalStateException();
        }
        if (sets == null) {
            sets = new RoaringBitmap[nValues + 1];
            for (int i = 0; i <= nValues; i++) {
                sets[i] = new RoaringBitmap();
            }
        }
    }

    private boolean isClosedForChange() {
        return closedForChange;
    }

    @Override
    public void closeForChange() {
        closedForChange = true;
    }

    @Override
    public int getSize() {
        return size;
    }

    public List<ImmutableBytesWritable> toBytes() {
        if (isClosedForChange() == false)
            closeForChange();

        List<ImmutableBytesWritable> r = new ArrayList<ImmutableBytesWritable>(nValues + 1);
        for (int i = 0; i <= nValues; i++) {
            r.add(setToBytes(sets[i]));
        }
        return r;
    }

    public void fromBytes(List<ImmutableBytesWritable> bytes) {
        assert nValues + 1 == bytes.size();
        sets = new RoaringBitmap[nValues + 1];
        size = 0;
        for (int i = 0; i <= nValues; i++) {
            sets[i] = bytesToSet(bytes.get(i));
            size += sets[i].getCardinality();
        }
        closedForChange = true;
    }

    private ImmutableBytesWritable setToBytes(RoaringBitmap set) {
        // Serializing a bitmap to a byte array can be expected to be expensive, this should not be commonly done.
        // If the purpose is to save the data to disk or to a network, then a direct serialization would be
        // far more efficient. If the purpose is to enforce immutability, it is an expensive way to do it.
        set.runOptimize(); //to improve compression
        final byte[] array = new byte[set.serializedSizeInBytes()];
        try {
            set.serialize(new java.io.DataOutputStream(new java.io.OutputStream() {
                int c = 0;

                @Override
                public void close() {
                }

                @Override
                public void flush() {
                }

                @Override
                public void write(int b) {
                    array[c++] = (byte)b;
                }

                @Override
                public void write(byte[] b) {
                    write(b, 0, b.length);
                }

                @Override
                public void write(byte[] b, int off, int l) {
                    System.arraycopy(b, off, array, c, l);
                    c += l;
                }
            }));
        } catch (IOException ioe) {
            // should never happen because we write to a byte array
            throw new RuntimeException("unexpected error while serializing to a byte array");
        }

        return new ImmutableBytesWritable(array);
    }

    private RoaringBitmap bytesToSet(final ImmutableBytesWritable bytes) {
        // converting a byte array to a bitmap can be expected to be expensive, hopefully this is not a common operation!
        RoaringBitmap set = new RoaringBitmap();
        if ((bytes.get() != null) && (bytes.getLength() > 0)) {
            // here we could use an ImmutableRoaringBitmap and just "map" it.
            // instead, we do a full deserialization
            // Note: we deserializing a Roaring bitmap, there is no need to know the length, the format is self-describing
            try {
                set.deserialize(new java.io.DataInputStream(new java.io.InputStream() {
                    byte[] array = bytes.get();
                    int c = bytes.getOffset();

                    @Override
                    public int read() {
                        return array[c++] & 0xff;
                    }

                    @Override
                    public int read(byte b[]) {
                        return read(b, 0, b.length);
                    }

                    @Override
                    public int read(byte[] b, int off, int l) {
                        System.arraycopy(array, c, b, off, l);
                        c += l;
                        return l;
                    }
                }));
            } catch (IOException ioe) {
                // should never happen because we read from a byte array
                throw new RuntimeException("unexpected error while deserializing from a byte array");
            }
        }
        return set;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (closedForChange ? 1231 : 1237);
        result = prime * result + nValues;
        result = prime * result + Arrays.hashCode(sets);
        result = prime * result + size;
        result = prime * result + valueLen;
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
        BitMapContainer other = (BitMapContainer) obj;
        if (closedForChange != other.closedForChange)
            return false;
        if (nValues != other.nValues)
            return false;
        if (!Arrays.equals(sets, other.sets))
            return false;
        if (size != other.size)
            return false;
        if (valueLen != other.valueLen)
            return false;
        return true;
    }

}
