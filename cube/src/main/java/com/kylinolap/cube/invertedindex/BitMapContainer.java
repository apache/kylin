/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube.invertedindex;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.dict.Dictionary;

/**
 * @author yangli9
 * 
 */
public class BitMapContainer implements ColumnValueContainer {

    int valueLen;
    int nValues;
    int size;
    ConciseSet[] sets;
    boolean closedForChange;
    
    transient byte[] temp;

    public BitMapContainer(TableRecordInfoDigest info, int col) {
        this.valueLen = info.length(col);
        this.size = 0;
        this.nValues = info.getMaxID(col) + 1;
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
            sets = new ConciseSet[nValues + 1];
            for (int i = 0; i <= nValues; i++) {
                sets[i] = new ConciseSet();
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
        sets = new ConciseSet[nValues + 1];
        size = 0;
        for (int i = 0; i <= nValues; i++) {
            sets[i] = bytesToSet(bytes.get(i));
            size += sets[i].size();
        }
        closedForChange = true;
    }

    private ImmutableBytesWritable setToBytes(ConciseSet set) {
        byte[] array;
        if (set.isEmpty()) // ConciseSet.toByteBuffer() throws exception when set is empty
            array = BytesUtil.EMPTY_BYTE_ARRAY;
        else
            array = set.toByteBuffer().array();
        return new ImmutableBytesWritable(array);
    }

    private ConciseSet bytesToSet(ImmutableBytesWritable bytes) {
        if (bytes.get() == null || bytes.getLength() == 0) {
            return new ConciseSet();
        } else {
            IntBuffer intBuffer = ByteBuffer.wrap(bytes.get(), bytes.getOffset(), bytes.getLength()).asIntBuffer();
            int[] words = new int[intBuffer.capacity()];
            intBuffer.get(words);
            return new ConciseSet(words, false);
        }
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
