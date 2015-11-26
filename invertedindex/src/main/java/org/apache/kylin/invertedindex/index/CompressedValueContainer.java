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
import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * @author yangli9
 */
public class CompressedValueContainer implements ColumnValueContainer {
    int valueLen;
    int nValues;
    int cap;
    int size;
    byte[] uncompressed;
    byte[] compressed;

    public CompressedValueContainer(TableRecordInfoDigest digest, int col, int cap) {
        this(digest.length(col), digest.getMaxID(col) + 1, cap);
    }

    public CompressedValueContainer(int valueLen, int nValues, int cap) {
        this.valueLen = valueLen;
        this.nValues = nValues;
        this.cap = cap;
        this.size = 0;
        this.uncompressed = null;
        this.compressed = null;
    }

    @Override
    public void append(ImmutableBytesWritable valueBytes) {
        checkUpdateMode();
        System.arraycopy(valueBytes.get(), valueBytes.getOffset(), uncompressed, valueLen * size, valueLen);
        size++;
    }

    @Override
    public void getValueAt(int i, ImmutableBytesWritable valueBytes) {
        valueBytes.set(uncompressed, valueLen * i, valueLen);
    }

    @Override
    public ConciseSet getBitMap(Integer startId, Integer endId) {
        ConciseSet ret = new ConciseSet();
        int nullId = Dictionary.NULL_ID[valueLen];

        if (startId == null && endId == null) {
            //entry for getting null values 
            for (int i = 0; i < size; ++i) {
                int valueID = BytesUtil.readUnsigned(uncompressed, i * valueLen, valueLen);
                if (nullId == valueID) {
                    ret.add(i);
                }
            }
            return ret;
        }

        //normal values
        for (int i = 0; i < size; ++i) {
            int valueID = BytesUtil.readUnsigned(uncompressed, i * valueLen, valueLen);
            if (valueID == nullId) {
                continue;
            }

            if (startId != null && valueID < startId) {
                continue;
            }

            if (endId != null && valueID > endId) {
                continue;
            }

            ret.add(i);
        }
        return ret;

    }

    @Override
    public int getMaxValueId() {
        return nValues - 1;
    }

    private void checkUpdateMode() {
        if (isClosedForChange()) {
            throw new IllegalArgumentException();
        }
        if (uncompressed == null) {
            uncompressed = new byte[valueLen * cap];
        }
    }

    private boolean isClosedForChange() {
        return compressed != null;
    }

    @Override
    public void closeForChange() {
        checkUpdateMode();
        try {
            compressed = LZFEncoder.encode(uncompressed, 0, valueLen * size);
        } catch (Exception e) {
            throw new RuntimeException("LZF encode failure", e);
        }
    }

    @Override
    public int getSize() {
        return size;
    }

    public ImmutableBytesWritable toBytes() {
        if (isClosedForChange() == false)
            closeForChange();
        return new ImmutableBytesWritable(compressed);
    }

    public void fromBytes(ImmutableBytesWritable bytes) {
        try {
            uncompressed = LZFDecoder.decode(bytes.get(), bytes.getOffset(), bytes.getLength());
        } catch (IOException e) {
            throw new RuntimeException("LZF decode failure", e);
        }
        size = cap = uncompressed.length / valueLen;
        compressed = BytesUtil.EMPTY_BYTE_ARRAY; // mark closed
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + size;
        result = prime * result + valueLen;
        result = prime * result + Arrays.hashCode(uncompressed);
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
        CompressedValueContainer other = (CompressedValueContainer) obj;
        if (size != other.size)
            return false;
        if (valueLen != other.valueLen)
            return false;
        if (!Bytes.equals(uncompressed, 0, size * valueLen, uncompressed, 0, size * valueLen))
            return false;
        return true;
    }

}
