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

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;

import com.google.common.base.Preconditions;

public class GTRecord implements Comparable<GTRecord>, Cloneable {

    final transient GTInfo info;
    final ByteArray[] cols;

    public GTRecord(GTInfo info, ByteArray[] cols) {
        this.info = info;
        this.cols = cols;
    }

    public GTRecord(GTInfo info) {
        this.cols = new ByteArray[info.getColumnCount()];
        for (int i = 0; i < this.cols.length; i++) {
            // consider column projection by pass in another bit set
            this.cols[i] = new ByteArray();
        }
        this.info = info;
    }
    
    @Override
    public GTRecord clone() { // deep copy
        ByteArray[] cols = new ByteArray[this.cols.length];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = this.cols[i].copy();
        }
        return new GTRecord(this.info, cols);
    }

    public void shallowCopyFrom(GTRecord source) {
        assert info == source.info;
        for (int i = 0; i < cols.length; i++) {
            cols[i].set(source.cols[i]);
        }
    }

    public GTInfo getInfo() {
        return info;
    }

    public ByteArray get(int i) {
        return cols[i];
    }

    public ByteArray[] getInternal() {
        return cols;
    }

    public void set(int i, ByteArray data) {
        cols[i].set(data.array(), data.offset(), data.length());
    }

    /** set record to the codes of specified values, new space allocated to hold the codes */
    public GTRecord setValues(Object... values) {
        setValues(info.colAll, new ByteArray(info.getMaxRecordLength()), values);
        return this;
    }

    /** set record to the codes of specified values, reuse given space to hold the codes */
    public GTRecord setValues(ImmutableBitSet selectedCols, ByteArray space, Object... values) {
        assert selectedCols.cardinality() == values.length;

        ByteBuffer buf = space.asBuffer();
        int pos = buf.position();
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            info.codeSystem.encodeColumnValue(c, values[i], buf);
            int newPos = buf.position();
            cols[c].set(buf.array(), buf.arrayOffset() + pos, newPos - pos);
            pos = newPos;
        }
        return this;
    }

    /** decode and return the values of this record */
    public Object[] getValues() {
        return getValues(info.colAll, new Object[info.getColumnCount()]);
    }

    /** decode and return the values of this record */
    public Object[] getValues(ImmutableBitSet selectedCols, Object[] result) {
        assert selectedCols.cardinality() == result.length;
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            result[i] = decodeValue(selectedCols.trueBitAt(i));
        }
        return result;
    }


    /** decode and return the values of this record */
    public Object[] getValues(int[] selectedColumns, Object[] result) {
        assert selectedColumns.length <= result.length;
        for (int i = 0; i < selectedColumns.length; i++) {
            result[i] = decodeValue(selectedColumns[i]);
        }
        return result;
    }

    public Object decodeValue(int c) {
        ByteArray col = cols[c];
        if (col != null && col.array() != null) {
            return info.codeSystem.decodeColumnValue(c, col.asBuffer());
        }
        return null;
    }

    public int sizeOf(ImmutableBitSet selectedCols) {
        int size = 0;
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            size += cols[c].length();
        }
        return size;
    }

    public GTRecord copy() {
        return copy(info.colAll);
    }

    public GTRecord copy(ImmutableBitSet selectedCols) {
        int len = sizeOf(selectedCols);
        byte[] space = new byte[len];

        GTRecord copy = new GTRecord(info);
        int pos = 0;
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            System.arraycopy(cols[c].array(), cols[c].offset(), space, pos, cols[c].length());
            copy.cols[c].set(space, pos, cols[c].length());
            pos += cols[c].length();
        }

        return copy;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        GTRecord o = (GTRecord) obj;
        if (this.info != o.info)
            return false;

        for (int i = 0; i < info.colAll.trueBitCount(); i++) {
            int c = info.colAll.trueBitAt(i);
            if (!this.cols[c].equals(o.cols[c])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = 0; i < info.colAll.trueBitCount(); i++) {
            int c = info.colAll.trueBitAt(i);
            hash = (31 * hash) + cols[c].hashCode();
        }
        return hash;
    }

    @Override
    public int compareTo(GTRecord o) {
        return compareToInternal(o, info.colAll);
    }

    public static Comparator<GTRecord> getComparator(final ImmutableBitSet participateCols) {
        return new Comparator<GTRecord>() {
            public int compare(GTRecord o1, GTRecord o2) {
                if (o1 == null || o2 == null) {
                    throw new IllegalStateException("Cannot handle null");
                }
                return o1.compareToInternal(o2, participateCols);
            }
        };
    }

    private int compareToInternal(GTRecord o, ImmutableBitSet participateCols) {
        assert this.info == o.info; // reference equal for performance
        IGTComparator comparator = info.codeSystem.getComparator();

        int comp = 0;
        for (int i = 0; i < participateCols.trueBitCount(); i++) {
            int c = participateCols.trueBitAt(i);
            comp = comparator.compare(cols[c], o.cols[c]);
            if (comp != 0)
                return comp;
        }
        return comp;
    }

    @Override
    public String toString() {
        return toString(info.colAll);
    }

    /** toString for MemoryHungry Measure is expensive, please invoke carefully */
    public String toString(ImmutableBitSet selectedColumns) {
        Object[] values = new Object[selectedColumns.cardinality()];
        getValues(selectedColumns, values);
        return Arrays.toString(values);
    }

    // ============================================================================

    public ByteArray exportColumns(ImmutableBitSet selectedCols) {
        int len = sizeOf(selectedCols);

        ByteArray buf = ByteArray.allocate(len);
        exportColumns(selectedCols, buf);
        return buf;
    }

    /** write data to given buffer, like serialize */
    public void exportColumns(ImmutableBitSet selectedCols, ByteArray buf) {
        int pos = 0;
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            Preconditions.checkNotNull(cols[c].array());
            System.arraycopy(cols[c].array(), cols[c].offset(), buf.array(), buf.offset() + pos, cols[c].length());
            pos += cols[c].length();
        }
        buf.setLength(pos);
    }

    /** write data to given buffer, like serialize */
    public void exportColumns(ImmutableBitSet selectedCols, ByteBuffer buf) {
        for (int i = 0; i < selectedCols.trueBitCount(); i++) {
            int c = selectedCols.trueBitAt(i);
            buf.put(cols[c].array(), cols[c].offset(), cols[c].length());
        }
    }

    public void exportColumns(int[] fieldIndex, ByteBuffer buf) {
        for (int i : fieldIndex) {
            buf.put(cols[i].array(), cols[i].offset(), cols[i].length());
        }
    }

    /** write data to given buffer, like serialize */
    public void exportColumnBlock(int c, ByteBuffer buf) {
        exportColumns(info.colBlocks[c], buf);
    }

    /** change pointers to point to data in given buffer, UNLIKE deserialize */
    public void loadCellBlock(int c, ByteBuffer buf) {
        loadColumns(info.colBlocks[c], buf);
    }

    /**
     * Change pointers to point to data in given buffer, UNLIKE deserialize
     * @param selectedCols positions of column to load
     * @param buf buffer containing continuous data of selected columns
     */
    public void loadColumns(Iterable<Integer> selectedCols, ByteBuffer buf) {
        int pos = buf.position();
        for (int c : selectedCols) {
            int len = info.codeSystem.codeLength(c, buf);
            cols[c].set(buf.array(), buf.arrayOffset() + pos, len);
            pos += len;
            buf.position(pos);
        }
    }

}
