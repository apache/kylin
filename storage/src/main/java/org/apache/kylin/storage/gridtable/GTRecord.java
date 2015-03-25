package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.kylin.common.util.ByteArray;

public class GTRecord implements Comparable<GTRecord> {

    final GTInfo info;
    final ByteArray[] cols;

    private BitSet maskForEqualHashComp;

    public GTRecord(GTInfo info) {
        this.info = info;
        this.cols = new ByteArray[info.nColumns];
        for (int i = 0; i < cols.length; i++)
            this.cols[i] = new ByteArray();
        this.maskForEqualHashComp = info.colAll;
    }
    
    public ByteArray get(int i) {
        return cols[i];
    }

    public void set(int i, ByteArray data) {
        cols[i].set(data.array(), data.offset(), data.length());
    }

    /** set record to the codes of specified values, new space allocated to hold the codes */
    public GTRecord setValues(Object... values) {
        setValues(new ByteArray(info.maxRecordLength), values);
        return this;
    }

    /** set record to the codes of specified values, reuse given space to hold the codes */
    public GTRecord setValues(ByteArray space, Object... values) {
        ByteBuffer buf = space.asBuffer();
        int pos = buf.position();
        for (int i = 0; i < info.nColumns; i++) {
            info.codeSystem.encodeColumnValue(i, values[i], buf);
            int newPos = buf.position();
            cols[i].set(buf.array(), buf.arrayOffset() + pos, newPos - pos);
            pos = newPos;
        }
        return this;
    }

    /** decode and return the values of this record */
    public Object[] getValues() {
        return getValues(new Object[info.nColumns]);
    }

    /** decode and return the values of this record */
    public Object[] getValues(Object[] result) {
        for (int i = 0; i < info.nColumns; i++) {
            if (cols[i].array() == null)
                result[i] = null;
            else
                result[i] = info.codeSystem.decodeColumnValue(i, cols[i].asBuffer());
        }
        return result;
    }

    public GTRecord copy() {
        return copy(info.colAll);
    }

    public GTRecord copy(BitSet selectedCols) {
        int len = 0;
        for (int i = selectedCols.nextSetBit(0); i >= 0; i = selectedCols.nextSetBit(i + 1)) {
            len += cols[i].length();
        }

        byte[] space = new byte[len];

        GTRecord copy = new GTRecord(info);
        copy.maskForEqualHashComp = this.maskForEqualHashComp;
        int pos = 0;
        for (int i = selectedCols.nextSetBit(0); i >= 0; i = selectedCols.nextSetBit(i + 1)) {
            System.arraycopy(cols[i].array(), cols[i].offset(), space, pos, cols[i].length());
            copy.cols[i].set(space, pos, cols[i].length());
            pos += cols[i].length();
        }

        return copy;
    }

    public BitSet maskForEqualHashComp() {
        return maskForEqualHashComp;
    }
    
    public void maskForEqualHashComp(BitSet set) {
        this.maskForEqualHashComp = set;
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
        if (this.maskForEqualHashComp != o.maskForEqualHashComp)
            return false;
        for (int i = maskForEqualHashComp.nextSetBit(0); i >= 0; i = maskForEqualHashComp.nextSetBit(i + 1)) {
            if (this.cols[i].equals(o.cols[i]) == false)
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = maskForEqualHashComp.nextSetBit(0); i >= 0; i = maskForEqualHashComp.nextSetBit(i + 1)) {
            hash = (31 * hash) + cols[i].hashCode();
        }
        return hash;
    }

    @Override
    public int compareTo(GTRecord o) {
        assert this.maskForEqualHashComp == o.maskForEqualHashComp; // reference equal for performance
        
        int comp = 0;
        for (int i = maskForEqualHashComp.nextSetBit(0); i >= 0; i = maskForEqualHashComp.nextSetBit(i + 1)) {
            comp = this.cols[i].compareTo(o.cols[i]);
            if (comp != 0)
                return comp;
        }
        return comp;
    }
    
    @Override
    public String toString() {
        return Arrays.toString(getValues());
    }

    // ============================================================================

    ByteArray exportColumns(BitSet selectedCols) {
        int len = 0;
        for (int i = selectedCols.nextSetBit(0); i >= 0; i = selectedCols.nextSetBit(i + 1)) {
            len += cols[i].length();
        }

        ByteArray buf = ByteArray.allocate(len);
        exportColumns(info.primaryKey, buf);
        return buf;
    }

    /** write data to given buffer, like serialize */
    void exportColumns(BitSet selectedCols, ByteArray buf) {
        int pos = 0;
        for (int i = selectedCols.nextSetBit(0); i >= 0; i = selectedCols.nextSetBit(i + 1)) {
            System.arraycopy(cols[i].array(), cols[i].offset(), buf.array(), buf.offset() + pos, cols[i].length());
            pos += cols[i].length();
        }
        buf.setLength(pos);
    }

    /** write data to given buffer, like serialize */
    void exportColumnBlock(int c, ByteBuffer buf) {
        BitSet setselectedCols = info.colBlocks[c];
        for (int i = setselectedCols.nextSetBit(0); i >= 0; i = setselectedCols.nextSetBit(i + 1)) {
            buf.put(cols[i].array(), cols[i].offset(), cols[i].length());
        }
    }

    /** change pointers to point to data in given buffer, UNLIKE deserialize */
    void loadPrimaryKey(ByteBuffer buf) {
        loadColumns(info.primaryKey, buf);
    }

    /** change pointers to point to data in given buffer, UNLIKE deserialize */
    void loadCellBlock(int c, ByteBuffer buf) {
        loadColumns(info.colBlocks[c], buf);
    }

    /** change pointers to point to data in given buffer, UNLIKE deserialize */
    void loadColumns(BitSet selectedCols, ByteBuffer buf) {
        int pos = buf.position();
        for (int i = selectedCols.nextSetBit(0); i >= 0; i = selectedCols.nextSetBit(i + 1)) {
            int len = info.codeSystem.codeLength(i, buf);
            cols[i].set(buf.array(), buf.arrayOffset() + pos, len);
            pos += len;
            buf.position(pos);
        }
    }

}
