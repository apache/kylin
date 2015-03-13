package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.kylin.common.util.ByteArray;

public class GTRecord implements Comparable<GTRecord> {

    final GTInfo info;
    final ByteArray[] cols;
    
    BitSet maskForEqualHashComp;
    
    public GTRecord(GTInfo info) {
        this.info = info;
        this.cols = new ByteArray[info.nColumns];
        for (int i = 0; i < cols.length; i++)
            this.cols[i] = new ByteArray();
        this.maskForEqualHashComp = info.colAll;
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
        int comp = 0;
        for (int i = maskForEqualHashComp.nextSetBit(0); i >= 0; i = maskForEqualHashComp.nextSetBit(i + 1)) {
            comp = this.cols[i].compareTo(o.cols[i]);
            if (comp != 0)
                return comp;
        }
        return comp;
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
            buf.position(pos + len);
        }
    }

}
