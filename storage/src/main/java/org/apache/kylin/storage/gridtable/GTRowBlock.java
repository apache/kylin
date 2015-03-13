package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;

public class GTRowBlock {
    
    /** create a row block, allocate memory, get ready for incoming data */
    public static GTRowBlock allocate(GTInfo info) {
        GTRowBlock b = new GTRowBlock(info);

        byte[] array = new byte[info.maxRecordLength];
        b.primaryKey.set(array);
        b.primaryKeyBuffer = ByteBuffer.wrap(array);
        
        int maxRows = info.isRowBlockEnabled() ? info.rowBlockSize : 1;
        for (int i = 0; i < b.cellBlocks.length; i++) {
            array = new byte[info.maxRecordLength * maxRows];
            b.cellBlocks[i].set(array);
            b.cellBlockBuffers[i] = ByteBuffer.wrap(array);
        }
        return b;
    }
    
    final GTInfo info;

    int seqId; // 0, 1, 2...
    int nRows;
    ByteArray primaryKey; // the primary key of the first row
    ByteBuffer primaryKeyBuffer;
    ByteArray[] cellBlocks; // cells for each column block
    ByteBuffer[] cellBlockBuffers;
    
    /** create a row block that has no underlying memory */
    public GTRowBlock(GTInfo info) {
        this.info = info;
        primaryKey = new ByteArray();
        cellBlocks = new ByteArray[info.colBlocks.length];
        for (int i = 0; i < cellBlocks.length; i++) {
            cellBlocks[i] = new ByteArray();
        }
        cellBlockBuffers = new ByteBuffer[info.colBlocks.length];
    }
    
    public GTRowBlock copy() {
        GTRowBlock copy = new GTRowBlock(info);
        
        ByteBuffer buf = ByteBuffer.allocate(this.exportLength());
        this.export(buf);
        buf.clear();
        copy.load(buf);
        
        return copy;
    }
    
    public void rewindBuffers() {
        primaryKeyBuffer.rewind();
        for (int i = 0; i < cellBlockBuffers.length; i++) {
            cellBlockBuffers[i].rewind();
        }
    }

    public boolean isEmpty() {
        return nRows == 0;
    }
    
    public void clear() {
        nRows = 0;
        rewindBuffers();
    }
    
    public int exportLength() {
        int len = 4 + 4 + (4 + primaryKey.length());
        for (ByteArray array : cellBlocks) {
            len += 4 + array.length();
        }
        return len;
    }
    
    /** write data to given buffer, like serialize */
    public void export(ByteBuffer buf) {
        buf.putInt(seqId);
        buf.putInt(nRows);
        export(primaryKey, buf);
        for (ByteArray cb : cellBlocks) {
            export(cb, buf);
        }
    }
    
    private void export(ByteArray array, ByteBuffer buf) {
        buf.putInt(array.length());
        buf.put(array.array(), array.offset(), array.length());
    }
    
    /** change pointers to point to data in given buffer, UNLIKE deserialize */
    public void load(ByteBuffer buf) {
        seqId = buf.getInt();
        nRows = buf.getInt();
        load(primaryKey, buf);
        primaryKeyBuffer = ByteBuffer.wrap(primaryKey.array(), primaryKey.offset(), primaryKey.length());
        for (int i = 0; i < info.colBlocks.length; i++) {
            ByteArray cb = cellBlocks[i];
            load(cb, buf);
            cellBlockBuffers[i] = ByteBuffer.wrap(cb.array(), cb.offset(), cb.length());
        }
    }

    private void load(ByteArray array, ByteBuffer buf) {
        int len = buf.getInt();
        int pos = buf.position();
        array.set(buf.array(), buf.arrayOffset() + pos, len);
        buf.position(pos + len);
    }
}
