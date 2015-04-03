package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.kylin.common.util.ByteArray;

public class GTRowBlock {

    /** create a row block, allocate memory, get ready for writing */
    public static GTRowBlock allocate(GTInfo info) {
        GTRowBlock b = new GTRowBlock(info);

        byte[] array = new byte[info.getMaxColumnLength(info.primaryKey)];
        b.primaryKey.set(array);

        int maxRows = info.isRowBlockEnabled() ? info.rowBlockSize : 1;
        for (int i = 0; i < b.cellBlocks.length; i++) {
            array = new byte[info.getMaxColumnLength(info.colBlocks[i]) * maxRows];
            b.cellBlocks[i].set(array);
        }
        return b;
    }

    final GTInfo info;

    int seqId; // 0, 1, 2...
    int nRows;
    ByteArray primaryKey; // the primary key of the first (smallest) row
    ByteArray[] cellBlocks; // cells for each column block

    /** create a row block that has no underlying space */
    public GTRowBlock(GTInfo info) {
        this.info = info;
        primaryKey = new ByteArray();
        cellBlocks = new ByteArray[info.colBlocks.length];
        for (int i = 0; i < cellBlocks.length; i++) {
            cellBlocks[i] = new ByteArray();
        }
    }
    
    public int sequenceId() {
        return seqId;
    }
    
    public Writer getWriter() {
        return new Writer();
    }
    
    public class Writer {
        ByteBuffer[] cellBlockBuffers;
        
        Writer() {
            cellBlockBuffers = new ByteBuffer[info.colBlocks.length];
            for (int i = 0; i < cellBlockBuffers.length; i++) {
                cellBlockBuffers[i] = cellBlocks[i].asBuffer();
            }
        }
        
        public void copyFrom(GTRowBlock other) {
            assert info == other.info;
            
            seqId = other.seqId;
            nRows = other.nRows;
            primaryKey.copyFrom(other.primaryKey);
            for (int i = 0; i < info.colBlocks.length; i++) {
                cellBlockBuffers[i].clear();
                cellBlockBuffers[i].put(other.cellBlocks[i].array(), other.cellBlocks[i].offset(), other.cellBlocks[i].length());
            }
        }

        public void append(GTRecord r) {
            // add record to block
            if (isEmpty()) {
                r.exportColumns(info.primaryKey, primaryKey);
            }
            for (int i = 0; i < info.colBlocks.length; i++) {
                r.exportColumnBlock(i, cellBlockBuffers[i]);
            }
            nRows++;
        }
        
        public void readyForFlush() {
            for (int i = 0; i < cellBlocks.length; i++) {
                cellBlocks[i].setLength(cellBlockBuffers[i].position());
            }
        }

        public void clearForNext() {
            seqId++;
            nRows = 0;
            for (int i = 0; i < cellBlockBuffers.length; i++) {
                cellBlockBuffers[i].clear();
            }
        }
    }
    
    public Reader getReader() {
        return new Reader(info.colBlocksAll);
    }
    
    public Reader getReader(BitSet selectedColBlocks) {
        return new Reader(selectedColBlocks);
    }
    
    public class Reader {
        int cur;
        ByteBuffer primaryKeyBuffer;
        ByteBuffer[] cellBlockBuffers;
        BitSet selectedColBlocks;
        
        Reader(BitSet selectedColBlocks) {
            primaryKeyBuffer = primaryKey.asBuffer();
            cellBlockBuffers = new ByteBuffer[info.colBlocks.length];
            for (int i = 0; i < cellBlockBuffers.length; i++) {
                cellBlockBuffers[i] = cellBlocks[i].asBuffer();
            }
            this.selectedColBlocks = selectedColBlocks;
        }
        
        public boolean hasNext() {
            return cur < nRows;
        }
        
        public void fetchNext(GTRecord result) {
            if (hasNext() == false)
                throw new IllegalArgumentException();
            
            for (int c = selectedColBlocks.nextSetBit(0); c >= 0; c = selectedColBlocks.nextSetBit(c + 1)) {
                result.loadCellBlock(c, cellBlockBuffers[c]);
            }
            cur++;
        }
    }

    public GTRowBlock copy() {
        GTRowBlock copy = new GTRowBlock(info);

        ByteBuffer buf = ByteBuffer.allocate(this.exportLength());
        this.export(buf);
        buf.clear();
        copy.load(buf);

        return copy;
    }
    
    public boolean isEmpty() {
        return nRows == 0;
    }

    public boolean isFull() {
        if (info.isRowBlockEnabled())
            return nRows >= info.rowBlockSize;
        else
            return nRows > 0;
    }

    // TODO export / load should optimize for disabled row block
    
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
        for (int i = 0; i < info.colBlocks.length; i++) {
            ByteArray cb = cellBlocks[i];
            load(cb, buf);
        }
    }

    private void load(ByteArray array, ByteBuffer buf) {
        int len = buf.getInt();
        int pos = buf.position();
        array.set(buf.array(), buf.arrayOffset() + pos, len);
        buf.position(pos + len);
    }

}
