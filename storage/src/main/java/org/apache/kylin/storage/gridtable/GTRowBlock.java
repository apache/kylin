package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

class GTRowBlock {

    private static final int PRIMARY_KEY_CAPACITY = 2048;
    private static final int CELL_BLOCK_CAPACITY = 128 * 1024;
    
    int seqId; // 0, 1, 2...
    int nRows;
    ByteBuffer primaryKey; // the primary key of the first row
    ByteBuffer[] cellBlocks; // cells for each column block
    
    public GTRowBlock(GTInfo info) {
        primaryKey = ByteBuffer.allocate(PRIMARY_KEY_CAPACITY);
        cellBlocks = new ByteBuffer[info.nColBlocks];
        for (int i = 0; i < cellBlocks.length; i++) {
            cellBlocks[i] = ByteBuffer.allocate(CELL_BLOCK_CAPACITY);
        }
    }
    
    public boolean isEmpty() {
        return nRows == 0;
    }
    
    public void clear() {
        nRows = 0;
        primaryKey.clear();
        for (int i = 0; i < cellBlocks.length; i++) {
            cellBlocks[i].clear();
        }
    }
}
