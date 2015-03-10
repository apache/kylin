package org.apache.kylin.storage.gridtable;

import org.apache.kylin.metadata.model.DataType;

public class GTInfo {

    // column schema
    int nColumns;
    DataType[] colType;
    boolean[] colIsMetrics;
    int maxRecordLength; // column length can vary
    
    // grid info
    int[] primaryKey; // columns sorted and unique
    int rowBlockSize; // 0: no row block
    int nColBlocks;
    int[] colBlockCuts; // [i]: the start index of i.th block; [i+1]: the exclusive end index of i.th block
    
    // sharding & rowkey
    int nShards; // 0: no sharding
    byte[] rowkeyPrefix;
    
    public boolean isShardingEnabled() {
        return nShards > 0;
    }
    
    public boolean isRowBlockEnabled() {
        return rowBlockSize > 0;
    }
    
    void validate() {
        if (nColBlocks != colBlockCuts.length - 1) {
            throw new IllegalStateException();
        }
        
        for (int i = 0; i < nColBlocks; i++) {
            int cbStart = colBlockCuts[i], cbEnd = colBlockCuts[i + 1];
            if (cbStart >= cbEnd)
                throw new IllegalStateException();
            
            // when row block disabled, primary key is persisted on rowkey, thus shall not repeat in column block
            if (isRowBlockEnabled() == false) {
                for (int ii : primaryKey) {
                    if (ii >= cbStart && ii < cbEnd)
                        throw new IllegalStateException();
                }
            }
        }
        
    }
    
}
