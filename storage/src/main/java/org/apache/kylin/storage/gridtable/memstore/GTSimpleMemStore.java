package org.apache.kylin.storage.gridtable.memstore;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRowBlock;
import org.apache.kylin.storage.gridtable.GTRowBlockIndex;
import org.apache.kylin.storage.gridtable.IGTStore;

public class GTSimpleMemStore implements IGTStore {
    
    final GTInfo info;
    final List<GTRowBlock> rowBlockList;

    public GTSimpleMemStore(GTInfo info) {
        this.info = info;
        this.rowBlockList = new ArrayList<GTRowBlock>();
        
        if (info.isShardingEnabled())
            throw new UnsupportedOperationException();
    }
    
    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public String getStorageDescription() {
        return this.toString();
    }

    @Override
    public IGTStoreWriter rebuild(int shard) {
        
        rowBlockList.clear();
        
        return new IGTStoreWriter() {
            @Override
            public void close() throws IOException {
            }

            @Override
            public void write(GTRowBlock block) throws IOException {
                rowBlockList.add(block.copy());
            }
        };
    }

    @Override
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEndExclusive, ConciseSet selectedRowBlocks, BitSet selectedColBlocks) {
        
        return new IGTStoreScanner() {
            Iterator<GTRowBlock> it = rowBlockList.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public GTRowBlock next() {
                GTRowBlock block = it.next();
                block.rewindBuffers();
                return block;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public void saveRowBlockIndex(int col, GTRowBlockIndex index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GTRowBlockIndex loadRowBlockIndex(int col) {
        throw new UnsupportedOperationException();
    }

}
