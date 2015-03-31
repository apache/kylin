package org.apache.kylin.storage.gridtable.memstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRowBlock;
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
        return new Writer();
    }

    @Override
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) {
        if (rowBlockList.size() > 0) {
            GTRowBlock last = rowBlockList.get(rowBlockList.size() - 1);
            fillLast.copyFrom(last);
        }
        return new Writer();
    }

    private class Writer implements IGTStoreWriter {
        @Override
        public void close() throws IOException {
        }

        @Override
        public void write(GTRowBlock block) throws IOException {
            GTRowBlock copy = block.copy();
            int id = block.sequenceId();
            if (id < rowBlockList.size()) {
                rowBlockList.set(id, copy);
            } else {
                assert id == rowBlockList.size();
                rowBlockList.add(copy);
            }
        }
    };

    @Override
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, TupleFilter filterPushDown) {

        return new IGTStoreScanner() {
            Iterator<GTRowBlock> it = rowBlockList.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public GTRowBlock next() {
                return it.next();
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

}
