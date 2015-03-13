package org.apache.kylin.storage.gridtable;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.apache.kylin.storage.gridtable.IGTStore.IGTStoreScanner;

class GTRawScanner implements IGTScanner {

    final GTInfo info;
    final IGTStoreScanner storeScanner;
    final TupleFilter filter;
    final BitSet selectedColBlocks;

    private GTRowBlock currentBlock;
    private int currentRow;
    private GTRecord next;
    final private GTRecord oneRecord; // avoid instance creation
    final private TupleAdapter oneTuple; // avoid instance creation

    GTRawScanner(GTInfo info, IGTStore store, GTRecord pkStart, GTRecord pkEndExclusive, BitSet dimensions, BitSet metrics, TupleFilter filter) {
        this.info = info;
        this.filter = filter;

        ByteArray start = pkStart.exportColumns(info.primaryKey);
        ByteArray endEx = pkEndExclusive.exportColumns(info.primaryKey);

        ConciseSet selectedRowBlocks = computeHitRowBlocks(start, endEx, filter);
        this.selectedColBlocks = computeHitColumnBlocks(dimensions, metrics);

        this.storeScanner = store.scan(start, endEx, selectedRowBlocks, selectedColBlocks);
        this.oneRecord = new GTRecord(info);
        this.oneTuple = new TupleAdapter(oneRecord);
    }

    private BitSet computeHitColumnBlocks(BitSet dimensions, BitSet metrics) {
        BitSet result = new BitSet();
        for (int i = 0; i < info.colBlocks.length; i++) {
            BitSet cb = info.colBlocks[i];
            if (cb.intersects(dimensions) || cb.intersects(metrics)) {
                result.set(i);
            }
        }
        return result;
    }

    private ConciseSet computeHitRowBlocks(ByteArray start, ByteArray endEx, TupleFilter filter) {
        // TODO block level index
        return null;
    }

    @Override
    public void close() throws IOException {
        storeScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return new Iterator<GTRecord>() {

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;

                IFilterCodeSystem<ByteArray> filterCodeSystem = info.codeSystem.getFilterCodeSystem();

                while (fetchNext()) {
                    if (filter != null && filter.evaluate(oneTuple, filterCodeSystem) == false) {
                        continue;
                    }
                    next = oneRecord;
                    return true;
                }
                return false;
            }

            private boolean fetchNext() {
                if (info.isRowBlockEnabled()) {
                    return fetchNextRowBlockEnabled();
                } else {
                    return fetchNextRowBlockDisabled();
                }
            }

            private boolean fetchNextRowBlockDisabled() {
                // row block disabled, every block is one row
                if (storeScanner.hasNext() == false)
                    return false;

                // when row block disabled, PK is persisted in block primary key (not in cell block)
                currentBlock = storeScanner.next();
                oneRecord.loadPrimaryKey(currentBlock.primaryKeyBuffer);
                for (int c = selectedColBlocks.nextSetBit(0); c >= 0; c = selectedColBlocks.nextSetBit(c + 1)) {
                    oneRecord.loadCellBlock(c, currentBlock.cellBlockBuffers[c]);
                }
                return true;
            }

            private boolean fetchNextRowBlockEnabled() {
                while (true) {
                    // get a block
                    if (currentBlock == null) {
                        if (storeScanner.hasNext()) {
                            currentBlock = storeScanner.next();
                            currentRow = 0;
                        } else {
                            return false;
                        }
                    }
                    // if block exhausted, try next block
                    if (currentRow >= currentBlock.nRows) {
                        currentBlock = null;
                        continue;
                    }
                    // fetch a row
                    for (int c = selectedColBlocks.nextSetBit(0); c >= 0; c = selectedColBlocks.nextSetBit(c + 1)) {
                        oneRecord.loadCellBlock(c, currentBlock.cellBlockBuffers[c]);
                    }
                    return true;
                }
            }

            @Override
            public GTRecord next() {
                // fetch next record
                if (next == null) {
                    hasNext();
                    if (next == null)
                        throw new NoSuchElementException();
                }

                GTRecord result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    private static class TupleAdapter implements IEvaluatableTuple {

        private GTRecord r;

        private TupleAdapter(GTRecord r) {
            this.r = r;
        }

        @Override
        public Object getValue(TblColRef col) {
            return r.cols[col.getColumn().getZeroBasedIndex()];
        }

    }
}
