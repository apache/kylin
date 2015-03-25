package org.apache.kylin.storage.gridtable;

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

class GTFilterScanner implements IGTScanner {

    final GTInfo info;
    final IGTStoreScanner storeScanner;
    final TupleFilter filter;
    final BitSet selectedColBlocks;

    private GTRowBlock.Reader curBlockReader;
    private GTRecord next;
    final private GTRecord oneRecord; // avoid instance creation
    final private TupleAdapter oneTuple; // avoid instance creation
    
    private int scannedRowCount = 0;
    private int scannedRowBlockCount = 0;

    GTFilterScanner(GTInfo info, IGTStore store, GTScanRequest req) {
        this.info = info;
        this.filter = req.getFilterPushDown();

        if (TupleFilter.isEvaluableRecursively(filter) == false)
            throw new IllegalArgumentException();

        ByteArray start = req.getPkStart() == null ? null : req.getPkStart().exportColumns(info.primaryKey);
        ByteArray endEx = req.getPkEnd() == null ? null : req.getPkEnd().exportColumns(info.primaryKey);
        this.selectedColBlocks = info.selectColumnBlocks(req.getColumns());

        this.storeScanner = store.scan(start, endEx, selectedColBlocks, filter);
        this.oneRecord = new GTRecord(info);
        this.oneTuple = new TupleAdapter(oneRecord);
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }
    
    @Override
    public int getScannedRowCount() {
        return scannedRowCount;
    }

    @Override
    public int getScannedRowBlockCount() {
        return scannedRowBlockCount;
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
                while (true) {
                    // get a block
                    if (curBlockReader == null) {
                        if (storeScanner.hasNext()) {
                            curBlockReader = storeScanner.next().getReader(selectedColBlocks);
                            scannedRowBlockCount++;
                        } else {
                            return false;
                        }
                    }
                    // if block exhausted, try next block
                    if (curBlockReader.hasNext() == false) {
                        curBlockReader = null;
                        continue;
                    }
                    // fetch a row
                    curBlockReader.fetchNext(oneRecord);
                    scannedRowCount++;
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
            return r.get(col.getColumn().getZeroBasedIndex());
        }

    }

}
