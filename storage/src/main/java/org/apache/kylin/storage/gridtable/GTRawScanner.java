package org.apache.kylin.storage.gridtable;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.storage.gridtable.IGTStore.IGTStoreScanner;

public class GTRawScanner implements IGTScanner {

    final GTInfo info;
    final IGTStoreScanner storeScanner;
    final BitSet selectedColBlocks;

    private GTRowBlock.Reader curBlockReader;
    private GTRecord next;
    final private GTRecord oneRecord; // avoid instance creation

    private int scannedRowCount = 0;
    private int scannedRowBlockCount = 0;

    public GTRawScanner(GTInfo info, IGTStore store, GTScanRequest req) throws IOException {
        this.info = info;

        ByteArray start = makeScanKey(req.getPkStart());
        ByteArray end = makeScanKey(req.getPkEnd());
        this.selectedColBlocks = info.selectColumnBlocks(req.getColumns());

        this.storeScanner = store.scan(start, end, selectedColBlocks, req);
        this.oneRecord = new GTRecord(info);
    }

    private ByteArray makeScanKey(GTRecord rec) {
        int firstPKCol = info.primaryKey.nextSetBit(0);
        if (rec == null || rec.cols[firstPKCol].array() == null)
            return null;

        BitSet selectedColumns = new BitSet();
        int len = 0;
        for (int i = info.primaryKey.nextSetBit(0); i >= 0; i = info.primaryKey.nextSetBit(i + 1)) {
            if (rec.cols[i].array() == null) {
                break;
            }
            selectedColumns.set(i);
            len += rec.cols[i].length();
        }

        ByteArray buf = ByteArray.allocate(len);
        rec.exportColumns(selectedColumns, buf);
        return buf;
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

                if (fetchOneRecord()) {
                    next = oneRecord;
                    return true;
                } else {
                    return false;
                }
            }

            private boolean fetchOneRecord() {
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

}
