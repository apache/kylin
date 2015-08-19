package org.apache.kylin.gridtable;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.IGTStore.IGTStoreScanner;

public class GTStoreBridgeScanner implements IGTScanner {

    final GTInfo info;
    final IGTStoreScanner storeScanner;
    final ImmutableBitSet selectedColBlocks;

    private GTRowBlock.Reader curBlockReader;
    private GTRecord next;
    final private GTRecord oneRecord; // avoid instance creation

    private int scannedRowCount = 0;
    private int scannedRowBlockCount = 0;

    public GTStoreBridgeScanner(GTInfo info, GTScanRequest req, IGTStoreScanner storeScanner) throws IOException {
        this.info = info;
        this.selectedColBlocks = req.getSelectedColBlocks();
        this.storeScanner = storeScanner;
        this.oneRecord = new GTRecord(info);
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
