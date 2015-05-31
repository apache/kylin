package org.apache.kylin.storage.gridtable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.storage.gridtable.memstore.GTMemDiskStore;
import org.apache.kylin.storage.gridtable.memstore.MemoryBudgetController;
import org.junit.Test;

public class GTMemDiskStoreTest {

    final MemoryBudgetController budgetCtrl = new MemoryBudgetController(20);
    final GTInfo info = SimpleGridTableTest.advancedInfo();
    final List<GTRecord> data = SimpleGridTableTest.mockupData(info, 1000000); // converts to about 34 MB data

    @Test
    public void testSingleThreadWriteRead() throws IOException {
        long start = System.currentTimeMillis();
        verifyOneTableWriteAndRead();
        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - start) + " millis");
    }

    @Test
    public void testMultiThreadWriteRead() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        int nThreads = 5;
        Thread[] t = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) {
            t[i] = new Thread() {
                public void run() {
                    try {
                        verifyOneTableWriteAndRead();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            };
            t[i].start();
        }
        for (int i = 0; i < nThreads; i++) {
            t[i].join();
        }

        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - start) + " millis");
    }

    private void verifyOneTableWriteAndRead() throws IOException {
        GTMemDiskStore store = new GTMemDiskStore(info, budgetCtrl);
        GridTable table = new GridTable(info, store);
        verifyWriteAndRead(table);
    }

    private void verifyWriteAndRead(GridTable table) throws IOException {
        GTInfo info = table.getInfo();

        GTBuilder builder = table.rebuild();
        for (GTRecord r : data) {
            builder.write(r);
        }
        builder.close();

        IGTScanner scanner = table.scan(new GTScanRequest(info));
        int i = 0;
        for (GTRecord r : scanner) {
            assertEquals(data.get(i++), r);
        }
        scanner.close();
    }
}
