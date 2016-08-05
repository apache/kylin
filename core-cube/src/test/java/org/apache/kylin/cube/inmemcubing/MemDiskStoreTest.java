/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.cube.inmemcubing;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.gridtable.GTBuilder;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.UnitTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MemDiskStoreTest extends LocalFileMetadataTestCase {

    final MemoryBudgetController budgetCtrl = new MemoryBudgetController(20);
    final GTInfo info = UnitTestSupport.advancedInfo();
    final List<GTRecord> data = UnitTestSupport.mockupData(info, 1000000); // converts to about 34 MB data

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

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
        MemDiskStore store = new MemDiskStore(info, budgetCtrl);
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

        IGTScanner scanner = table.scan(new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest());
        int i = 0;
        for (GTRecord r : scanner) {
            assertEquals(data.get(i++), r);
        }
        scanner.close();
    }
}
