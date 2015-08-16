/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job.tools;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;

public class GridTableHBaseBenchmark {

    private static final String TEST_TABLE = "GridTableTest";
    private static final byte[] CF = "F".getBytes();
    private static final byte[] QN = "C".getBytes();
    private static final int N_ROWS = 10000;
    private static final int CELL_SIZE = 128 * 1024; // 128 KB
    private static final double DFT_HIT_RATIO = 0.3;
    private static final double DFT_INDEX_RATIO = 0.1;
    private static final int ROUND = 3;

    public static void main(String[] args) throws IOException {
        double hitRatio = DFT_HIT_RATIO;
        try {
            hitRatio = Double.parseDouble(args[0]);
        } catch (Exception e) {
            // nevermind
        }

        double indexRatio = DFT_INDEX_RATIO;
        try {
            indexRatio = Double.parseDouble(args[1]);
        } catch (Exception e) {
            // nevermind
        }

        testGridTable(hitRatio, indexRatio);
    }

    public static void testGridTable(double hitRatio, double indexRatio) throws IOException {
        System.out.println("Testing grid table scanning, hit ratio " + hitRatio + ", index ratio " + indexRatio);
        String hbaseUrl = "hbase"; // use hbase-site.xml on classpath
        Connection conn = HBaseConnection.get(hbaseUrl);
        createHTableIfNeeded(conn, TEST_TABLE);
        prepareData(conn);

        Hits hits = new Hits(N_ROWS, hitRatio, indexRatio);

        for (int i = 0; i < ROUND; i++) {
            System.out.println("==================================== ROUND " + (i + 1) + " ========================================");
            testRowScanWithIndex(conn, hits.getHitsForRowScanWithIndex());
            testRowScanNoIndexFullScan(conn, hits.getHitsForRowScanNoIndex());
            testRowScanNoIndexSkipScan(conn, hits.getHitsForRowScanNoIndex());
            testColumnScan(conn, hits.getHitsForColumnScan());
        }

    }

    private static void testColumnScan(Connection conn, List<Pair<Integer, Integer>> colScans) throws IOException {
        Stats stats = new Stats("COLUMN_SCAN");

        Table table = conn.getTable(TableName.valueOf(TEST_TABLE));
        try {
            stats.markStart();

            int nLogicCols = colScans.size();
            int nLogicRows = colScans.get(0).getSecond() - colScans.get(0).getFirst();

            Scan[] scans = new Scan[nLogicCols];
            ResultScanner[] scanners = new ResultScanner[nLogicCols];
            for (int i = 0; i < nLogicCols; i++) {
                scans[i] = new Scan();
                scans[i].addFamily(CF);
                scanners[i] = table.getScanner(scans[i]);
            }
            for (int i = 0; i < nLogicRows; i++) {
                for (int c = 0; c < nLogicCols; c++) {
                    Result r = scanners[c].next();
                    stats.consume(r);
                }
                dot(i, nLogicRows);
            }

            stats.markEnd();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private static void testRowScanNoIndexFullScan(Connection conn, boolean[] hits) throws IOException {
        fullScan(conn, hits, new Stats("ROW_SCAN_NO_IDX_FULL"));
    }

    private static void testRowScanNoIndexSkipScan(Connection conn, boolean[] hits) throws IOException {
        jumpScan(conn, hits, new Stats("ROW_SCAN_NO_IDX_SKIP"));
    }

    private static void testRowScanWithIndex(Connection conn, boolean[] hits) throws IOException {
        jumpScan(conn, hits, new Stats("ROW_SCAN_IDX"));
    }

    private static void fullScan(Connection conn, boolean[] hits, Stats stats) throws IOException {
        Table table = conn.getTable(TableName.valueOf(TEST_TABLE));
        try {
            stats.markStart();

            Scan scan = new Scan();
            scan.addFamily(CF);
            ResultScanner scanner = table.getScanner(scan);
            int i = 0;
            for (Result r : scanner) {
                if (hits[i])
                    stats.consume(r);
                dot(i, N_ROWS);
                i++;
            }

            stats.markEnd();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private static void jumpScan(Connection conn, boolean[] hits, Stats stats) throws IOException {

        final int jumpThreshold = 6; // compensate for Scan() overhead, totally by experience

        Table table = conn.getTable(TableName.valueOf(TEST_TABLE));
        try {

            stats.markStart();

            int i = 0;
            while (i < N_ROWS) {
                int start, end;
                for (start = i; start < N_ROWS; start++) {
                    if (hits[start])
                        break;
                }
                for (end = start + 1; end < N_ROWS; end++) {
                    boolean isEnd = true;
                    for (int j = 0; j < jumpThreshold && end + j < N_ROWS; j++)
                        if (hits[end + j])
                            isEnd = false;
                    if (isEnd)
                        break;
                }

                if (start < N_ROWS) {
                    Scan scan = new Scan();
                    scan.setStartRow(Bytes.toBytes(start));
                    scan.setStopRow(Bytes.toBytes(end));
                    scan.addFamily(CF);
                    ResultScanner scanner = table.getScanner(scan);
                    i = start;
                    for (Result r : scanner) {
                        stats.consume(r);
                        dot(i, N_ROWS);
                        i++;
                    }
                }
                i = end;
            }

            stats.markEnd();

        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private static void prepareData(Connection conn) throws IOException {
        Table table = conn.getTable(TableName.valueOf(TEST_TABLE));

        try {
            // check how many rows existing
            int nRows = 0;
            Scan scan = new Scan();
            scan.setFilter(new KeyOnlyFilter());
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                r.getRow(); // nothing to do
                nRows++;
            }

            if (nRows > 0) {
                System.out.println(nRows + " existing rows");
                if (nRows != N_ROWS)
                    throw new IOException("Expect " + N_ROWS + " rows but it is not");
                return;
            }

            // insert rows into empty table
            System.out.println("Writing " + N_ROWS + " rows to " + TEST_TABLE);
            long nBytes = 0;
            for (int i = 0; i < N_ROWS; i++) {
                byte[] rowkey = Bytes.toBytes(i);
                Put put = new Put(rowkey);
                byte[] cell = randomBytes();
                put.addColumn(CF, QN, cell);
                table.put(put);
                nBytes += cell.length;
                dot(i, N_ROWS);
            }
            System.out.println();
            System.out.println("Written " + N_ROWS + " rows, " + nBytes + " bytes");

        } finally {
            IOUtils.closeQuietly(table);
        }

    }

    private static void dot(int i, int nRows) {
        if (i % (nRows / 100) == 0)
            System.out.print(".");
    }

    private static byte[] randomBytes() {
        byte[] bytes = new byte[CELL_SIZE];
        Random rand = new Random();
        rand.nextBytes(bytes);
        return bytes;
    }

    private static void createHTableIfNeeded(Connection conn, String tableName) throws IOException {
        Admin hbase = conn.getAdmin();

        try {
            boolean tableExist = false;
            try {
                hbase.getTableDescriptor(TableName.valueOf(tableName));
                tableExist = true;
            } catch (TableNotFoundException e) {
            }

            if (tableExist) {
                System.out.println("HTable '" + tableName + "' already exists");
                return;
            }

            System.out.println("Creating HTable '" + tableName + "'");

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

            HColumnDescriptor fd = new HColumnDescriptor(CF);
            fd.setBlocksize(CELL_SIZE);
            desc.addFamily(fd);
            hbase.createTable(desc);

            System.out.println("HTable '" + tableName + "' created");
        } finally {
            hbase.close();
        }
    }

    static class Hits {

        boolean[] hitsForRowScanWithIndex;
        boolean[] hitsForRowScanNoIndex;
        List<Pair<Integer, Integer>> hitsForColumnScan;

        public Hits(int nRows, double hitRatio, double indexRatio) {
            Random rand = new Random();

            hitsForRowScanWithIndex = new boolean[nRows];
            hitsForRowScanNoIndex = new boolean[nRows];

            // for row scan
            int blockSize = (int) (1.0 / indexRatio);
            int nBlocks = nRows / blockSize;

            for (int i = 0; i < nBlocks; i++) {

                if (rand.nextDouble() < hitRatio) {
                    for (int j = 0; j < blockSize; j++) {
                        hitsForRowScanNoIndex[i * blockSize + j] = true;
                        hitsForRowScanWithIndex[i * blockSize + j] = true;
                    }
                } else {
                    // case of not hit
                    hitsForRowScanNoIndex[i * blockSize] = true;
                }
            }

            hitsForColumnScan = Lists.newArrayList();

            // for column scan
            int nColumns = 20;
            int logicRows = nRows / nColumns;
            for (int i = 0; i < nColumns; i++) {
                if (rand.nextDouble() < hitRatio) {
                    hitsForColumnScan.add(new Pair<Integer, Integer>(i * logicRows, (i + 1) * logicRows));
                }
            }

        }

        public boolean[] getHitsForRowScanWithIndex() {
            return hitsForRowScanWithIndex;
        }

        public boolean[] getHitsForRowScanNoIndex() {
            return hitsForRowScanNoIndex;
        }

        public List<Pair<Integer, Integer>> getHitsForColumnScan() {
            return hitsForColumnScan;
        }
    }

    static class Stats {
        String name;
        long startTime;
        long endTime;
        long rowsRead;
        long bytesRead;

        public Stats(String name) {
            this.name = name;
        }

        public void consume(Result r) {
            consume(r, Integer.MAX_VALUE);
        }

        private void consume(Result r, int nBytesToConsume) {
            Cell cell = r.getColumnLatestCell(CF, QN);
            byte mix = 0;
            byte[] valueArray = cell.getValueArray();
            int n = Math.min(nBytesToConsume, cell.getValueLength());
            for (int i = 0; i < n; i++) {
                mix ^= valueArray[i];
                bytesRead++;
            }
            discard(mix);
            rowsRead++;
        }

        private void discard(byte n) {
            // do nothing
        }

        public void markStart() {
            System.out.println(name + " starts");
            startTime = System.currentTimeMillis();
        }

        public void markEnd() {
            endTime = System.currentTimeMillis();
            System.out.println();
            System.out.println(name + " ends, " + (endTime - startTime) + " ms, " + rowsRead + " rows read, " + bytesRead + " bytes read");
        }
    }

}
