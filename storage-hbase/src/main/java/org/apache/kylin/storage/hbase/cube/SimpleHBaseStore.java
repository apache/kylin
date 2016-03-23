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

package org.apache.kylin.storage.hbase.cube;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.gridtable.IGTWriter;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.steps.CubeHTableUtil;

public class SimpleHBaseStore implements IGTStore {

    static final String CF = "F";
    static final byte[] CF_B = Bytes.toBytes(CF);
    static final String COL = "C";
    static final byte[] COL_B = Bytes.toBytes(COL);
    static final int ID_LEN = RowConstants.ROWKEY_CUBOIDID_LEN;

    final GTInfo info;
    final TableName htableName;

    public SimpleHBaseStore(GTInfo info, TableName htable) {
        this.info = info;
        this.htableName = htable;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    public void cleanup() throws IOException {
        CubeHTableUtil.deleteHTable(htableName);
    }

    @Override
    public IGTWriter rebuild() throws IOException {
        CubeHTableUtil.createBenchmarkHTable(htableName, CF);
        return new Writer();
    }

    @Override
    public IGTWriter append() throws IOException {
        return new Writer();
    }

    @Override
    public IGTScanner scan(GTScanRequest scanRequest) throws IOException {
        return new Reader();
    }

    private class Writer implements IGTWriter {
        final BufferedMutator table;
        final ByteBuffer rowkey = ByteBuffer.allocate(50);
        final ByteBuffer value = ByteBuffer.allocate(50);

        Writer() throws IOException {
            Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
            table = conn.getBufferedMutator(htableName);
        }

        @Override
        public void write(GTRecord rec) throws IOException {
            assert info.getColumnBlockCount() == 2;

            rowkey.clear();
            for (int i = 0; i < ID_LEN; i++) {
                rowkey.put((byte) 0);
            }
            rec.exportColumnBlock(0, rowkey);
            rowkey.flip();

            value.clear();
            rec.exportColumnBlock(1, value);
            value.flip();

            Put put = new Put(rowkey);
            put.addImmutable(CF_B, ByteBuffer.wrap(COL_B), HConstants.LATEST_TIMESTAMP, value);
            table.mutate(put);
        }

        @Override
        public void close() throws IOException {
            table.flush();
            table.close();
        }
    }

    class Reader implements IGTScanner {
        final Table table;
        final ResultScanner scanner;

        int count = 0;

        Reader() throws IOException {
            Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
            table = conn.getTable(htableName);

            Scan scan = new Scan();
            scan.addFamily(CF_B);
            scan.setCaching(1024);
            scan.setCacheBlocks(true);
            scanner = table.getScanner(scan);
        }

        public ResultScanner getHBaseScanner() {
            return scanner;
        }

        @Override
        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {
                GTRecord next = null;
                GTRecord rec = new GTRecord(info);

                @Override
                public boolean hasNext() {
                    if (next != null)
                        return true;

                    try {
                        Result r = scanner.next();
                        if (r != null) {
                            loadRecord(r);
                            next = rec;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return next != null;
                }

                private void loadRecord(Result r) {
                    Cell[] cells = r.rawCells();
                    Cell cell = cells[0];
                    if (Bytes.compareTo(CF_B, 0, CF_B.length, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) != 0 //
                            || Bytes.compareTo(COL_B, 0, COL_B.length, cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) != 0)
                        throw new IllegalStateException();

                    rec.loadCellBlock(0, ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset() + ID_LEN, cell.getRowLength() - ID_LEN));
                    rec.loadCellBlock(1, ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }

                @Override
                public GTRecord next() {
                    if (hasNext() == false)
                        throw new NoSuchElementException();

                    count++;
                    next = null;
                    return rec;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void close() throws IOException {
            scanner.close();
            table.close();
        }

        @Override
        public GTInfo getInfo() {
            return info;
        }

        @Override
        public long getScannedRowCount() {
            return count;
        }
    }
}
