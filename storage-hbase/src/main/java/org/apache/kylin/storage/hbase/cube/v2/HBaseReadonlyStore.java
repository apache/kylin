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

package org.apache.kylin.storage.hbase.cube.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.gridtable.IGTWriter;

import com.google.common.base.Preconditions;

public class HBaseReadonlyStore implements IGTStore {

    private CellListIterator cellListIterator;

    private GTInfo info;
    private List<Pair<byte[], byte[]>> hbaseColumns;
    private List<List<Integer>> hbaseColumnsToGT;
    private int rowkeyPreambleSize;
    private boolean withDelay = false;


    /**
     * @param withDelay is for test use
     */
    public HBaseReadonlyStore(CellListIterator cellListIterator, GTScanRequest gtScanRequest, List<Pair<byte[], byte[]>> hbaseColumns, List<List<Integer>> hbaseColumnsToGT, int rowkeyPreambleSize, boolean withDelay) {
        this.cellListIterator = cellListIterator;
        this.info = gtScanRequest.getInfo();
        this.hbaseColumns = hbaseColumns;
        this.hbaseColumnsToGT = hbaseColumnsToGT;
        this.rowkeyPreambleSize = rowkeyPreambleSize;
        this.withDelay = withDelay;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public IGTWriter rebuild() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IGTWriter append() throws IOException {
        throw new UnsupportedOperationException();
    }

    //TODO: possible to use binary search as cells might be sorted?
    public static Cell findCell(List<Cell> cells, byte[] familyName, byte[] columnName) {
        for (Cell c : cells) {
            if (BytesUtil.compareBytes(familyName, 0, c.getFamilyArray(), c.getFamilyOffset(), familyName.length) == 0 && //
                    BytesUtil.compareBytes(columnName, 0, c.getQualifierArray(), c.getQualifierOffset(), columnName.length) == 0) {
                return c;
            }
        }
        return null;
    }

    @Override
    public IGTScanner scan(GTScanRequest scanRequest) throws IOException {
        return new IGTScanner() {
            int count;

            @Override
            public void close() throws IOException {
                cellListIterator.close();
            }

            @Override
            public Iterator<GTRecord> iterator() {
                return new Iterator<GTRecord>() {
                    GTRecord oneRecord = new GTRecord(info); // avoid object creation

                    @Override
                    public boolean hasNext() {
                        if (withDelay) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        return cellListIterator.hasNext();
                    }

                    @Override
                    public GTRecord next() {
                        count++;
                        List<Cell> oneRow = cellListIterator.next();
                        if (oneRow.size() < 1) {
                            throw new IllegalStateException("cell list's size less than 1");
                        }

                        // dimensions, set to primary key, also the 0th column block
                        Cell firstCell = oneRow.get(0);
                        ByteBuffer buf = byteBuffer(firstCell.getRowArray(), rowkeyPreambleSize + firstCell.getRowOffset(), firstCell.getRowLength() - rowkeyPreambleSize);
                        oneRecord.loadCellBlock(0, buf);

                        // metrics
                        for (int i = 0; i < hbaseColumns.size(); i++) {
                            Pair<byte[], byte[]> hbaseColumn = hbaseColumns.get(i);
                            Cell cell = findCell(oneRow, hbaseColumn.getFirst(), hbaseColumn.getSecond());
                            Preconditions.checkNotNull(cell);
                            buf = byteBuffer(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                            oneRecord.loadColumns(hbaseColumnsToGT.get(i), buf);
                        }
                        return oneRecord;

                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                    private ByteBuffer byteBuffer(byte[] array, int offset, int length) {
                        return ByteBuffer.wrap(array, offset, length);
                    }

                };
            }

            @Override
            public GTInfo getInfo() {
                return info;
            }
        };
    }
}
