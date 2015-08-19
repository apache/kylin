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

package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRowBlock;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTStoreBridgeScanner;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;

public class HBaseGTStore implements IGTStore {

    private CellListIterator cellListIterator;

    private GTInfo info;
    private List<Pair<byte[], byte[]>> hbaseColumns;
    private ImmutableBitSet selectedColBlocks;

    private GTRowBlock oneBlock;

    public HBaseGTStore(CellListIterator cellListIterator, GTScanRequest gtScanRequest, List<Pair<byte[], byte[]>> hbaseColumns) {
        this.cellListIterator = cellListIterator;

        this.info = gtScanRequest.getInfo();
        this.hbaseColumns = hbaseColumns;
        this.selectedColBlocks = gtScanRequest.getSelectedColBlocks().set(0);

        oneBlock = new GTRowBlock(info); // avoid object creation
        oneBlock.setNumberOfRows(1); // row block is always disabled for cubes, row block contains only one record 
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public IGTStoreWriter rebuild(int shard) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IGTScanner scan(GTScanRequest scanRequest) throws IOException {
        return new GTStoreBridgeScanner(info, scanRequest, oldScan(scanRequest));
    }

    private IGTStoreScanner oldScan(GTScanRequest scanRequest) throws IOException {
        return new IGTStoreScanner() {
            @Override
            public boolean hasNext() {
                return cellListIterator.hasNext();
            }

            @Override
            public GTRowBlock next() {
                List<Cell> oneRow = cellListIterator.next();
                if (oneRow.size() < 1) {
                    throw new IllegalStateException("cell list's size less than 1");
                }

                // dimensions, set to primary key, also the 0th column block
                Cell firstCell = oneRow.get(0);
                byte[] rowkey = firstCell.getRowArray();
                oneBlock.getPrimaryKey().set(rowkey, RowConstants.ROWKEY_CUBOIDID_LEN + firstCell.getRowOffset(), firstCell.getRowLength() - RowConstants.ROWKEY_CUBOIDID_LEN);
                oneBlock.getCellBlock(0).set(rowkey, RowConstants.ROWKEY_CUBOIDID_LEN + firstCell.getRowOffset(), firstCell.getRowLength() - RowConstants.ROWKEY_CUBOIDID_LEN);

                // metrics
                int hbaseColIdx = 0;
                for (int i = 1; i < selectedColBlocks.trueBitCount(); i++) {
                    int colBlockIdx = selectedColBlocks.trueBitAt(i);
                    Pair<byte[], byte[]> hbaseColumn = hbaseColumns.get(hbaseColIdx++);
                    Cell cell = CubeHBaseRPC.findCell(oneRow, hbaseColumn.getFirst(), hbaseColumn.getSecond());
                    oneBlock.getCellBlock(colBlockIdx).set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
                return oneBlock;

            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
                cellListIterator.close();
            }
        };
    }
}
