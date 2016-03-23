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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * for test use only
 */
public class CubeHBaseScanRPC extends CubeHBaseRPC {
    public static final Logger logger = LoggerFactory.getLogger(CubeHBaseScanRPC.class);

    static class TrimmedInfoGTRecordAdapter implements Iterable<GTRecord> {

        private final GTInfo info;
        private final Iterator<GTRecord> input;

        public TrimmedInfoGTRecordAdapter(GTInfo info, Iterator<GTRecord> input) {
            this.info = info;
            this.input = input;
        }

        @Override
        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {
                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }

                @Override
                public GTRecord next() {
                    GTRecord x = input.next();
                    return new GTRecord(info, x.getInternal());
                }

                @Override
                public void remove() {

                }
            };
        }
    }

    public CubeHBaseScanRPC(ISegment segment, Cuboid cuboid, final GTInfo fullGTInfo) {
        super(segment, cuboid, fullGTInfo);
    }

    @Override
    public IGTScanner getGTScanner(final GTScanRequest scanRequest) throws IOException {
        final IGTScanner scanner = getGTScannerInternal(scanRequest);

        return new IGTScanner() {
            @Override
            public GTInfo getInfo() {
                return scanner.getInfo();
            }

            @Override
            public long getScannedRowCount() {
                long sum = 0;
                sum += scanner.getScannedRowCount();
                return sum;
            }

            @Override
            public void close() throws IOException {
                scanner.close();
            }

            @Override
            public Iterator<GTRecord> iterator() {
                return scanner.iterator();
            }
        };
    }

    //for non-sharding cases it will only return one byte[] with not shard at beginning
    private List<byte[]> getRowKeysDifferentShards(byte[] halfCookedKey) {
        final short cuboidShardNum = cubeSeg.getCuboidShardNum(cuboid.getId());

        if (!cubeSeg.isEnableSharding()) {
            return Lists.newArrayList(halfCookedKey);//not shard to append at head, so it is already well cooked
        } else {
            List<byte[]> ret = Lists.newArrayList();
            for (short i = 0; i < cuboidShardNum; ++i) {
                short shard = ShardingHash.normalize(cubeSeg.getCuboidBaseShard(cuboid.getId()), i, cubeSeg.getTotalShards(cuboid.getId()));
                byte[] cookedKey = Arrays.copyOf(halfCookedKey, halfCookedKey.length);
                BytesUtil.writeShort(shard, cookedKey, 0, RowConstants.ROWKEY_SHARDID_LEN);
                ret.add(cookedKey);
            }
            return ret;
        }
    }

    private List<RawScan> spawnRawScansForAllShards(RawScan rawScan) {
        List<RawScan> ret = Lists.newArrayList();
        List<byte[]> startKeys = getRowKeysDifferentShards(rawScan.startKey);
        List<byte[]> endKeys = getRowKeysDifferentShards(rawScan.endKey);
        for (int i = 0; i < startKeys.size(); i++) {
            RawScan temp = new RawScan(rawScan);
            temp.startKey = startKeys.get(i);
            temp.endKey = endKeys.get(i);
            ret.add(temp);
        }
        return ret;
    }

    private IGTScanner getGTScannerInternal(final GTScanRequest scanRequest) throws IOException {

        // primary key (also the 0th column block) is always selected
        final ImmutableBitSet selectedColBlocks = scanRequest.getSelectedColBlocks().set(0);
        // globally shared connection, does not require close
        Connection hbaseConn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());
        final Table hbaseTable = hbaseConn.getTable(TableName.valueOf(cubeSeg.getStorageLocationIdentifier()));

        List<RawScan> rawScans = preparedHBaseScans(scanRequest.getGTScanRanges(), selectedColBlocks);
        List<List<Integer>> hbaseColumnsToGT = getHBaseColumnsGTMapping(selectedColBlocks);

        final List<ResultScanner> scanners = Lists.newArrayList();
        final List<Iterator<Result>> resultIterators = Lists.newArrayList();

        for (RawScan rawScan : rawScans) {
            for (RawScan rawScanWithShard : spawnRawScansForAllShards(rawScan)) {
                logScan(rawScanWithShard, cubeSeg.getStorageLocationIdentifier());
                Scan hbaseScan = buildScan(rawScanWithShard);

                final ResultScanner scanner = hbaseTable.getScanner(hbaseScan);
                final Iterator<Result> iterator = scanner.iterator();

                scanners.add(scanner);
                resultIterators.add(iterator);
            }
        }

        final Iterator<Result> allResultsIterator = Iterators.concat(resultIterators.iterator());

        CellListIterator cellListIterator = new CellListIterator() {
            @Override
            public void close() throws IOException {
                for (ResultScanner scanner : scanners) {
                    scanner.close();
                }
                hbaseTable.close();
            }

            @Override
            public boolean hasNext() {
                return allResultsIterator.hasNext();
            }

            @Override
            public List<Cell> next() {
                return allResultsIterator.next().listCells();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        IGTStore store = new HBaseReadonlyStore(cellListIterator, scanRequest, rawScans.get(0).hbaseColumns, hbaseColumnsToGT, cubeSeg.getRowKeyPreambleSize(), false);
        IGTScanner rawScanner = store.scan(scanRequest);

        final IGTScanner decorateScanner = scanRequest.decorateScanner(rawScanner);
        final TrimmedInfoGTRecordAdapter trimmedInfoGTRecordAdapter = new TrimmedInfoGTRecordAdapter(fullGTInfo, decorateScanner.iterator());

        return new IGTScanner() {
            @Override
            public GTInfo getInfo() {
                return fullGTInfo;
            }

            @Override
            public long getScannedRowCount() {
                return decorateScanner.getScannedRowCount();
            }

            @Override
            public void close() throws IOException {
                decorateScanner.close();
            }

            @Override
            public Iterator<GTRecord> iterator() {
                return trimmedInfoGTRecordAdapter.iterator();
            }
        };
    }
}
