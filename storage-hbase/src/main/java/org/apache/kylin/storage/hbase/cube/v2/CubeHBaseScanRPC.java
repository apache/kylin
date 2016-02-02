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
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
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

    public CubeHBaseScanRPC(CubeSegment cubeSeg, Cuboid cuboid, GTInfo fullGTInfo) {
        super(cubeSeg, cuboid, fullGTInfo);
    }

    @Override
    public IGTScanner getGTScanner(final List<GTScanRequest> scanRequests) throws IOException {
        final List<IGTScanner> scanners = Lists.newArrayList();
        for (GTScanRequest request : scanRequests) {
            scanners.add(getGTScanner(request));
        }

        return new IGTScanner() {
            @Override
            public GTInfo getInfo() {
                return scanners.get(0).getInfo();
            }

            @Override
            public int getScannedRowCount() {
                int sum = 0;
                for (IGTScanner s : scanners) {
                    sum += s.getScannedRowCount();
                }
                return sum;
            }

            @Override
            public void close() throws IOException {
                for (IGTScanner s : scanners) {
                    s.close();
                }
            }

            @Override
            public Iterator<GTRecord> iterator() {
                return Iterators.concat(Iterators.transform(scanners.iterator(), new Function<IGTScanner, Iterator<GTRecord>>() {
                    @Nullable
                    @Override
                    public Iterator<GTRecord> apply(IGTScanner input) {
                        return input.iterator();
                    }
                }));
            }
        };
    }

    private IGTScanner getGTScanner(final GTScanRequest scanRequest) throws IOException {

        // primary key (also the 0th column block) is always selected
        final ImmutableBitSet selectedColBlocks = scanRequest.getSelectedColBlocks().set(0);
        // globally shared connection, does not require close
        HConnection hbaseConn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());
        final HTableInterface hbaseTable = hbaseConn.getTable(cubeSeg.getStorageLocationIdentifier());

        List<RawScan> rawScans = preparedHBaseScans(scanRequest.getPkStart(), scanRequest.getPkEnd(), scanRequest.getFuzzyKeys(), selectedColBlocks);
        List<List<Integer>> hbaseColumnsToGT = getHBaseColumnsGTMapping(selectedColBlocks);

        final List<ResultScanner> scanners = Lists.newArrayList();
        final List<Iterator<Result>> resultIterators = Lists.newArrayList();

        for (RawScan rawScan : rawScans) {

            logScan(rawScan, cubeSeg.getStorageLocationIdentifier());
            Scan hbaseScan = buildScan(rawScan);

            final ResultScanner scanner = hbaseTable.getScanner(hbaseScan);
            final Iterator<Result> iterator = scanner.iterator();

            scanners.add(scanner);
            resultIterators.add(iterator);
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

        IGTStore store = new HBaseReadonlyStore(cellListIterator, scanRequest, rawScans.get(0).hbaseColumns, hbaseColumnsToGT, cubeSeg.getRowKeyPreambleSize());
        IGTScanner rawScanner = store.scan(scanRequest);

        final IGTScanner decorateScanner = scanRequest.decorateScanner(rawScanner);
        final TrimmedInfoGTRecordAdapter trimmedInfoGTRecordAdapter = new TrimmedInfoGTRecordAdapter(fullGTInfo, decorateScanner.iterator());

        return new IGTScanner() {
            @Override
            public GTInfo getInfo() {
                return fullGTInfo;
            }

            @Override
            public int getScannedRowCount() {
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
