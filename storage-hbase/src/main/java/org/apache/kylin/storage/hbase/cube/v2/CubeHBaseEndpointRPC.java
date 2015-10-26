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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.util.KryoUtils;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest.IntList;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse.Stats;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

public class CubeHBaseEndpointRPC extends CubeHBaseRPC {

    static class EndpointResultsAsGTScanner implements IGTScanner {
        private GTInfo info;
        private Iterator<byte[]> blocks;
        private ImmutableBitSet columns;

        public EndpointResultsAsGTScanner(GTInfo info, Iterator<byte[]> blocks, ImmutableBitSet columns) {
            this.info = info;
            this.blocks = blocks;
            this.columns = columns;
        }

        @Override
        public GTInfo getInfo() {
            return info;
        }

        @Override
        public int getScannedRowCount() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            //do nothing
        }

        @Override
        public Iterator<GTRecord> iterator() {
            return Iterators.concat(Iterators.transform(blocks, new Function<byte[], Iterator<GTRecord>>() {
                @Nullable
                @Override
                public Iterator<GTRecord> apply(@Nullable final byte[] input) {

                    return new Iterator<GTRecord>() {
                        private ByteBuffer inputBuffer = null;
                        private GTRecord oneRecord = null;

                        @Override
                        public boolean hasNext() {
                            if (inputBuffer == null) {
                                inputBuffer = ByteBuffer.wrap(input);
                                oneRecord = new GTRecord(info);
                            }

                            return inputBuffer.position() < inputBuffer.limit();
                        }

                        @Override
                        public GTRecord next() {
                            oneRecord.loadColumns(columns, inputBuffer);
                            return oneRecord;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            }));
        }
    }

    public CubeHBaseEndpointRPC(CubeSegment cubeSeg, Cuboid cuboid, GTInfo fullGTInfo) {
        super(cubeSeg, cuboid, fullGTInfo);
    }

    @Override
    public IGTScanner getGTScanner(final GTScanRequest scanRequest) throws IOException {

        // primary key (also the 0th column block) is always selected
        final ImmutableBitSet selectedColBlocks = scanRequest.getSelectedColBlocks().set(0);
        // globally shared connection, does not require close
        HConnection hbaseConn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());
        final HTableInterface hbaseTable = hbaseConn.getTable(cubeSeg.getStorageLocationIdentifier());

        List<RawScan> rawScans = preparedHBaseScan(scanRequest.getPkStart(), scanRequest.getPkEnd(), scanRequest.getFuzzyKeys(), selectedColBlocks);
        List<List<Integer>> hbaseColumnsToGT = getHBaseColumnsGTMapping(selectedColBlocks);
        final List<IntList> hbaseColumnsToGTIntList = Lists.newArrayList();
        for (List<Integer> list : hbaseColumnsToGT) {
            hbaseColumnsToGTIntList.add(IntList.newBuilder().addAllInts(list).build());
        }

        byte[] scanRequestBytes = KryoUtils.serialize(scanRequest);
        final ByteString scanRequestBytesString = HBaseZeroCopyByteString.wrap(scanRequestBytes);

        ExecutorService executorService = Executors.newFixedThreadPool(rawScans.size());
        final List<byte[]> rowBlocks = Collections.synchronizedList(Lists.<byte[]> newArrayList());

        logger.info("Total RawScan range count: " + rawScans.size());
        for (RawScan rawScan : rawScans) {
            logScan(rawScan, cubeSeg.getStorageLocationIdentifier());
        }

        for (int i = 0; i < rawScans.size(); ++i) {
            final int shardIndex = i;
            final RawScan rawScan = rawScans.get(i);

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    final byte[] rawScanBytes = KryoUtils.serialize(rawScan);
                    CubeVisitProtos.CubeVisitRequest.Builder builder = CubeVisitProtos.CubeVisitRequest.newBuilder();
                    builder.setGtScanRequest(scanRequestBytesString).setHbaseRawScan(HBaseZeroCopyByteString.wrap(rawScanBytes));
                    for (IntList intList : hbaseColumnsToGTIntList) {
                        builder.addHbaseColumnsToGT(intList);
                    }

                    Collection<CubeVisitProtos.CubeVisitResponse> results;
                    try {
                        results = getResults(builder.build(), hbaseTable, rawScan.startKey, rawScan.endKey);
                    } catch (Throwable throwable) {
                        throw new RuntimeException("Error when visiting cubes by endpoint:", throwable);
                    }

                    //results.size() supposed to be 1;
                    if (results.size() != 1) {
                        logger.warn("{} CubeVisitResponse returned for shard {}", results.size(), shardIndex);
                    }

                    for (CubeVisitProtos.CubeVisitResponse result : results) {
                        logger.info(getStatsString(result, shardIndex));
                    }

                    Collection<byte[]> part = Collections2.transform(results, new Function<CubeVisitProtos.CubeVisitResponse, byte[]>() {
                        @Nullable
                        @Override
                        public byte[] apply(CubeVisitProtos.CubeVisitResponse input) {
                            try {
                                return CompressionUtils.decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(input.getCompressedRows()));
                            } catch (IOException | DataFormatException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    rowBlocks.addAll(part);
                }
            });
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                throw new RuntimeException("Visiting cube by endpoint timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Visiting cube by endpoint gets interrupted");
        }

        return new EndpointResultsAsGTScanner(fullGTInfo, rowBlocks.iterator(), scanRequest.getColumns());
    }

    private String getStatsString(CubeVisitProtos.CubeVisitResponse result, int shardIndex) {
        StringBuilder sb = new StringBuilder();
        Stats stats = result.getStats();
        sb.append("Shard " + shardIndex + ": ");
        sb.append("Total scanned row: " + stats.getScannedRowCount() + ". ");
        sb.append("Total filtered/aggred row: " + stats.getAggregatedRowCount() + ". ");
        sb.append("Time elapsed in EP: " + (stats.getServiceEndTime() - stats.getServiceStartTime()) + "(ms). ");
        return sb.toString();

    }

    private Collection<CubeVisitProtos.CubeVisitResponse> getResults(final CubeVisitProtos.CubeVisitRequest request, HTableInterface table, byte[] startKey, byte[] endKey) throws Throwable {
        Map<byte[], CubeVisitProtos.CubeVisitResponse> results = table.coprocessorService(CubeVisitProtos.CubeVisitService.class, startKey, endKey, new Batch.Call<CubeVisitProtos.CubeVisitService, CubeVisitProtos.CubeVisitResponse>() {
            public CubeVisitProtos.CubeVisitResponse call(CubeVisitProtos.CubeVisitService rowsService) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<CubeVisitProtos.CubeVisitResponse> rpcCallback = new BlockingRpcCallback<>();
                rowsService.visitCube(controller, request, rpcCallback);
                CubeVisitProtos.CubeVisitResponse response = rpcCallback.get();
                if (controller.failedOnException()) {
                    throw controller.getFailedOn();
                }
                return response;
            }
        });

        return results.values();
    }
}
