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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.util.KryoUtils;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest.Builder;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitService;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

public class CubeHBaseEndpointRPC extends CubeHBaseRPC {

    static class EndpintResultsAsGTScanner implements IGTScanner {
        private GTInfo info;
        private Iterator<byte[]> blocks;

        public EndpintResultsAsGTScanner(GTInfo info, Iterator<byte[]> blocks) {
            this.info = info;
            this.blocks = blocks;
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

                    logger.info("Reassembling a raw block returned from Endpoint with byte length: " + input.length);
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
                            oneRecord.loadAllColumns(inputBuffer);
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

        try {
            // primary key (also the 0th column block) is always selected
            final ImmutableBitSet selectedColBlocks = scanRequest.getSelectedColBlocks().set(0);
            // globally shared connection, does not require close
            HConnection hbaseConn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());
            final HTableInterface hbaseTable = hbaseConn.getTable(cubeSeg.getStorageLocationIdentifier());
            final List<Pair<byte[], byte[]>> hbaseColumns = makeHBaseColumns(selectedColBlocks);

            RawScan rawScan = prepareRawScan(scanRequest.getPkStart(), scanRequest.getPkEnd(), hbaseColumns);

            byte[] scanRequestBytes = KryoUtils.serialize(scanRequest);
            byte[] rawScanBytes = KryoUtils.serialize(rawScan);
            Builder builder = CubeVisitRequest.newBuilder();
            builder.setGtScanRequest(ByteString.copyFrom(scanRequestBytes)).setHbaseRawScan(ByteString.copyFrom(rawScanBytes));

            Collection<CubeVisitResponse> results = getResults(builder.build(), hbaseTable, rawScan.startKey, rawScan.endKey);
            final Collection<byte[]> rowBlocks = Collections2.transform(results, new Function<CubeVisitResponse, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(CubeVisitResponse input) {
                    try {
                        return CompressionUtils.decompress(input.getCompressedRows().toByteArray());
                    } catch (IOException | DataFormatException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            return new EndpintResultsAsGTScanner(fullGTInfo, rowBlocks.iterator());

        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return null;
    }

    //TODO : async callback
    private Collection<CubeVisitResponse> getResults(final CubeVisitRequest request, HTableInterface table, byte[] startKey, byte[] endKey) throws Throwable {
        Map<byte[], CubeVisitResponse> results = table.coprocessorService(CubeVisitService.class, startKey, endKey, new Batch.Call<CubeVisitService, CubeVisitResponse>() {
            public CubeVisitResponse call(CubeVisitService rowsService) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<CubeVisitResponse> rpcCallback = new BlockingRpcCallback<>();
                rowsService.visitCube(controller, request, rpcCallback);
                CubeVisitResponse response = rpcCallback.get();
                if (controller.failedOnException()) {
                    throw controller.getFailedOn();
                }
                return response;
            }
        });

        logger.info("{} regions returned results ", results.values().size());

        return results.values();
    }
}
