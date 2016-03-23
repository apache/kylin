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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LoggableCachedThreadPool;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanSelfTerminatedException;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.gtrecord.DummyPartitionStreamer;
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest.IntList;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse.Stats;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

public class CubeHBaseEndpointRPC extends CubeHBaseRPC {

    private static final Logger logger = LoggerFactory.getLogger(CubeHBaseEndpointRPC.class);

    private static ExecutorService executorService = new LoggableCachedThreadPool();

    public CubeHBaseEndpointRPC(ISegment segment, Cuboid cuboid, GTInfo fullGTInfo) {
        super(segment, cuboid, fullGTInfo);
    }

    private byte[] getByteArrayForShort(short v) {
        byte[] split = new byte[Bytes.SIZEOF_SHORT];
        BytesUtil.writeUnsigned(v, split, 0, Bytes.SIZEOF_SHORT);
        return split;
    }

    @SuppressWarnings("unchecked")
    private List<Pair<byte[], byte[]>> getEPKeyRanges(short baseShard, short shardNum, int totalShards) {
        if (shardNum == 0) {
            return Lists.newArrayList();
        }

        if (shardNum == totalShards) {
            //all shards
            return Lists.newArrayList(Pair.newPair(getByteArrayForShort((short) 0), getByteArrayForShort((short) (shardNum - 1))));
        } else if (baseShard + shardNum <= totalShards) {
            //endpoint end key is inclusive, so no need to append 0 or anything
            return Lists.newArrayList(Pair.newPair(getByteArrayForShort(baseShard), getByteArrayForShort((short) (baseShard + shardNum - 1))));
        } else {
            //0,1,2,3,4 wants 4,0
            return Lists.newArrayList(Pair.newPair(getByteArrayForShort(baseShard), getByteArrayForShort((short) (totalShards - 1))), //
                    Pair.newPair(getByteArrayForShort((short) 0), getByteArrayForShort((short) (baseShard + shardNum - totalShards - 1))));
        }
    }

    protected Pair<Short, Short> getShardNumAndBaseShard() {
        return Pair.newPair(cubeSeg.getCuboidShardNum(cuboid.getId()), cubeSeg.getCuboidBaseShard(cuboid.getId()));
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Override
    public IGTScanner getGTScanner(final GTScanRequest scanRequest) throws IOException {

        Pair<Short, Short> shardNumAndBaseShard = getShardNumAndBaseShard();
        short shardNum = shardNumAndBaseShard.getFirst();
        short cuboidBaseShard = shardNumAndBaseShard.getSecond();
        int totalShards = cubeSeg.getTotalShards(cuboid.getId());

        ByteString scanRequestByteString = null;
        ByteString rawScanByteString = null;

        // primary key (also the 0th column block) is always selected
        final ImmutableBitSet selectedColBlocks = scanRequest.getSelectedColBlocks().set(0);

        // globally shared connection, does not require close
        final Connection conn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());

        final List<IntList> hbaseColumnsToGTIntList = Lists.newArrayList();
        List<List<Integer>> hbaseColumnsToGT = getHBaseColumnsGTMapping(selectedColBlocks);
        for (List<Integer> list : hbaseColumnsToGT) {
            hbaseColumnsToGTIntList.add(IntList.newBuilder().addAllInts(list).build());
        }

        //TODO: raw scan can be constructed at region side to reduce traffic
        List<RawScan> rawScans = preparedHBaseScans(scanRequest.getGTScanRanges(), selectedColBlocks);
        rawScanByteString = serializeRawScans(rawScans);

        int coprocessorTimeout = getCoprocessorTimeoutMillis();
        scanRequest.setTimeout(coprocessorTimeout);
        scanRequest.clearScanRanges();//since raw scans are sent to coprocessor, we don't need to duplicate sending it
        scanRequestByteString = serializeGTScanReq(scanRequest);

        final ExpectedSizeIterator epResultItr = new ExpectedSizeIterator(shardNum, coprocessorTimeout);

        logger.info("Serialized scanRequestBytes {} bytes, rawScanBytesString {} bytes", scanRequestByteString.size(), rawScanByteString.size());

        logger.info("The scan {} for segment {} is as below with {} separate raw scans, shard part of start/end key is set to 0", Integer.toHexString(System.identityHashCode(scanRequest)), cubeSeg, rawScans.size());
        for (RawScan rs : rawScans) {
            logScan(rs, cubeSeg.getStorageLocationIdentifier());
        }

        logger.debug("Submitting rpc to {} shards starting from shard {}, scan range count {}", shardNum, cuboidBaseShard, rawScans.size());

        final AtomicLong totalScannedCount = new AtomicLong(0);

        // KylinConfig: use env instance instead of CubeSegment, because KylinConfig will share among queries
        // for different cubes until redeployment of coprocessor jar.
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final boolean compressionResult = kylinConfig.getCompressionResult();
        final CubeVisitProtos.CubeVisitRequest.Builder builder = CubeVisitProtos.CubeVisitRequest.newBuilder();
        builder.setGtScanRequest(scanRequestByteString).setHbaseRawScan(rawScanByteString);
        for (IntList intList : hbaseColumnsToGTIntList) {
            builder.addHbaseColumnsToGT(intList);
        }
        builder.setRowkeyPreambleSize(cubeSeg.getRowKeyPreambleSize());
        builder.setKylinProperties(kylinConfig.getConfigAsString());
        final String queryId = QueryContext.getQueryId();
        if (queryId != null) {
            builder.setQueryId(queryId);
        }

        for (final Pair<byte[], byte[]> epRange : getEPKeyRanges(cuboidBaseShard, shardNum, totalShards)) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {

                    final String logHeader = String.format("<sub-thread for Query %s GTScanRequest %s>", queryId, Integer.toHexString(System.identityHashCode(scanRequest)));
                    final boolean[] abnormalFinish = new boolean[1];

                    try {
                        Table table = conn.getTable(TableName.valueOf(cubeSeg.getStorageLocationIdentifier()), HBaseConnection.getCoprocessorPool());

                        final CubeVisitRequest request = builder.build();
                        final byte[] startKey = epRange.getFirst();
                        final byte[] endKey = epRange.getSecond();

                        table.coprocessorService(CubeVisitService.class, startKey, endKey, //
                                new Batch.Call<CubeVisitService, CubeVisitResponse>() {
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
                                }, new Batch.Callback<CubeVisitResponse>() {
                                    @Override
                                    public void update(byte[] region, byte[] row, CubeVisitResponse result) {
                                        if (region == null)
                                            return;

                                        totalScannedCount.addAndGet(result.getStats().getScannedRowCount());
                                        logger.info(logHeader + getStatsString(region, result));

                                        if (result.getStats().getNormalComplete() != 1) {
                                            abnormalFinish[0] = true;
                                            return;
                                        }
                                        try {
                                            if (compressionResult) {
                                                epResultItr.append(CompressionUtils.decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getCompressedRows())));
                                            } else {
                                                epResultItr.append(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getCompressedRows()));
                                            }
                                        } catch (IOException | DataFormatException e) {
                                            throw new RuntimeException(logHeader + "Error when decompressing", e);
                                        }
                                    }
                                });

                    } catch (Throwable ex) {
                        logger.error(logHeader + "Error when visiting cubes by endpoint", ex); // double log coz the query thread may already timeout
                        epResultItr.notifyCoprocException(ex);
                        return;
                    }

                    if (abnormalFinish[0]) {
                        Throwable ex = new GTScanSelfTerminatedException(logHeader + "The coprocessor thread stopped itself due to scan timeout or scan threshold(check region server log), failing current query...");
                        logger.error(logHeader + "Error when visiting cubes by endpoint", ex); // double log coz the query thread may already timeout
                        epResultItr.notifyCoprocException(ex);
                        return;
                    }
                }
            });
        }

        return new StorageResponseGTScatter(fullGTInfo, new DummyPartitionStreamer(epResultItr), scanRequest.getColumns(), totalScannedCount.get(), scanRequest.getStoragePushDownLimit());
    }

    private ByteString serializeGTScanReq(GTScanRequest scanRequest) {
        ByteString scanRequestByteString;
        int scanRequestBufferSize = BytesSerializer.SERIALIZE_BUFFER_SIZE;
        while (true) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(scanRequestBufferSize);
                GTScanRequest.serializer.serialize(scanRequest, buffer);
                buffer.flip();
                scanRequestByteString = HBaseZeroCopyByteString.wrap(buffer.array(), buffer.position(), buffer.limit());
                break;
            } catch (BufferOverflowException boe) {
                logger.info("Buffer size {} cannot hold the scan request, resizing to 4 times", scanRequestBufferSize);
                scanRequestBufferSize *= 4;
            }
        }
        return scanRequestByteString;
    }

    private ByteString serializeRawScans(List<RawScan> rawScans) {
        ByteString rawScanByteString;
        int rawScanBufferSize = BytesSerializer.SERIALIZE_BUFFER_SIZE;
        while (true) {
            try {
                ByteBuffer rawScanBuffer = ByteBuffer.allocate(rawScanBufferSize);
                BytesUtil.writeVInt(rawScans.size(), rawScanBuffer);
                for (RawScan rs : rawScans) {
                    RawScan.serializer.serialize(rs, rawScanBuffer);
                }
                rawScanBuffer.flip();
                rawScanByteString = HBaseZeroCopyByteString.wrap(rawScanBuffer.array(), rawScanBuffer.position(), rawScanBuffer.limit());
                break;
            } catch (BufferOverflowException boe) {
                logger.info("Buffer size {} cannot hold the raw scans, resizing to 4 times", rawScanBufferSize);
                rawScanBufferSize *= 4;
            }
        }
        return rawScanByteString;
    }

    private String getStatsString(byte[] region, CubeVisitResponse result) {
        StringBuilder sb = new StringBuilder();
        Stats stats = result.getStats();
        sb.append("Endpoint RPC returned from HTable ").append(cubeSeg.getStorageLocationIdentifier()).append(" Shard ").append(BytesUtil.toHex(region)).append(" on host: ").append(stats.getHostname()).append(".");
        sb.append("Total scanned row: ").append(stats.getScannedRowCount()).append(". ");
        sb.append("Total filtered/aggred row: ").append(stats.getAggregatedRowCount()).append(". ");
        sb.append("Time elapsed in EP: ").append(stats.getServiceEndTime() - stats.getServiceStartTime()).append("(ms). ");
        sb.append("Server CPU usage: ").append(stats.getSystemCpuLoad()).append(", server physical mem left: ").append(stats.getFreePhysicalMemorySize()).append(", server swap mem left:").append(stats.getFreeSwapSpaceSize()).append(".");
        sb.append("Etc message: ").append(stats.getEtcMsg()).append(".");
        sb.append("Normal Complete: ").append(stats.getNormalComplete() == 1).append(".");
        return sb.toString();

    }

}
