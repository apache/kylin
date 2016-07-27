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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;

import javax.annotation.Nullable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LoggableCachedThreadPool;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorBehavior;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitService;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest.IntList;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

public class CubeHBaseEndpointRPC extends CubeHBaseRPC {

    public static final Logger logger = LoggerFactory.getLogger(CubeHBaseEndpointRPC.class);

    private static ExecutorService executorService = new LoggableCachedThreadPool();

    static class ExpectedSizeIterator implements Iterator<byte[]> {

        BlockingQueue<byte[]> queue;

        int expectedSize;
        int current = 0;
        long timeout;
        long timeoutTS;
        volatile Throwable coprocException;

        public ExpectedSizeIterator(int expectedSize) {
            this.expectedSize = expectedSize;
            this.queue = new ArrayBlockingQueue<byte[]>(expectedSize);

            Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
            this.timeout = hconf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5) * hconf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 60000);
            this.timeout = Math.max(this.timeout, 5 * 60000);
            this.timeout *= KylinConfig.getInstanceFromEnv().getCubeVisitTimeoutTimes();

            if (BackdoorToggles.getQueryTimeout() != -1) {
                this.timeout = BackdoorToggles.getQueryTimeout();
            }

            this.timeout *= 1.1; // allow for some delay

            logger.info("Timeout for ExpectedSizeIterator is: " + this.timeout);

            this.timeoutTS = System.currentTimeMillis() + this.timeout;
        }

        @Override
        public boolean hasNext() {
            return (current < expectedSize);
        }

        @Override
        public byte[] next() {
            if (current >= expectedSize) {
                throw new IllegalStateException("Won't have more data");
            }
            try {
                current++;
                byte[] ret = null;

                while (ret == null && coprocException == null && timeoutTS - System.currentTimeMillis() > 0) {
                    ret = queue.poll(5000, TimeUnit.MILLISECONDS);
                }

                if (coprocException != null) {
                    throw new RuntimeException("Error in coprocessor", coprocException);
                } else if (ret == null) {
                    throw new RuntimeException("Timeout visiting cube!");
                } else {
                    return ret;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Error when waiting queue", e);
            }
        }

        @Override
        public void remove() {
            throw new NotImplementedException();
        }

        public void append(byte[] data) {
            try {
                queue.put(data);
            } catch (InterruptedException e) {
                throw new RuntimeException("error when waiting queue", e);
            }
        }

        public long getTimeout() {
            return timeout;
        }

        public void notifyCoprocException(Throwable ex) {
            coprocException = ex;
        }
    }

    static class EndpointResultsAsGTScanner implements IGTScanner {
        private GTInfo info;
        private Iterator<byte[]> blocks;
        private ImmutableBitSet columns;
        private long totalScannedCount;

        public EndpointResultsAsGTScanner(GTInfo info, Iterator<byte[]> blocks, ImmutableBitSet columns, long totalScannedCount) {
            this.info = info;
            this.blocks = blocks;
            this.columns = columns;
            this.totalScannedCount = totalScannedCount;
        }

        @Override
        public GTInfo getInfo() {
            return info;
        }

        @Override
        public long getScannedRowCount() {
            return totalScannedCount;
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

        final String toggle = BackdoorToggles.getCoprocessorBehavior() == null ? CoprocessorBehavior.SCAN_FILTER_AGGR_CHECKMEM.toString() : BackdoorToggles.getCoprocessorBehavior();

        logger.debug("New scanner for current segment {} will use {} as endpoint's behavior", cubeSeg, toggle);

        Pair<Short, Short> shardNumAndBaseShard = getShardNumAndBaseShard();
        short shardNum = shardNumAndBaseShard.getFirst();
        short cuboidBaseShard = shardNumAndBaseShard.getSecond();
        int totalShards = cubeSeg.getTotalShards(cuboid.getId());

        ByteString scanRequestByteString = null;
        ByteString rawScanByteString = null;

        // primary key (also the 0th column block) is always selected
        final ImmutableBitSet selectedColBlocks = scanRequest.getSelectedColBlocks().set(0);

        // globally shared connection, does not require close
        final HConnection conn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());

        final List<IntList> hbaseColumnsToGTIntList = Lists.newArrayList();
        List<List<Integer>> hbaseColumnsToGT = getHBaseColumnsGTMapping(selectedColBlocks);
        for (List<Integer> list : hbaseColumnsToGT) {
            hbaseColumnsToGTIntList.add(IntList.newBuilder().addAllInts(list).build());
        }

        //TODO: raw scan can be constructed at region side to reduce traffic
        List<RawScan> rawScans = preparedHBaseScans(scanRequest.getGTScanRanges(), selectedColBlocks);
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
        scanRequest.setGTScanRanges(Lists.<GTScanRange> newArrayList());//since raw scans are sent to coprocessor, we don't need to duplicate sending it

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

        logger.debug("Serialized scanRequestBytes {} bytes, rawScanBytesString {} bytes", scanRequestByteString.size(), rawScanByteString.size());

        logger.info("The scan {} for segment {} is as below with {} separate raw scans, shard part of start/end key is set to 0", Integer.toHexString(System.identityHashCode(scanRequest)), cubeSeg, rawScans.size());
        for (RawScan rs : rawScans) {
            logScan(rs, cubeSeg.getStorageLocationIdentifier());
        }

        logger.debug("Submitting rpc to {} shards starting from shard {}, scan range count {}", shardNum, cuboidBaseShard, rawScans.size());

        final AtomicLong totalScannedCount = new AtomicLong(0);
        final ExpectedSizeIterator epResultItr = new ExpectedSizeIterator(shardNum);

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
        builder.setBehavior(toggle);
        builder.setStartTime(System.currentTimeMillis());
        builder.setTimeout(epResultItr.getTimeout());
        builder.setKylinProperties(kylinConfig.getConfigAsString());

        for (final Pair<byte[], byte[]> epRange : getEPKeyRanges(cuboidBaseShard, shardNum, totalShards)) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {

                    final String logHeader = "<sub-thread for GTScanRequest " + Integer.toHexString(System.identityHashCode(scanRequest)) + "> ";
                    final boolean[] abnormalFinish = new boolean[1];

                    try {
                        HTableInterface table = conn.getTable(cubeSeg.getStorageLocationIdentifier(), HBaseConnection.getCoprocessorPool());

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
                        Throwable ex = new RuntimeException(logHeader + "The coprocessor thread stopped itself due to scan timeout, failing current query...");
                        logger.error(logHeader + "Error when visiting cubes by endpoint", ex); // double log coz the query thread may already timeout
                        epResultItr.notifyCoprocException(ex);
                        return;
                    }
                }
            });
        }

        return new EndpointResultsAsGTScanner(fullGTInfo, epResultItr, scanRequest.getColumns(), totalScannedCount.get());
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
