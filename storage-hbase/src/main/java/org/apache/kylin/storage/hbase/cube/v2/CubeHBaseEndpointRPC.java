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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DataFormatException;

import javax.annotation.Nullable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorBehavior;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
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

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    static class ExpectedSizeIterator implements Iterator<byte[]> {

        int expectedSize;
        int current = 0;
        BlockingQueue<byte[]> queue;

        public ExpectedSizeIterator(int expectedSize) {
            this.expectedSize = expectedSize;
            this.queue = new ArrayBlockingQueue<byte[]>(expectedSize);
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
                return queue.poll(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                throw new RuntimeException("error when waiting queue", e);
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
    }

    static class EndpointResultsAsGTScanner implements IGTScanner {
        private GTInfo info;
        private Iterator<byte[]> blocks;
        private ImmutableBitSet columns;
        private int totalScannedCount;

        public EndpointResultsAsGTScanner(GTInfo info, Iterator<byte[]> blocks, ImmutableBitSet columns, int totalScannedCount) {
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
        public int getScannedRowCount() {
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

    private List<Pair<byte[], byte[]>> getEPKeyRanges(short baseShard, short shardNum, int totalShards) {
        if (baseShard + shardNum <= totalShards) {
            //endpoint end key is inclusive, so no need to append 0 or anything
            return Lists.newArrayList(Pair.newPair(getByteArrayForShort(baseShard), getByteArrayForShort((short) (baseShard + shardNum - 1))));
        } else {
            //0,1,2,3,4 wants 4,0
            return Lists.newArrayList(Pair.newPair(getByteArrayForShort(baseShard), getByteArrayForShort((short) (totalShards - 1))),//
                    Pair.newPair(getByteArrayForShort((short) 0), getByteArrayForShort((short) (baseShard + shardNum - totalShards - 1))));
        }
    }

    @Override
    public IGTScanner getGTScanner(final List<GTScanRequest> scanRequests) throws IOException {

        final String toggle = BackdoorToggles.getCoprocessorBehavior() == null ? CoprocessorBehavior.SCAN_FILTER_AGGR_CHECKMEM.toString() : BackdoorToggles.getCoprocessorBehavior();
        logger.debug("The execution of this query will use " + toggle + " as endpoint's behavior");

        short cuboidBaseShard = cubeSeg.getCuboidBaseShard(this.cuboid.getId());
        short shardNum = cubeSeg.getCuboidShardNum(this.cuboid.getId());
        int totalShards = cubeSeg.getTotalShards();

        final List<ByteString> scanRequestByteStrings = Lists.newArrayList();
        final List<ByteString> rawScanByteStrings = Lists.newArrayList();

        // primary key (also the 0th column block) is always selected
        final ImmutableBitSet selectedColBlocks = scanRequests.get(0).getSelectedColBlocks().set(0);

        // globally shared connection, does not require close
        final HTableInterface hbaseTable = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl()).getTable(cubeSeg.getStorageLocationIdentifier());

        final List<IntList> hbaseColumnsToGTIntList = Lists.newArrayList();
        List<List<Integer>> hbaseColumnsToGT = getHBaseColumnsGTMapping(selectedColBlocks);
        for (List<Integer> list : hbaseColumnsToGT) {
            hbaseColumnsToGTIntList.add(IntList.newBuilder().addAllInts(list).build());
        }

        for (GTScanRequest req : scanRequests) {
            ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
            GTScanRequest.serializer.serialize(req, buffer);
            buffer.flip();
            scanRequestByteStrings.add(HBaseZeroCopyByteString.wrap(buffer.array(), buffer.position(), buffer.limit()));

            RawScan rawScan = preparedHBaseScan(req.getPkStart(), req.getPkEnd(), req.getFuzzyKeys(), selectedColBlocks);

            ByteBuffer rawScanBuffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
            RawScan.serializer.serialize(rawScan, rawScanBuffer);
            rawScanBuffer.flip();
            rawScanByteStrings.add(HBaseZeroCopyByteString.wrap(rawScanBuffer.array(), rawScanBuffer.position(), rawScanBuffer.limit()));

            logger.debug("Serialized scanRequestBytes {} bytes, rawScanBytesString {} bytes", buffer.limit() - buffer.position(), rawScanBuffer.limit() - rawScanBuffer.position());
            logScan(rawScan, cubeSeg.getStorageLocationIdentifier());
        }

        logger.debug("Start to executing: {} shards starting from {}", shardNum, cuboidBaseShard);

        final AtomicInteger totalScannedCount = new AtomicInteger(0);
        final ExpectedSizeIterator epResultItr = new ExpectedSizeIterator(scanRequests.size() * shardNum);

        for (final Pair<byte[], byte[]> epRange : getEPKeyRanges(cuboidBaseShard, shardNum, totalShards)) {
            for (int i = 0; i < scanRequests.size(); ++i) {
                final int scanIndex = i;
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        CubeVisitProtos.CubeVisitRequest.Builder builder = CubeVisitProtos.CubeVisitRequest.newBuilder();
                        builder.setGtScanRequest(scanRequestByteStrings.get(scanIndex)).setHbaseRawScan(rawScanByteStrings.get(scanIndex));
                        for (IntList intList : hbaseColumnsToGTIntList) {
                            builder.addHbaseColumnsToGT(intList);
                        }
                        builder.setRowkeyPreambleSize(cubeSeg.getRowKeyPreambleSize());
                        builder.setBehavior(toggle);

                        Map<byte[], CubeVisitProtos.CubeVisitResponse> results;
                        try {
                            results = getResults(builder.build(), hbaseTable, epRange.getFirst(), epRange.getSecond());
                        } catch (Throwable throwable) {
                            throw new RuntimeException("Error when visiting cubes by endpoint:", throwable);
                        }

                        for (Map.Entry<byte[], CubeVisitProtos.CubeVisitResponse> result : results.entrySet()) {
                            totalScannedCount.addAndGet(result.getValue().getStats().getScannedRowCount());
                            logger.info(getStatsString(result));
                            try {
                                epResultItr.append(CompressionUtils.decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getValue().getCompressedRows())));
                            } catch (IOException | DataFormatException e) {
                                throw new RuntimeException("Error when decompressing", e);
                            }
                        }
                    }
                });
            }
        }

        return new EndpointResultsAsGTScanner(fullGTInfo, epResultItr, scanRequests.get(0).getColumns(), totalScannedCount.get());
    }

    private String getStatsString(Map.Entry<byte[], CubeVisitProtos.CubeVisitResponse> result) {
        StringBuilder sb = new StringBuilder();
        Stats stats = result.getValue().getStats();
        sb.append("Shard " + BytesUtil.toHex(result.getKey()) + " on host: " + stats.getHostname() + ".");
        sb.append("Total scanned row: " + stats.getScannedRowCount() + ". ");
        sb.append("Total filtered/aggred row: " + stats.getAggregatedRowCount() + ". ");
        sb.append("Time elapsed in EP: " + (stats.getServiceEndTime() - stats.getServiceStartTime()) + "(ms). ");
        sb.append("Server CPU usage: " + stats.getSystemCpuLoad() + ", server physical mem left: " + stats.getFreePhysicalMemorySize() + ", server swap mem left:" + stats.getFreeSwapSpaceSize() + ".");
        sb.append("Etc message: " + stats.getEtcMsg() + ".");
        return sb.toString();

    }

    private Map<byte[], CubeVisitProtos.CubeVisitResponse> getResults(final CubeVisitProtos.CubeVisitRequest request, HTableInterface table, byte[] startKey, byte[] endKey) throws Throwable {
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

        return results;
    }
}
